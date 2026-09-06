// Package bootguard owns AO's pre-database compatibility boundary. It does not
// authorize custody transfers or prove that existing runtime writers stopped.
package bootguard

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/aoagents/agent-orchestrator/backend/internal/datadirlock"
)

// Protocol constants define the exact on-disk compatibility contract.
const (
	MarkerSchema      = "ao-data-compatibility/v1"
	MarkerName        = "compatibility.json"
	SupportedProtocol = 2
)

// Stable errors allow offline tooling to distinguish refusal from uncertainty.
var (
	ErrMalformed   = errors.New("AO_COMPATIBILITY_MALFORMED")
	ErrUnsupported = errors.New("AO_COMPATIBILITY_UNSUPPORTED")
	ErrUnavailable = errors.New("AO_COMPATIBILITY_UNAVAILABLE")
	ErrOwnership   = errors.New("AO_COMPATIBILITY_OWNERSHIP_REQUIRED")
	ErrDowngrade   = errors.New("AO_COMPATIBILITY_DOWNGRADE_FORBIDDEN")
)

// Marker is a permanent monotonic floor, not a global live suspension switch.
// Only the canonical encoding written by this package is accepted.
type Marker struct {
	Schema           string `json:"schema"`
	RequiredProtocol int    `json:"requiredProtocol"`
}

// Inspection is observational. A mutating installer must hold ao.lock and
// recheck compatibility across replacement; this unlocked read grants no lease.
type Inspection struct {
	Schema               string `json:"schema"`
	SupportedProtocol    int    `json:"supportedProtocol"`
	MarkerPath           string `json:"markerPath"`
	State                string `json:"state"`
	RequiredProtocol     int    `json:"requiredProtocol,omitempty"`
	InspectionOnly       bool   `json:"inspectionOnly"`
	StartPausedSupported bool   `json:"startPausedSupported"`
}

func encode(m Marker) []byte {
	data, _ := json.Marshal(m)
	return append(data, '\n')
}

func read(dataDir string) (Marker, bool, error) {
	path := filepath.Join(dataDir, MarkerName)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return Marker{}, false, nil
	}
	if err != nil {
		return Marker{}, false, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if !info.Mode().IsRegular() || info.Size() > 1024 {
		return Marker{}, false, fmt.Errorf("%w: marker must be a small regular file", ErrMalformed)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Marker{}, false, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	var m Marker
	if err := json.Unmarshal(data, &m); err != nil || m.Schema != MarkerSchema || m.RequiredProtocol < 1 || !bytes.Equal(data, encode(m)) {
		return Marker{}, false, fmt.Errorf("%w: marker schema or canonical encoding invalid", ErrMalformed)
	}
	return m, true, nil
}

// Inspect reads no database, creates no files and does not acquire ownership.
func Inspect(dataDir string) (Inspection, error) {
	out := Inspection{Schema: "ao-compatibility/v1", SupportedProtocol: SupportedProtocol, InspectionOnly: true, StartPausedSupported: true}
	abs, err := canonicalInspectionPath(dataDir)
	if err != nil {
		out.State = "unavailable"
		// The unresolved path is diagnostic only; callers must reject unavailable.
		if attempted, pathErr := filepath.Abs(dataDir); pathErr == nil {
			out.MarkerPath = filepath.Join(attempted, MarkerName)
		}
		return out, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	out.MarkerPath = filepath.Join(abs, MarkerName)
	m, exists, err := read(abs)
	if err != nil {
		out.State = "unavailable"
		if errors.Is(err, ErrMalformed) {
			out.State = "malformed"
		}
		return out, err
	}
	if !exists {
		out.State = "missing_legacy"
		return out, nil
	}
	out.RequiredProtocol = m.RequiredProtocol
	out.State = "compatible"
	if m.RequiredProtocol > SupportedProtocol {
		out.State = "unsupported"
		return out, ErrUnsupported
	}
	return out, nil
}

// Resolve existing ancestors without creating a missing data directory. A
// dangling symlink or inaccessible ancestor is uncertainty, never fresh state.
func canonicalInspectionPath(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	if _, err := os.Lstat(abs); err == nil {
		return filepath.EvalSymlinks(abs)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	parent := filepath.Dir(abs)
	if parent == abs {
		return "", fmt.Errorf("cannot resolve data-directory root")
	}
	resolved, err := canonicalInspectionPath(parent)
	if err != nil {
		return "", err
	}
	return filepath.Join(resolved, filepath.Base(abs)), nil
}

// Guard retains the data-directory lock until all daemon/offline work ends.
// Its fields are private so callers cannot invent a ratchet capability.
type Guard struct {
	mu        sync.Mutex
	lock      *datadirlock.Lock
	supported int
}

// Open must precede any product database open, migration or subsystem creation.
func Open(dataDir string) (*Guard, error) { return open(dataDir, SupportedProtocol) }

func open(dataDir string, supported int) (*Guard, error) {
	lock, err := datadirlock.Acquire(dataDir)
	if err != nil {
		return nil, err
	}
	g := &Guard{lock: lock, supported: supported}
	m, exists, err := read(lock.DataDir())
	if err == nil && exists && m.RequiredProtocol > supported {
		err = ErrUnsupported
	}
	if err == nil && !exists {
		err = write(lock.DataDir(), Marker{Schema: MarkerSchema, RequiredProtocol: 1})
	}
	if err != nil {
		_ = g.Close()
		return nil, err
	}
	return g, nil
}

// DataDir returns the canonical locked directory while this Guard is held.
func (g *Guard) DataDir() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.lock == nil {
		return ""
	}
	return g.lock.DataDir()
}

// Ratchet durably raises the floor before any corresponding newer custody
// state is written. Errors, including fsync uncertainty, forbid that later write.
// Raising requires a binary that actually understands the requested protocol.
func (g *Guard) Ratchet(required int) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.lock == nil {
		return ErrOwnership
	}
	m, exists, err := read(g.lock.DataDir())
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: held marker disappeared", ErrUnavailable)
	}
	if m.RequiredProtocol > g.supported || required > g.supported {
		return ErrUnsupported
	}
	if required < m.RequiredProtocol {
		return ErrDowngrade
	}
	// Re-publish even on an equal requirement so retrying a prior uncertain
	// fsync must establish durability before it can authorize a later write.
	m.RequiredProtocol = required
	return write(g.lock.DataDir(), m)
}

// Close releases ownership after all product work has ended.
func (g *Guard) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.lock == nil {
		return nil
	}
	err := g.lock.Close()
	g.lock = nil
	return err
}

func write(dataDir string, m Marker) error {
	f, err := os.CreateTemp(dataDir, ".compatibility-*")
	if err != nil {
		return fmt.Errorf("%w: create marker: %w", ErrUnavailable, err)
	}
	name := f.Name()
	defer func() { _ = f.Close(); _ = os.Remove(name) }()
	if _, err = f.Write(encode(m)); err == nil {
		err = f.Sync()
	}
	if err == nil {
		err = f.Close()
	}
	if err == nil {
		err = replaceDurably(name, filepath.Join(dataDir, MarkerName))
	}
	if err != nil {
		return fmt.Errorf("%w: persist marker: %w", ErrUnavailable, err)
	}
	return nil
}
