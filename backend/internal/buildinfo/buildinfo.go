// Package buildinfo owns immutable source and executable identity for the CLI,
// daemon probes, and recovery diagnostics.
package buildinfo

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
)

// Release tooling stamps these variables with -ldflags. When they are empty,
// Metadata falls back to Go's embedded VCS settings for developer builds.
var (
	Version = "dev"
	Commit  = ""
	Date    = ""
)

// Metadata identifies the source used to build the executable. SourceModified
// always reflects Go's embedded vcs.modified setting when it is available;
// linker stamps identify a revision but do not prove the checkout was clean.
type Metadata struct {
	Version        string `json:"version"`
	SourceCommit   string `json:"sourceCommit,omitempty"`
	BuiltAt        string `json:"builtAt,omitempty"`
	SourceModified bool   `json:"sourceModified"`
}

// Executable identifies the exact running file. Desktop clients authorize a
// spawned/discovered bridge by matching both Path and SHA256 in addition to the
// probe PID; version strings alone are never an executable identity proof.
type Executable struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// Identity is the complete immutable build/runtime identity returned by
// readiness and version diagnostics.
type Identity struct {
	Build      Metadata   `json:"build"`
	Executable Executable `json:"executable"`
}

// MetadataForBuild resolves stamped metadata, falling back to Go's VCS data.
func MetadataForBuild() Metadata {
	info, ok := debug.ReadBuildInfo()
	return metadataForBuild(Version, Commit, Date, info, ok)
}

func metadataForBuild(version, commit, builtAt string, info *debug.BuildInfo, ok bool) Metadata {
	version = strings.TrimSpace(version)
	if version == "" {
		version = "dev"
	}
	result := Metadata{
		Version:      version,
		SourceCommit: strings.TrimSpace(commit),
		BuiltAt:      strings.TrimSpace(builtAt),
	}
	if !ok || info == nil {
		return result
	}
	if (result.Version == "" || result.Version == "dev") && info.Main.Version != "" && info.Main.Version != "(devel)" {
		result.Version = info.Main.Version
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			if result.SourceCommit == "" {
				result.SourceCommit = setting.Value
			}
		case "vcs.time":
			if result.BuiltAt == "" {
				result.BuiltAt = setting.Value
			}
		case "vcs.modified":
			result.SourceModified = setting.Value == "true"
		}
	}
	return result
}

// VersionString renders the existing CLI-compatible single-line form.
func VersionString() string {
	return formatVersion(MetadataForBuild())
}

func formatVersion(meta Metadata) string {
	parts := []string{meta.Version}
	if meta.SourceCommit != "" {
		parts = append(parts, "commit "+meta.SourceCommit)
	}
	if meta.BuiltAt != "" {
		parts = append(parts, "built "+meta.BuiltAt)
	}
	return strings.Join(parts, " ")
}

// Capture resolves and hashes the exact running executable. Callers should do
// this once during boot and keep the result immutable for the process lifetime.
func Capture() (Identity, error) {
	path, err := os.Executable()
	if err != nil {
		return Identity{}, fmt.Errorf("resolve executable: %w", err)
	}
	return captureExecutable(path, MetadataForBuild())
}

func captureExecutable(path string, build Metadata) (Identity, error) {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return Identity{}, fmt.Errorf("resolve absolute executable path: %w", err)
	}
	path = absolutePath
	if resolved, resolveErr := filepath.EvalSymlinks(path); resolveErr == nil {
		path = resolved
	}
	f, err := os.Open(path)
	if err != nil {
		return Identity{}, fmt.Errorf("open executable for identity: %w", err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return Identity{}, fmt.Errorf("hash executable identity: %w", err)
	}
	return Identity{
		Build: build,
		Executable: Executable{
			Path:   filepath.Clean(path),
			SHA256: hex.EncodeToString(h.Sum(nil)),
		},
	}, nil
}
