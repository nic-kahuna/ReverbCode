package httpd

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/aoagents/agent-orchestrator/backend/internal/buildinfo"
	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// ReadinessSchemaVersion freezes the daemon health/readiness proof shape.
// Clients must reject versions they do not understand rather than inferring
// normal operation from a partial or legacy response.
const ReadinessSchemaVersion = 1

// RecoveryDiagnosticBase is the only recovery-diagnostic API prefix. It is
// advertised in readiness so a client that attaches to a fenced daemon does not
// have to guess which surface is safe.
const RecoveryDiagnosticBase = "/api/v1/recovery"

// DaemonMode is the exhaustive boot mode vocabulary carried by health/readiness.
type DaemonMode string

const (
	DaemonModeNormal         DaemonMode = "normal"
	DaemonModeRecoveryFenced DaemonMode = "recovery_fenced"
)

// DataDirIdentity identifies the exact durable-state directory selected for
// this boot. SHA256 is sha256(CanonicalPath's UTF-8 bytes), not a digest of the
// directory contents.
type DataDirIdentity struct {
	CanonicalPath string `json:"canonicalPath"`
	SHA256        string `json:"sha256"`
}

// ProbeContext is the immutable, boot-scoped proof input shared by /healthz and
// /readyz. The daemon supplies one UUID and one captured build/data-dir/fence
// snapshot; router construction copies pointer-bearing fence fields so later
// caller mutation cannot change the served proof.
type ProbeContext struct {
	InstanceID       string
	Mode             DaemonMode
	WorkingDirectory string
	DataDir          DataDirIdentity
	Build            buildinfo.Identity
	Fence            domain.RecoveryFenceStatus
}

// CaptureDataDirIdentity resolves dataDir through symlinks and hashes the exact
// canonical UTF-8 path string. Production calls this after the data-dir lease
// has created/canonicalised the directory, once per boot (never per request).
func CaptureDataDirIdentity(dataDir string) (DataDirIdentity, error) {
	if strings.TrimSpace(dataDir) == "" {
		dataDir = "."
	}
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return DataDirIdentity{}, fmt.Errorf("resolve absolute data dir: %w", err)
	}
	canonical, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return DataDirIdentity{}, fmt.Errorf("resolve canonical data dir: %w", err)
	}
	canonical = filepath.Clean(canonical)
	if !utf8.ValidString(canonical) {
		return DataDirIdentity{}, fmt.Errorf("canonical data dir is not valid UTF-8")
	}
	sum := sha256.Sum256([]byte(canonical))
	return DataDirIdentity{CanonicalPath: canonical, SHA256: hex.EncodeToString(sum[:])}, nil
}

// CaptureWorkingDirectory snapshots the process working directory once for
// compatibility diagnostics. It must never be recomputed per probe request.
func CaptureWorkingDirectory() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve working directory: %w", err)
	}
	cwd, err = filepath.Abs(cwd)
	if err != nil {
		return "", fmt.Errorf("resolve absolute working directory: %w", err)
	}
	cwd = filepath.Clean(cwd)
	if !utf8.ValidString(cwd) {
		return "", fmt.Errorf("working directory is not valid UTF-8")
	}
	return cwd, nil
}

// Validate proves that the snapshot is complete and internally consistent.
// Only a complete supported-inactive fence can accompany normal mode; every
// other safely-readable classification belongs to recovery_fenced mode.
func (p ProbeContext) Validate() error {
	if parsed, err := uuid.Parse(p.InstanceID); err != nil || parsed.String() != p.InstanceID {
		return fmt.Errorf("instanceId must be a canonical UUID")
	}
	if p.Mode != DaemonModeNormal && p.Mode != DaemonModeRecoveryFenced {
		return fmt.Errorf("unsupported daemon mode %q", p.Mode)
	}
	if err := validateDataDirIdentity(p.DataDir); err != nil {
		return err
	}
	if p.WorkingDirectory == "" || !utf8.ValidString(p.WorkingDirectory) ||
		!filepath.IsAbs(p.WorkingDirectory) || filepath.Clean(p.WorkingDirectory) != p.WorkingDirectory {
		return fmt.Errorf("workingDirectory must be a non-empty absolute clean UTF-8 path")
	}
	if strings.TrimSpace(p.Build.Build.Version) == "" {
		return fmt.Errorf("build version is required")
	}
	if strings.TrimSpace(p.Build.Executable.Path) == "" {
		return fmt.Errorf("build executable path is required")
	}
	if !utf8.ValidString(p.Build.Executable.Path) || !filepath.IsAbs(p.Build.Executable.Path) ||
		filepath.Clean(p.Build.Executable.Path) != p.Build.Executable.Path {
		return fmt.Errorf("build executable path must be absolute, clean, and valid UTF-8")
	}
	if !validLowerSHA256(p.Build.Executable.SHA256) {
		return fmt.Errorf("build executable sha256 must be 64 lowercase hexadecimal characters")
	}
	if p.Fence.SupportedProtocolVersion != domain.RecoveryFenceProtocolVersion {
		return fmt.Errorf("fence supportedProtocolVersion = %d, want %d", p.Fence.SupportedProtocolVersion, domain.RecoveryFenceProtocolVersion)
	}
	if strings.TrimSpace(string(p.Fence.Disposition)) == "" || strings.TrimSpace(string(p.Fence.ReasonCode)) == "" {
		return fmt.Errorf("complete fence disposition and reasonCode are required")
	}
	if p.Mode == DaemonModeNormal && !p.Fence.AllowsNormalBoot() {
		return fmt.Errorf("normal mode requires an exact supported-inactive fence")
	}
	if p.Mode == DaemonModeRecoveryFenced && p.Fence.AllowsNormalBoot() {
		return fmt.Errorf("recovery_fenced mode cannot carry a normal-boot fence")
	}
	return nil
}

func validateDataDirIdentity(identity DataDirIdentity) error {
	if identity.CanonicalPath == "" || !utf8.ValidString(identity.CanonicalPath) {
		return fmt.Errorf("dataDir canonicalPath must be non-empty valid UTF-8")
	}
	if !filepath.IsAbs(identity.CanonicalPath) || filepath.Clean(identity.CanonicalPath) != identity.CanonicalPath {
		return fmt.Errorf("dataDir canonicalPath must be absolute and clean")
	}
	sum := sha256.Sum256([]byte(identity.CanonicalPath))
	if identity.SHA256 != hex.EncodeToString(sum[:]) {
		return fmt.Errorf("dataDir sha256 does not match canonicalPath")
	}
	return nil
}

func validLowerSHA256(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func snapshotProbeContext(in ProbeContext) ProbeContext {
	out := in
	out.Fence = cloneFenceStatus(in.Fence)
	return out
}

func cloneFenceStatus(in domain.RecoveryFenceStatus) domain.RecoveryFenceStatus {
	out := in
	if in.ProtocolVersion != nil {
		v := *in.ProtocolVersion
		out.ProtocolVersion = &v
	}
	if in.Generation != nil {
		v := *in.Generation
		out.Generation = &v
	}
	return out
}

func supportedInactiveFence() domain.RecoveryFenceStatus {
	protocol := domain.RecoveryFenceProtocolVersion
	generation := int64(0)
	return domain.RecoveryFenceStatus{
		SupportedProtocolVersion: domain.RecoveryFenceProtocolVersion,
		ProtocolVersion:          &protocol,
		State:                    domain.RecoveryFenceStateInactive,
		Disposition:              domain.RecoveryFenceDispositionInactive,
		ReasonCode:               domain.RecoveryFenceReasonSupportedInactive,
		RowCount:                 1,
		ProtocolStorageClass:     "integer",
		StateStorageClass:        "text",
		PayloadStorageClass:      "blob",
		GenerationStorageClass:   "integer",
		ActivationIDStorageClass: "null",
		PayloadByteLength:        len(domain.RecoveryFenceCanonicalPayload),
		PayloadSHA256:            domain.RecoveryFenceCanonicalPayloadSHA256,
		Generation:               &generation,
	}
}

var (
	compatibilityBuildOnce sync.Once
	compatibilityBuild     buildinfo.Identity
	compatibilityBuildErr  error
)

// compatibilityNormalProbe preserves pre-recovery router constructors while
// callers migrate to the explicit proof-taking variants. Production daemon
// wiring should pass its own boot UUID and captured identities instead.
func compatibilityNormalProbe(cfg config.Config) (ProbeContext, error) {
	compatibilityBuildOnce.Do(func() {
		compatibilityBuild, compatibilityBuildErr = buildinfo.Capture()
	})
	if compatibilityBuildErr != nil {
		return ProbeContext{}, compatibilityBuildErr
	}
	dataDir, err := CaptureDataDirIdentity(cfg.DataDir)
	if err != nil {
		return ProbeContext{}, err
	}
	workingDirectory, err := CaptureWorkingDirectory()
	if err != nil {
		return ProbeContext{}, err
	}
	proof := ProbeContext{
		InstanceID:       uuid.NewString(),
		Mode:             DaemonModeNormal,
		WorkingDirectory: workingDirectory,
		DataDir:          dataDir,
		Build:            compatibilityBuild,
		Fence:            supportedInactiveFence(),
	}
	return proof, proof.Validate()
}

// DaemonHealthResponse is the exact HTTP 200 body of /healthz.
type DaemonHealthResponse struct {
	SchemaVersion    int                        `json:"schemaVersion"`
	Status           string                     `json:"status" enum:"ok"`
	Service          string                     `json:"service"`
	PID              int                        `json:"pid"`
	InstanceID       string                     `json:"instanceId"`
	Mode             DaemonMode                 `json:"mode" enum:"normal,recovery_fenced"`
	DiagnosticBase   string                     `json:"diagnosticBase"`
	ExecutablePath   string                     `json:"executablePath"`
	WorkingDirectory string                     `json:"workingDirectory"`
	DataDir          DataDirIdentity            `json:"dataDir"`
	Build            buildinfo.Identity         `json:"build"`
	Fence            domain.RecoveryFenceStatus `json:"fence"`
}

// DaemonReadyResponse is the exact HTTP 200 body of /readyz. A fenced daemon is
// structurally ready for its restricted diagnostic surface; Mode, not the HTTP
// status, determines whether mutation may be authorized.
type DaemonReadyResponse struct {
	SchemaVersion    int                        `json:"schemaVersion"`
	Status           string                     `json:"status" enum:"ready"`
	Service          string                     `json:"service"`
	PID              int                        `json:"pid"`
	InstanceID       string                     `json:"instanceId"`
	Mode             DaemonMode                 `json:"mode" enum:"normal,recovery_fenced"`
	DiagnosticBase   string                     `json:"diagnosticBase"`
	ExecutablePath   string                     `json:"executablePath"`
	WorkingDirectory string                     `json:"workingDirectory"`
	DataDir          DataDirIdentity            `json:"dataDir"`
	Build            buildinfo.Identity         `json:"build"`
	Fence            domain.RecoveryFenceStatus `json:"fence"`
}

// DaemonVersionResponse is the store-independent build diagnostic returned by
// GET /version in both normal and recovery-fenced modes.
type DaemonVersionResponse struct {
	SchemaVersion    int                `json:"schemaVersion"`
	Status           string             `json:"status" enum:"ok"`
	Service          string             `json:"service"`
	PID              int                `json:"pid"`
	InstanceID       string             `json:"instanceId"`
	Mode             DaemonMode         `json:"mode" enum:"normal,recovery_fenced"`
	ExecutablePath   string             `json:"executablePath"`
	WorkingDirectory string             `json:"workingDirectory"`
	Build            buildinfo.Identity `json:"build"`
}

func healthResponse(proof ProbeContext) DaemonHealthResponse {
	return DaemonHealthResponse{
		SchemaVersion:    ReadinessSchemaVersion,
		Status:           "ok",
		Service:          daemonmeta.ServiceName,
		PID:              os.Getpid(),
		InstanceID:       proof.InstanceID,
		Mode:             proof.Mode,
		DiagnosticBase:   RecoveryDiagnosticBase,
		ExecutablePath:   proof.Build.Executable.Path,
		WorkingDirectory: proof.WorkingDirectory,
		DataDir:          proof.DataDir,
		Build:            proof.Build,
		Fence:            cloneFenceStatus(proof.Fence),
	}
}

func readyResponse(proof ProbeContext) DaemonReadyResponse {
	return DaemonReadyResponse{
		SchemaVersion:    ReadinessSchemaVersion,
		Status:           "ready",
		Service:          daemonmeta.ServiceName,
		PID:              os.Getpid(),
		InstanceID:       proof.InstanceID,
		Mode:             proof.Mode,
		DiagnosticBase:   RecoveryDiagnosticBase,
		ExecutablePath:   proof.Build.Executable.Path,
		WorkingDirectory: proof.WorkingDirectory,
		DataDir:          proof.DataDir,
		Build:            proof.Build,
		Fence:            cloneFenceStatus(proof.Fence),
	}
}

func versionResponse(proof ProbeContext) DaemonVersionResponse {
	return DaemonVersionResponse{
		SchemaVersion:    ReadinessSchemaVersion,
		Status:           "ok",
		Service:          daemonmeta.ServiceName,
		PID:              os.Getpid(),
		InstanceID:       proof.InstanceID,
		Mode:             proof.Mode,
		ExecutablePath:   proof.Build.Executable.Path,
		WorkingDirectory: proof.WorkingDirectory,
		Build:            proof.Build,
	}
}
