package httpd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/aoagents/agent-orchestrator/backend/internal/buildinfo"
	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
)

// RecoveryInventory is the complete read-only persistence projection available
// in recovery mode. It deliberately exposes no general store or write method.
type RecoveryInventory interface {
	InventoryStatus(context.Context) domain.RecoveryInventoryStatus
	ListProjects(context.Context) ([]domain.RecoveryProject, error)
	GetProject(context.Context, string) (domain.RecoveryProject, bool, error)
	ListSessions(context.Context) ([]domain.RecoverySession, error)
	GetSession(context.Context, string) (domain.RecoverySession, bool, error)
	ListWorkspaceRepos(context.Context, string) ([]domain.RecoveryWorkspaceRepo, error)
	GetWorkspaceRepo(context.Context, string, string) (domain.RecoveryWorkspaceRepo, bool, error)
	ListSessionWorktrees(context.Context, string) ([]domain.RecoverySessionWorktree, error)
	GetSessionWorktree(context.Context, string, string) (domain.RecoverySessionWorktree, bool, error)
}

// RecoveryClearRequest is the entire client-supplied clear contract. These are
// compare-and-swap identity fields only; evidence is derived and verified by a
// future sealed storage/service implementation, never asserted by the client.
type RecoveryClearRequest struct {
	ProtocolVersion int64  `json:"protocolVersion" minimum:"1"`
	Generation      int64  `json:"generation" minimum:"0"`
	ActivationID    string `json:"activationId" minLength:"1"`
	PayloadSHA256   string `json:"payloadSha256" minLength:"64" maxLength:"64"`
}

// RecoveryClearer is the optional command-side adapter for a build that knows
// how to verify sealed recovery evidence. A nil implementation leaves the route
// unmounted, so POST /clear receives the standard RECOVERY_FENCED response.
type RecoveryClearer interface {
	ClearRecoveryFence(context.Context, RecoveryClearRequest) (domain.RecoveryFenceStatus, error)
}

// RecoveryClearFunc adapts a function to RecoveryClearer without requiring a
// command service to depend on an HTTP-specific concrete type.
type RecoveryClearFunc func(context.Context, RecoveryClearRequest) (domain.RecoveryFenceStatus, error)

func (f RecoveryClearFunc) ClearRecoveryFence(ctx context.Context, req RecoveryClearRequest) (domain.RecoveryFenceStatus, error) {
	return f(ctx, req)
}

// ErrRecoveryClearConflict lets an injected clearer distinguish stale CAS or
// insufficient sealed evidence (409) from an internal clear failure (500).
var ErrRecoveryClearConflict = errors.New("recovery clear compare-and-swap rejected")

// RecoveryDeps are the only data/command capabilities the fenced router can
// receive. Clearer is optional; Inventory may be nil only to keep diagnostics
// available while reporting inventory as unavailable.
type RecoveryDeps struct {
	Inventory RecoveryInventory
	Clearer   RecoveryClearer
}

// RecoveryStatusResponse is the body of GET /api/v1/recovery.
type RecoveryStatusResponse struct {
	SchemaVersion    int                            `json:"schemaVersion"`
	Status           string                         `json:"status" enum:"recovery_fenced"`
	Service          string                         `json:"service"`
	PID              int                            `json:"pid"`
	InstanceID       string                         `json:"instanceId"`
	Mode             DaemonMode                     `json:"mode" enum:"recovery_fenced"`
	DiagnosticBase   string                         `json:"diagnosticBase"`
	ExecutablePath   string                         `json:"executablePath"`
	WorkingDirectory string                         `json:"workingDirectory"`
	DataDir          DataDirIdentity                `json:"dataDir"`
	Build            buildinfo.Identity             `json:"build"`
	Fence            domain.RecoveryFenceStatus     `json:"fence"`
	Inventory        domain.RecoveryInventoryStatus `json:"inventory"`
}

type RecoveryProjectIDParam struct {
	ProjectID string `path:"projectId" description:"Persisted project identifier."`
}

type RecoverySessionIDParam struct {
	SessionID string `path:"sessionId" description:"Persisted session identifier."`
}

type RecoveryRepoNameParam struct {
	RepoName string `path:"repoName" description:"Persisted workspace repository name."`
}

type RecoveryListProjectsResponse struct {
	Inventory domain.RecoveryInventoryStatus `json:"inventory"`
	Projects  []domain.RecoveryProject       `json:"projects"`
}

type RecoveryProjectResponse struct {
	Inventory domain.RecoveryInventoryStatus `json:"inventory"`
	Project   domain.RecoveryProject         `json:"project"`
}

type RecoveryListSessionsResponse struct {
	Inventory domain.RecoveryInventoryStatus `json:"inventory"`
	Sessions  []domain.RecoverySession       `json:"sessions"`
}

type RecoverySessionResponse struct {
	Inventory domain.RecoveryInventoryStatus `json:"inventory"`
	Session   domain.RecoverySession         `json:"session"`
}

type RecoveryListWorkspaceReposResponse struct {
	Inventory domain.RecoveryInventoryStatus `json:"inventory"`
	ProjectID string                         `json:"projectId"`
	Repos     []domain.RecoveryWorkspaceRepo `json:"workspaceRepos"`
}

type RecoveryWorkspaceRepoResponse struct {
	Inventory domain.RecoveryInventoryStatus `json:"inventory"`
	Repo      domain.RecoveryWorkspaceRepo   `json:"workspaceRepo"`
}

type RecoveryListSessionWorktreesResponse struct {
	Inventory domain.RecoveryInventoryStatus   `json:"inventory"`
	SessionID string                           `json:"sessionId"`
	Worktrees []domain.RecoverySessionWorktree `json:"sessionWorktrees"`
}

type RecoverySessionWorktreeResponse struct {
	Inventory domain.RecoveryInventoryStatus `json:"inventory"`
	Worktree  domain.RecoverySessionWorktree `json:"sessionWorktree"`
}

type RecoveryClearResponse struct {
	Fence           domain.RecoveryFenceStatus `json:"fence"`
	RestartRequired bool                       `json:"restartRequired"`
}

type recoveryController struct {
	log       *slog.Logger
	proof     ProbeContext
	inventory RecoveryInventory
	clearer   RecoveryClearer
	shutdown  func()
}

// NewRecoveryRouter constructs the fenced surface from scratch. It intentionally
// does not call the normal router or mount CORS, OpenAPI, mux, telemetry, mobile,
// events, or any full API controller. Any route/method not registered here is a
// stable HTTP 423 RECOVERY_FENCED response, including OPTIONS preflights.
func NewRecoveryRouter(cfg config.Config, log *slog.Logger, proof ProbeContext, deps RecoveryDeps, control ControlDeps) (chi.Router, error) {
	if proof.Mode != DaemonModeRecoveryFenced {
		return nil, fmt.Errorf("recovery router requires mode %q, got %q", DaemonModeRecoveryFenced, proof.Mode)
	}
	if err := proof.Validate(); err != nil {
		return nil, fmt.Errorf("validate recovery readiness proof: %w", err)
	}
	if control.RequestShutdown == nil {
		return nil, fmt.Errorf("recovery router requires graceful shutdown control")
	}
	proof = snapshotProbeContext(proof)
	c := &recoveryController{
		log:       loggerOrDefault(log),
		proof:     proof,
		inventory: deps.Inventory,
		clearer:   deps.Clearer,
		shutdown:  control.RequestShutdown,
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(recoverTelemetry(c.log, nil))
	r.Use(recoveryCORSMiddleware(cfg.AllowedOrigins))
	r.NotFound(recoveryFenced)
	r.MethodNotAllowed(recoveryFenced)

	mountRecoveryHealth(r, proof)
	r.Get(RecoveryDiagnosticBase, c.status)
	r.Get(RecoveryDiagnosticBase+"/projects", c.listProjects)
	r.Get(RecoveryDiagnosticBase+"/projects/{projectId}", c.getProject)
	r.Get(RecoveryDiagnosticBase+"/projects/{projectId}/workspace-repos", c.listWorkspaceRepos)
	r.Get(RecoveryDiagnosticBase+"/projects/{projectId}/workspace-repos/{repoName}", c.getWorkspaceRepo)
	r.Get(RecoveryDiagnosticBase+"/sessions", c.listSessions)
	r.Get(RecoveryDiagnosticBase+"/sessions/{sessionId}", c.getSession)
	r.Get(RecoveryDiagnosticBase+"/sessions/{sessionId}/worktrees", c.listSessionWorktrees)
	r.Get(RecoveryDiagnosticBase+"/sessions/{sessionId}/worktrees/{repoName}", c.getSessionWorktree)
	if deps.Clearer != nil {
		r.Post(RecoveryDiagnosticBase+"/clear", c.clear)
	}
	mountRecoveryControl(r, control)
	return r, nil
}

func mountRecoveryHealth(r chi.Router, proof ProbeContext) {
	proof = snapshotProbeContext(proof)
	r.Get("/healthz", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.RawQuery != "" {
			recoveryFenced(w, req)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		envelope.WriteJSON(w, http.StatusOK, healthResponse(proof))
	})
	r.Get("/readyz", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.RawQuery != "" {
			recoveryFenced(w, req)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		envelope.WriteJSON(w, http.StatusOK, readyResponse(proof))
	})
	r.Get("/version", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.RawQuery != "" {
			recoveryFenced(w, req)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		envelope.WriteJSON(w, http.StatusOK, versionResponse(proof))
	})
}

func mountRecoveryControl(r chi.Router, control ControlDeps) {
	r.Post("/shutdown", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.RawQuery != "" || !emptyRequestBody(req) {
			recoveryFenced(w, req)
			return
		}
		if !localControlRequest(req) {
			w.Header().Set("Cache-Control", "no-store")
			envelope.WriteJSON(w, http.StatusForbidden, map[string]any{
				"status":  "forbidden",
				"service": daemonmeta.ServiceName,
			})
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		envelope.WriteJSON(w, http.StatusAccepted, map[string]any{
			"status":  "shutting_down",
			"service": daemonmeta.ServiceName,
			"pid":     os.Getpid(),
		})
		control.RequestShutdown()
	})
}

func emptyRequestBody(req *http.Request) bool {
	if req.Body == nil || req.Body == http.NoBody {
		return true
	}
	var one [1]byte
	n, err := req.Body.Read(one[:])
	return n == 0 && errors.Is(err, io.EOF)
}

func recoveryFenced(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	envelope.WriteAPIError(w, r, http.StatusLocked, "locked", "RECOVERY_FENCED",
		"Operation is unavailable while the daemon is recovery-fenced", map[string]any{
			"diagnosticBase": RecoveryDiagnosticBase,
		})
}

func (c *recoveryController) exactRequest(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.RawQuery == "" {
		return true
	}
	recoveryFenced(w, r)
	return false
}

func (c *recoveryController) status(w http.ResponseWriter, r *http.Request) {
	if !c.exactRequest(w, r) {
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	envelope.WriteJSON(w, http.StatusOK, RecoveryStatusResponse{
		SchemaVersion:    ReadinessSchemaVersion,
		Status:           string(DaemonModeRecoveryFenced),
		Service:          daemonmeta.ServiceName,
		PID:              os.Getpid(),
		InstanceID:       c.proof.InstanceID,
		Mode:             c.proof.Mode,
		DiagnosticBase:   RecoveryDiagnosticBase,
		ExecutablePath:   c.proof.Build.Executable.Path,
		WorkingDirectory: c.proof.WorkingDirectory,
		DataDir:          c.proof.DataDir,
		Build:            c.proof.Build,
		Fence:            cloneFenceStatus(c.proof.Fence),
		Inventory:        c.inventoryStatus(r.Context()),
	})
}

func (c *recoveryController) inventoryStatus(ctx context.Context) domain.RecoveryInventoryStatus {
	if c.inventory == nil {
		return domain.RecoveryInventoryStatus{
			SchemaVersion: domain.RecoveryInventorySchemaVersion,
			Fingerprint:   domain.RecoveryInventorySchemaFingerprint,
			Available:     false,
			ReasonCode:    "not_configured",
		}
	}
	return c.inventory.InventoryStatus(ctx)
}

func inventoryAvailable(status domain.RecoveryInventoryStatus) bool {
	return status.Available &&
		status.SchemaVersion == domain.RecoveryInventorySchemaVersion &&
		status.Fingerprint == domain.RecoveryInventorySchemaFingerprint
}

func (c *recoveryController) requireInventory(w http.ResponseWriter, r *http.Request) (domain.RecoveryInventoryStatus, bool) {
	status := c.inventoryStatus(r.Context())
	if c.inventory != nil && inventoryAvailable(status) {
		return status, true
	}
	w.Header().Set("Cache-Control", "no-store")
	envelope.WriteAPIError(w, r, http.StatusServiceUnavailable, "unavailable", "RECOVERY_INVENTORY_UNAVAILABLE",
		"Persisted recovery inventory is unavailable", map[string]any{"inventory": status})
	return status, false
}

func (c *recoveryController) inventoryFailure(w http.ResponseWriter, r *http.Request, status domain.RecoveryInventoryStatus, operation string, err error) {
	c.log.Error("recovery inventory read failed", "operation", operation, "err", err)
	w.Header().Set("Cache-Control", "no-store")
	envelope.WriteAPIError(w, r, http.StatusServiceUnavailable, "unavailable", "RECOVERY_INVENTORY_UNAVAILABLE",
		"Persisted recovery inventory could not be read", map[string]any{"inventory": status})
}

func (c *recoveryController) listProjects(w http.ResponseWriter, r *http.Request) {
	if !c.exactRequest(w, r) {
		return
	}
	status, ok := c.requireInventory(w, r)
	if !ok {
		return
	}
	projects, err := c.inventory.ListProjects(r.Context())
	if err != nil {
		c.inventoryFailure(w, r, status, "list_projects", err)
		return
	}
	if projects == nil {
		projects = []domain.RecoveryProject{}
	}
	envelope.WriteJSON(w, http.StatusOK, RecoveryListProjectsResponse{Inventory: status, Projects: projects})
}

func (c *recoveryController) getProject(w http.ResponseWriter, r *http.Request) {
	if !c.exactRequest(w, r) {
		return
	}
	status, ok := c.requireInventory(w, r)
	if !ok {
		return
	}
	project, found, err := c.inventory.GetProject(r.Context(), chi.URLParam(r, "projectId"))
	if err != nil {
		c.inventoryFailure(w, r, status, "get_project", err)
		return
	}
	if !found {
		envelope.WriteAPIError(w, r, http.StatusNotFound, "not_found", "RECOVERY_PROJECT_NOT_FOUND", "Persisted project was not found", nil)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, RecoveryProjectResponse{Inventory: status, Project: project})
}

func (c *recoveryController) listSessions(w http.ResponseWriter, r *http.Request) {
	if !c.exactRequest(w, r) {
		return
	}
	status, ok := c.requireInventory(w, r)
	if !ok {
		return
	}
	sessions, err := c.inventory.ListSessions(r.Context())
	if err != nil {
		c.inventoryFailure(w, r, status, "list_sessions", err)
		return
	}
	if sessions == nil {
		sessions = []domain.RecoverySession{}
	}
	envelope.WriteJSON(w, http.StatusOK, RecoveryListSessionsResponse{Inventory: status, Sessions: sessions})
}

func (c *recoveryController) getSession(w http.ResponseWriter, r *http.Request) {
	if !c.exactRequest(w, r) {
		return
	}
	status, ok := c.requireInventory(w, r)
	if !ok {
		return
	}
	session, found, err := c.inventory.GetSession(r.Context(), chi.URLParam(r, "sessionId"))
	if err != nil {
		c.inventoryFailure(w, r, status, "get_session", err)
		return
	}
	if !found {
		envelope.WriteAPIError(w, r, http.StatusNotFound, "not_found", "RECOVERY_SESSION_NOT_FOUND", "Persisted session was not found", nil)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, RecoverySessionResponse{Inventory: status, Session: session})
}

func (c *recoveryController) listWorkspaceRepos(w http.ResponseWriter, r *http.Request) {
	if !c.exactRequest(w, r) {
		return
	}
	status, ok := c.requireInventory(w, r)
	if !ok {
		return
	}
	projectID := chi.URLParam(r, "projectId")
	repos, err := c.inventory.ListWorkspaceRepos(r.Context(), projectID)
	if err != nil {
		c.inventoryFailure(w, r, status, "list_workspace_repos", err)
		return
	}
	if repos == nil {
		repos = []domain.RecoveryWorkspaceRepo{}
	}
	envelope.WriteJSON(w, http.StatusOK, RecoveryListWorkspaceReposResponse{Inventory: status, ProjectID: projectID, Repos: repos})
}

func (c *recoveryController) getWorkspaceRepo(w http.ResponseWriter, r *http.Request) {
	if !c.exactRequest(w, r) {
		return
	}
	status, ok := c.requireInventory(w, r)
	if !ok {
		return
	}
	repo, found, err := c.inventory.GetWorkspaceRepo(r.Context(), chi.URLParam(r, "projectId"), chi.URLParam(r, "repoName"))
	if err != nil {
		c.inventoryFailure(w, r, status, "get_workspace_repo", err)
		return
	}
	if !found {
		envelope.WriteAPIError(w, r, http.StatusNotFound, "not_found", "RECOVERY_WORKSPACE_REPO_NOT_FOUND", "Persisted workspace repository was not found", nil)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, RecoveryWorkspaceRepoResponse{Inventory: status, Repo: repo})
}

func (c *recoveryController) listSessionWorktrees(w http.ResponseWriter, r *http.Request) {
	if !c.exactRequest(w, r) {
		return
	}
	status, ok := c.requireInventory(w, r)
	if !ok {
		return
	}
	sessionID := chi.URLParam(r, "sessionId")
	worktrees, err := c.inventory.ListSessionWorktrees(r.Context(), sessionID)
	if err != nil {
		c.inventoryFailure(w, r, status, "list_session_worktrees", err)
		return
	}
	if worktrees == nil {
		worktrees = []domain.RecoverySessionWorktree{}
	}
	envelope.WriteJSON(w, http.StatusOK, RecoveryListSessionWorktreesResponse{Inventory: status, SessionID: sessionID, Worktrees: worktrees})
}

func (c *recoveryController) getSessionWorktree(w http.ResponseWriter, r *http.Request) {
	if !c.exactRequest(w, r) {
		return
	}
	status, ok := c.requireInventory(w, r)
	if !ok {
		return
	}
	worktree, found, err := c.inventory.GetSessionWorktree(r.Context(), chi.URLParam(r, "sessionId"), chi.URLParam(r, "repoName"))
	if err != nil {
		c.inventoryFailure(w, r, status, "get_session_worktree", err)
		return
	}
	if !found {
		envelope.WriteAPIError(w, r, http.StatusNotFound, "not_found", "RECOVERY_SESSION_WORKTREE_NOT_FOUND", "Persisted session worktree was not found", nil)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, RecoverySessionWorktreeResponse{Inventory: status, Worktree: worktree})
}

func (c *recoveryController) clear(w http.ResponseWriter, r *http.Request) {
	if !c.exactRequest(w, r) {
		return
	}
	if !localControlRequest(r) {
		envelope.WriteAPIError(w, r, http.StatusForbidden, "forbidden", "LOCAL_CONTROL_REQUIRED", "Recovery clear requires a local non-browser control client", nil)
		return
	}
	var req RecoveryClearRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_RECOVERY_CLEAR", "Recovery clear body must be exact valid JSON", nil)
		return
	}
	if err := requireJSONEOF(dec); err != nil {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_RECOVERY_CLEAR", "Recovery clear body must contain exactly one JSON object", nil)
		return
	}
	if err := validateRecoveryClearRequest(req); err != nil {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_RECOVERY_CLEAR", err.Error(), nil)
		return
	}
	result, err := c.clearer.ClearRecoveryFence(r.Context(), req)
	if err != nil {
		if errors.Is(err, ErrRecoveryClearConflict) {
			envelope.WriteAPIError(w, r, http.StatusConflict, "conflict", "RECOVERY_CLEAR_CONFLICT", "Recovery clear evidence or compare-and-swap identity did not match", nil)
			return
		}
		c.log.Error("recovery clear failed", "err", err)
		envelope.WriteAPIError(w, r, http.StatusInternalServerError, "internal", "RECOVERY_CLEAR_FAILED", "Recovery fence could not be cleared", nil)
		return
	}
	if !validClearedFence(req, result) {
		c.log.Error("recovery clearer returned an invalid inactive fence")
		envelope.WriteAPIError(w, r, http.StatusInternalServerError, "internal", "RECOVERY_CLEAR_INVALID_RESULT", "Recovery clearer returned an invalid fence transition", nil)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, RecoveryClearResponse{Fence: cloneFenceStatus(result), RestartRequired: true})
	c.shutdown()
}

func requireJSONEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("extra JSON value")
}

func validateRecoveryClearRequest(req RecoveryClearRequest) error {
	if req.ProtocolVersion != domain.RecoveryFenceProtocolVersion {
		return fmt.Errorf("protocolVersion must be %d", domain.RecoveryFenceProtocolVersion)
	}
	if req.Generation < 0 || req.Generation == math.MaxInt64 {
		return fmt.Errorf("generation must allow an exact increment")
	}
	if strings.TrimSpace(req.ActivationID) == "" {
		return fmt.Errorf("activationId is required")
	}
	if !validLowerSHA256(req.PayloadSHA256) {
		return fmt.Errorf("payloadSha256 must be 64 lowercase hexadecimal characters")
	}
	return nil
}

// validClearedFence verifies the exact supported-active -> supported-inactive
// transition result without consulting unrelated database schema state.
func validClearedFence(req RecoveryClearRequest, result domain.RecoveryFenceStatus) bool {
	return result.SupportedProtocolVersion == domain.RecoveryFenceProtocolVersion &&
		result.ProtocolVersion != nil && *result.ProtocolVersion == req.ProtocolVersion &&
		result.State == domain.RecoveryFenceStateInactive &&
		result.Disposition == domain.RecoveryFenceDispositionInactive &&
		result.ReasonCode == domain.RecoveryFenceReasonSupportedInactive &&
		result.RowCount == 1 &&
		result.ProtocolStorageClass == "integer" &&
		result.StateStorageClass == "text" &&
		result.PayloadStorageClass == "blob" &&
		result.GenerationStorageClass == "integer" &&
		result.ActivationIDStorageClass == "null" &&
		result.PayloadByteLength == len(domain.RecoveryFenceCanonicalPayload) &&
		result.PayloadSHA256 == domain.RecoveryFenceCanonicalPayloadSHA256 &&
		result.Generation != nil && *result.Generation == req.Generation+1 &&
		result.ActivationID == ""
}
