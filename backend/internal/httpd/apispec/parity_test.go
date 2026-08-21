package apispec_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	yaml "gopkg.in/yaml.v3"

	"github.com/aoagents/agent-orchestrator/backend/internal/buildinfo"
	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apispec"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/controllers"
)

// TestRouteSpecParity asserts that the documented route union from the normal
// and recovery-fenced routers is in 1:1 correspondence with the OpenAPI
// operations. The recovery clear capability is optional at runtime, so this
// test supplies it to make the complete contract visible to chi.Walk.
func TestRouteSpecParity(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	// Mobile carries a non-nil MobileController so mountMobile (which, like
	// mountControl, skips mounting entirely on a nil controller) registers its
	// routes here — otherwise the mobile spec operations below would have no
	// mounted route to match.
	deps := httpd.APIDeps{Mobile: &controllers.MobileController{}}
	normal, err := httpd.NewRouterWithControlAndProbe(
		config.Config{}, log, nil, deps, httpd.ControlDeps{}, parityProbe(t, httpd.DaemonModeNormal),
	)
	if err != nil {
		t.Fatalf("build explicit normal router: %v", err)
	}
	recovery, err := httpd.NewRecoveryRouter(
		config.Config{},
		log,
		parityProbe(t, httpd.DaemonModeRecoveryFenced),
		httpd.RecoveryDeps{
			Inventory: parityRecoveryInventory{},
			Clearer: httpd.RecoveryClearFunc(func(context.Context, httpd.RecoveryClearRequest) (domain.RecoveryFenceStatus, error) {
				return domain.RecoveryFenceStatus{}, nil
			}),
		},
		httpd.ControlDeps{RequestShutdown: func() {}},
	)
	if err != nil {
		t.Fatalf("build recovery router: %v", err)
	}

	mounted := map[string]bool{}
	for name, router := range map[string]chi.Router{"normal": normal, "recovery": recovery} {
		err := chi.Walk(router, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
			if documentedRoute(route) {
				mounted[strings.ToUpper(method)+" "+route] = true
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s routes: %v", name, err)
		}
	}
	if len(mounted) == 0 {
		t.Fatal("no documented routes mounted — router wiring changed?")
	}

	// Forward: every mounted documented route resolves to an operation slice.
	for route := range mounted {
		parts := strings.SplitN(route, " ", 2)
		if apispec.Default().Operation(parts[0], parts[1]) == nil {
			t.Errorf("mounted route %s has no OpenAPI operation", route)
		}
	}

	// Reverse: every spec operation is mounted by at least one router mode.
	var doc struct {
		Paths map[string]map[string]yaml.Node `yaml:"paths"`
	}
	if err := yaml.Unmarshal(apispec.Default().YAML(), &doc); err != nil {
		t.Fatalf("parse spec: %v", err)
	}
	httpMethods := map[string]bool{"get": true, "post": true, "put": true, "patch": true, "delete": true}
	for path, item := range doc.Paths {
		for method := range item {
			if !httpMethods[method] {
				continue // skip parameters, summary, etc.
			}
			key := strings.ToUpper(method) + " " + path
			if !mounted[key] {
				t.Errorf("spec operation %s has no mounted route", key)
			}
		}
	}
}

// Control, terminal, telemetry, and the served spec itself are deliberately
// outside the generated application client. Health/readiness and every API
// operation are contract-backed.
func documentedRoute(route string) bool {
	return route == "/healthz" || route == "/readyz" || route == "/version" ||
		(strings.HasPrefix(route, "/api/v1/") && route != "/api/v1/openapi.yaml")
}

func parityProbe(t *testing.T, mode httpd.DaemonMode) httpd.ProbeContext {
	t.Helper()
	dataDir, err := httpd.CaptureDataDirIdentity(t.TempDir())
	if err != nil {
		t.Fatalf("capture data-dir identity: %v", err)
	}
	protocol := domain.RecoveryFenceProtocolVersion
	generation := int64(0)
	fence := domain.RecoveryFenceStatus{
		SupportedProtocolVersion:       domain.RecoveryFenceProtocolVersion,
		SupportedDatabaseSchemaVersion: 25,
		DatabaseSchemaVersion:          25,
		ProtocolVersion:                &protocol,
		State:                          domain.RecoveryFenceStateInactive,
		Disposition:                    domain.RecoveryFenceDispositionInactive,
		ReasonCode:                     domain.RecoveryFenceReasonSupportedInactive,
		RowCount:                       1,
		ProtocolStorageClass:           "integer",
		StateStorageClass:              "text",
		PayloadStorageClass:            "blob",
		GenerationStorageClass:         "integer",
		ActivationIDStorageClass:       "null",
		PayloadByteLength:              len(domain.RecoveryFenceCanonicalPayload),
		PayloadSHA256:                  domain.RecoveryFenceCanonicalPayloadSHA256,
		Generation:                     &generation,
	}
	if mode == httpd.DaemonModeRecoveryFenced {
		generation = 7
		fence.State = domain.RecoveryFenceStateActive
		fence.Disposition = domain.RecoveryFenceDispositionActive
		fence.ReasonCode = domain.RecoveryFenceReasonSupportedActive
		fence.ActivationIDStorageClass = "text"
		fence.ActivationID = "11111111-1111-4111-8111-111111111111"
	}
	return httpd.ProbeContext{
		InstanceID:       "00000000-0000-4000-8000-000000000009",
		Mode:             mode,
		WorkingDirectory: dataDir.CanonicalPath,
		DataDir:          dataDir,
		Build: buildinfo.Identity{
			Build: buildinfo.Metadata{Version: "test"},
			Executable: buildinfo.Executable{
				Path:   "/test/ao",
				SHA256: strings.Repeat("a", 64),
			},
		},
		Fence: fence,
	}
}

type parityRecoveryInventory struct{}

func (parityRecoveryInventory) InventoryStatus(context.Context) domain.RecoveryInventoryStatus {
	return domain.RecoveryInventoryStatus{
		SchemaVersion: domain.RecoveryInventorySchemaVersion,
		Fingerprint:   domain.RecoveryInventorySchemaFingerprint,
		Available:     true,
	}
}

func (parityRecoveryInventory) ListProjects(context.Context) ([]domain.RecoveryProject, error) {
	return nil, nil
}

func (parityRecoveryInventory) GetProject(context.Context, string) (domain.RecoveryProject, bool, error) {
	return domain.RecoveryProject{}, false, nil
}

func (parityRecoveryInventory) ListSessions(context.Context) ([]domain.RecoverySession, error) {
	return nil, nil
}

func (parityRecoveryInventory) GetSession(context.Context, string) (domain.RecoverySession, bool, error) {
	return domain.RecoverySession{}, false, nil
}

func (parityRecoveryInventory) ListWorkspaceRepos(context.Context, string) ([]domain.RecoveryWorkspaceRepo, error) {
	return nil, nil
}

func (parityRecoveryInventory) GetWorkspaceRepo(context.Context, string, string) (domain.RecoveryWorkspaceRepo, bool, error) {
	return domain.RecoveryWorkspaceRepo{}, false, nil
}

func (parityRecoveryInventory) ListSessionWorktrees(context.Context, string) ([]domain.RecoverySessionWorktree, error) {
	return nil, nil
}

func (parityRecoveryInventory) GetSessionWorktree(context.Context, string, string) (domain.RecoverySessionWorktree, bool, error) {
	return domain.RecoverySessionWorktree{}, false, nil
}
