package daemon

import (
	"context"
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/bootguard"
	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/runfile"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// openGuardedStartupStore is the prelane sequence shared with offline preparation.
// The caller closes the store before releasing its already-held Guard.
func openGuardedStartupStore(ctx context.Context, guard *bootguard.Guard, paused bool) (*sqlite.Store, []string, error) {
	store, err := sqlite.Open(guard.DataDir())
	if err != nil {
		return nil, nil, fmt.Errorf("open store: %w", err)
	}
	store.AttachCompatibilityRatchet(guard)
	ids := []string{}
	if paused {
		ids, err = store.PauseAdmissionForStartup(ctx)
		if err != nil {
			_ = store.Close()
			return nil, nil, fmt.Errorf("persist startup admission pause: %w", err)
		}
	}
	return store, ids, nil
}

// StartPausedPreparation attests only an offline admission transition. Existing
// worker processes may still be alive; no custody or quiescence is certified.
type StartPausedPreparation struct {
	Schema                       string   `json:"schema"`
	DataDir                      string   `json:"dataDir"`
	ProjectIDs                   []string `json:"projectIds"`
	AdmissionPaused              bool     `json:"admissionPaused"`
	Scope                        string   `json:"scope"`
	ExistingSessionsMayBeRunning bool     `json:"existingSessionsMayBeRunning"`
	RequiredProtocol             int      `json:"requiredProtocol"`
	SupportedProtocol            int      `json:"supportedProtocol"`
	MutationLanesStarted         bool     `json:"mutationLanesStarted"`
	PreparationOnly              bool     `json:"preparationOnly"`
}

// PrepareStartPaused is a native offline writer like legacy import. It never
// constructs a runtime, observer, HTTP server, telemetry sink or lifecycle.
func PrepareStartPaused(ctx context.Context, cfg config.Config) (StartPausedPreparation, error) {
	guard, err := bootguard.Open(cfg.DataDir)
	if err != nil {
		return StartPausedPreparation{}, err
	}
	defer func() { _ = guard.Close() }()
	// Pre-guard daemons do not hold ao.lock. Retain the conservative legacy
	// run-file check too; supported orchestration separately proves app stop.
	if live, err := runfile.CheckStale(cfg.RunFilePath); err != nil {
		return StartPausedPreparation{}, err
	} else if live != nil {
		return StartPausedPreparation{}, fmt.Errorf("AO daemon pid %d remains alive; stop it before preparation", live.PID)
	}
	store, ids, err := openGuardedStartupStore(ctx, guard, true)
	if err != nil {
		return StartPausedPreparation{}, err
	}
	status, err := bootguard.Inspect(guard.DataDir())
	closeErr := store.Close()
	if err != nil {
		return StartPausedPreparation{}, err
	}
	if closeErr != nil {
		return StartPausedPreparation{}, closeErr
	}
	return StartPausedPreparation{Schema: "ao-start-paused-preparation/v1", DataDir: guard.DataDir(), ProjectIDs: ids, AdmissionPaused: true, Scope: "new_launches_only", ExistingSessionsMayBeRunning: true, RequiredProtocol: status.RequiredProtocol, SupportedProtocol: status.SupportedProtocol, PreparationOnly: true}, nil
}
