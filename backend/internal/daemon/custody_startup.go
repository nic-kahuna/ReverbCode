package daemon

import (
	"context"
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/bootguard"
	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/custody"
	"github.com/aoagents/agent-orchestrator/backend/internal/runfile"
)

type CustodyActivation struct {
	Schema               string   `json:"schema"`
	DataDir              string   `json:"data_dir"`
	ProjectIDs           []string `json:"project_ids"`
	RequiredProtocol     int      `json:"required_protocol"`
	SupportedProtocol    int      `json:"supported_protocol"`
	MutationLanesStarted bool     `json:"mutation_lanes_started"`
	Capability           string   `json:"capability"`
}

// PrepareCustody is the explicit offline protocol-2 activation boundary. Merely
// installing or starting a protocol-2 binary never calls this operation.
func PrepareCustody(ctx context.Context, cfg config.Config, projects []string) (CustodyActivation, error) {
	var out CustodyActivation
	if err := custody.ProcessCapability(); err != nil {
		return out, err
	}
	guard, err := bootguard.Open(cfg.DataDir)
	if err != nil {
		return out, err
	}
	defer func() { _ = guard.Close() }()
	if live, err := runfile.CheckStale(cfg.RunFilePath); err != nil {
		return out, err
	} else if live != nil {
		return out, fmt.Errorf("AO daemon pid %d remains alive; activation requires offline ownership", live.PID)
	}
	store, _, err := openGuardedStartupStore(ctx, guard, false)
	if err != nil {
		return out, err
	}
	defer func() { _ = store.Close() }()
	if err = store.ActivateCustody(ctx, guard, projects); err != nil {
		return out, err
	}
	return CustodyActivation{Schema: "ao-custody-activation/v1", DataDir: guard.DataDir(), ProjectIDs: projects, RequiredProtocol: 2, SupportedProtocol: 2, MutationLanesStarted: false, Capability: "darwin_audit_token"}, nil
}
