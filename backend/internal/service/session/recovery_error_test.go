package session

import (
	"errors"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apierr"
	sessionmanager "github.com/aoagents/agent-orchestrator/backend/internal/session_manager"
)

func TestManagedRecoveryFailureMapsToStructuredConflictWithoutProviderID(t *testing.T) {
	err := &sessionmanager.RecoveryFailure{
		SessionID: "demo-1", ProjectID: "demo", Kind: domain.KindWorker,
		Code: "provider_session_missing", Phase: "preflight", ProviderSessionSaved: false,
		Worktrees: []domain.SessionWorktreeRecord{{
			SessionID: "demo-1", RepoName: domain.RootWorkspaceRepoName,
			Branch: "ao/demo-1", WorktreePath: "/managed/demo/demo-1",
			State: domain.SessionWorktreeStatePreserved,
		}},
	}
	mapped := toAPIError(err)
	var got *apierr.Error
	if !errors.As(mapped, &got) {
		t.Fatalf("mapped error = %T %v, want *apierr.Error", mapped, mapped)
	}
	if got.Kind != apierr.KindConflict || got.Code != "SESSION_RESTORE_FAILED" {
		t.Fatalf("api error = %#v", got)
	}
	if got.Details["reason"] != "provider_session_missing" || got.Details["sessionId"] != domain.SessionID("demo-1") || got.Details["providerSessionSaved"] != false {
		t.Fatalf("details = %#v", got.Details)
	}
	if got.Details["phase"] != "preflight" {
		t.Fatalf("phase = %#v", got.Details["phase"])
	}
	if _, exists := got.Details["runtimeStopped"]; exists {
		t.Fatalf("preflight exposed runtimeStopped: %#v", got.Details)
	}
	if _, leaked := got.Details["agentSessionId"]; leaked {
		t.Fatalf("details leaked provider session id: %#v", got.Details)
	}
	worktrees, ok := got.Details["worktrees"].([]map[string]any)
	if !ok || len(worktrees) != 1 || worktrees[0]["state"] != domain.SessionWorktreeStatePreserved {
		t.Fatalf("worktree evidence = %#v", got.Details["worktrees"])
	}
}

func TestManagedRecoveryRollbackFailureMapsRuntimeOutcome(t *testing.T) {
	stopped := false
	mapped := toAPIError(&sessionmanager.RecoveryFailure{
		SessionID: "demo-1", ProjectID: "demo", Kind: domain.KindWorker,
		Code: "rollback_failed", Phase: "rollback", RuntimeStopped: &stopped,
	})
	var got *apierr.Error
	if !errors.As(mapped, &got) || got.Code != "SESSION_RESTORE_FAILED" {
		t.Fatalf("mapped error = %T %v", mapped, mapped)
	}
	if got.Details["phase"] != "rollback" || got.Details["runtimeStopped"] != false || got.Details["reason"] != "rollback_failed" {
		t.Fatalf("details = %#v", got.Details)
	}
}
