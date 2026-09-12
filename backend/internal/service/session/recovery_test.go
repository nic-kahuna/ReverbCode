package session

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apierr"
)

// service_test.go's fakeStore predates recovery inventory. Keep its existing
// broad fixture untouched and supply the new narrow Store method here.
var fakeRecoveryWorktrees = map[domain.SessionID][]domain.SessionWorktreeRecord{}

func (f *fakeStore) ListSessionWorktrees(_ context.Context, id domain.SessionID) ([]domain.SessionWorktreeRecord, error) {
	return append([]domain.SessionWorktreeRecord(nil), fakeRecoveryWorktrees[id]...), nil
}

func TestSessionRecoveryKeepsTerminatedStatusAndReturnsExactInventory(t *testing.T) {
	st := newFakeStore()
	rec := domain.SessionRecord{
		ID:           "mer-1",
		ProjectID:    "mer",
		Kind:         domain.KindWorker,
		Harness:      domain.HarnessCodex,
		IsTerminated: true,
		Activity:     domain.Activity{State: domain.ActivityWaitingInput},
		Metadata: domain.SessionMetadata{
			AgentSessionID: "provider-secret-id",
		},
	}
	st.sessions[rec.ID] = rec
	st.projects["mer"] = domain.ProjectRecord{ID: "mer", Config: domain.ProjectConfig{
		StartupRestorePolicy: domain.StartupRestorePreserveOnly,
	}}
	rows := []domain.SessionWorktreeRecord{
		{
			SessionID:    rec.ID,
			RepoName:     domain.RootWorkspaceRepoName,
			Branch:       "ao/mer-1/root",
			BaseSHA:      "base-root",
			WorktreePath: "/worktrees/mer-1",
			PreservedRef: "refs/ao/preserved/mer-1",
			State:        domain.SessionWorktreeStatePreserved,
		},
		{
			SessionID:    rec.ID,
			RepoName:     "api",
			Branch:       "ao/mer-1/root",
			BaseSHA:      "base-api",
			WorktreePath: "/worktrees/mer-1/api",
			State:        "active",
		},
	}
	fakeRecoveryWorktrees[rec.ID] = rows
	t.Cleanup(func() { delete(fakeRecoveryWorktrees, rec.ID) })

	svc := &Service{store: st}
	got, err := svc.Get(context.Background(), rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.StatusTerminated {
		t.Fatalf("status = %q, want %q", got.Status, domain.StatusTerminated)
	}
	if got.Activity.State != domain.ActivityExited {
		t.Fatalf("activity = %q, want exited recovery projection", got.Activity.State)
	}
	if !got.IsTerminated || got.TerminalHandleID != "" {
		t.Fatalf("recovery projection = terminated %v terminal handle %q, want true/empty", got.IsTerminated, got.TerminalHandleID)
	}
	if got.Recovery == nil {
		t.Fatal("recovery = nil, want managed recovery evidence")
	}
	if got.Recovery.State != domain.SessionRecoveryAwaitingRecovery ||
		got.Recovery.Policy != domain.StartupRestorePreserveOnly ||
		got.Recovery.RuntimeState != domain.SessionRecoveryRuntimeAbsent ||
		!got.Recovery.ProviderSessionSaved {
		t.Fatalf("recovery header = %#v", got.Recovery)
	}
	wantWorktrees := []domain.RecoveryWorktree{
		{RepoName: domain.RootWorkspaceRepoName, Branch: "ao/mer-1/root", BaseSHA: "base-root", WorktreePath: "/worktrees/mer-1", PreservedRef: "refs/ao/preserved/mer-1", State: domain.SessionWorktreeStatePreserved},
		{RepoName: "api", Branch: "ao/mer-1/root", BaseSHA: "base-api", WorktreePath: "/worktrees/mer-1/api", PreservedRef: "", State: "active"},
	}
	if !reflect.DeepEqual(got.Recovery.Worktrees, wantWorktrees) {
		t.Fatalf("worktrees = %#v, want %#v", got.Recovery.Worktrees, wantWorktrees)
	}

	list, err := svc.List(context.Background(), ListFilter{ProjectID: rec.ProjectID})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Status != domain.StatusTerminated || !reflect.DeepEqual(list[0].Recovery, got.Recovery) {
		t.Fatalf("list = %#v, want same recovery projection as Get", list)
	}
}

func TestSessionRecoveryFiltersUseProjectedTermination(t *testing.T) {
	st := newFakeStore()
	id := domain.SessionID("mer-stale-live")
	rec := domain.SessionRecord{
		ID: id, ProjectID: "mer", Kind: domain.KindWorker, IsTerminated: false,
		Activity: domain.Activity{State: domain.ActivityWaitingInput},
		Metadata: domain.SessionMetadata{RuntimeHandleID: "stale-runtime", AgentSessionID: "saved-provider"},
	}
	st.sessions[id] = rec
	fakeRecoveryWorktrees[id] = []domain.SessionWorktreeRecord{{SessionID: id, RepoName: domain.RootWorkspaceRepoName, State: domain.SessionWorktreeStatePreserved}}
	t.Cleanup(func() { delete(fakeRecoveryWorktrees, id) })
	svc := &Service{store: st}
	active, inactive := true, false
	for name, filter := range map[string]ListFilter{
		"active": {ProjectID: "mer", Active: &active},
		"fresh":  {ProjectID: "mer", Fresh: true},
	} {
		got, err := svc.List(context.Background(), filter)
		if err != nil || len(got) != 0 {
			t.Fatalf("%s list = %#v, %v, want empty", name, got, err)
		}
	}
	got, err := svc.List(context.Background(), ListFilter{ProjectID: "mer", Active: &inactive})
	if err != nil || len(got) != 1 {
		t.Fatalf("inactive list = %#v, %v", got, err)
	}
	if !got[0].IsTerminated || got[0].Activity.State != domain.ActivityExited || got[0].TerminalHandleID != "" || got[0].Recovery == nil {
		t.Fatalf("inactive recovery projection = %#v", got[0])
	}
	if got[0].Recovery.RuntimeState != domain.SessionRecoveryRuntimeUnknown {
		t.Fatalf("runtime state = %q, want unknown", got[0].Recovery.RuntimeState)
	}
	if st.sessions[id].IsTerminated || st.sessions[id].Activity.State != domain.ActivityWaitingInput || st.sessions[id].Metadata.RuntimeHandleID != "stale-runtime" {
		t.Fatalf("read projection mutated durable fake row: %#v", st.sessions[id])
	}
}

func TestSessionRecoveryUsesDurableMarkerAsAuthoritativeEvidence(t *testing.T) {
	tests := []struct {
		name         string
		policy       domain.StartupRestorePolicy
		terminated   bool
		rowState     string
		configError  string
		projectGone  bool
		wantStatus   domain.SessionStatus
		wantRecovery bool
	}{
		{name: "automatic config cannot erase marker", policy: domain.StartupRestoreAutomatic, terminated: true, rowState: domain.SessionWorktreeStatePreserved, wantStatus: domain.StatusTerminated, wantRecovery: true},
		{name: "legacy empty config cannot erase marker", policy: "", terminated: true, rowState: domain.SessionWorktreeStatePreserved, wantStatus: domain.StatusTerminated, wantRecovery: true},
		{name: "stale live fact cannot outrank marker", policy: domain.StartupRestorePreserveOnly, terminated: false, rowState: domain.SessionWorktreeStatePreserved, wantStatus: domain.StatusTerminated, wantRecovery: true},
		{name: "degraded config cannot erase marker", policy: domain.StartupRestorePreserveOnly, terminated: true, rowState: domain.SessionWorktreeStatePreserved, configError: "invalid config JSON", wantStatus: domain.StatusTerminated, wantRecovery: true},
		{name: "missing project cannot erase marker", terminated: true, rowState: domain.SessionWorktreeStatePreserved, projectGone: true, wantStatus: domain.StatusTerminated, wantRecovery: true},
		{name: "managed session without journal rows", policy: domain.StartupRestorePreserveOnly, terminated: true, wantStatus: domain.StatusTerminated},
		{name: "plain active row is legacy inventory", policy: domain.StartupRestorePreserveOnly, terminated: true, rowState: "active", wantStatus: domain.StatusTerminated},
		{name: "partial restore retains durable evidence", policy: domain.StartupRestorePreserveOnly, terminated: true, rowState: domain.SessionWorktreeStatePreservedPartial, wantStatus: domain.StatusTerminated, wantRecovery: true},
		{name: "absent preserved worktree remains exact evidence", policy: domain.StartupRestorePreserveOnly, terminated: true, rowState: domain.SessionWorktreeStatePreservedRemoved, wantStatus: domain.StatusTerminated, wantRecovery: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newFakeStore()
			id := domain.SessionID("mer-" + tt.name)
			st.sessions[id] = domain.SessionRecord{ID: id, ProjectID: "mer", IsTerminated: tt.terminated, Activity: domain.Activity{State: domain.ActivityIdle}}
			if !tt.projectGone {
				st.projects["mer"] = domain.ProjectRecord{ID: "mer", Config: domain.ProjectConfig{StartupRestorePolicy: tt.policy}, ConfigDecodeError: tt.configError}
			}
			if tt.rowState != "" {
				fakeRecoveryWorktrees[id] = []domain.SessionWorktreeRecord{{SessionID: id, RepoName: domain.RootWorkspaceRepoName, State: tt.rowState}}
			}
			t.Cleanup(func() { delete(fakeRecoveryWorktrees, id) })

			got, err := (&Service{store: st}).Get(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != tt.wantStatus || (got.Recovery != nil) != tt.wantRecovery {
				t.Fatalf("session = status %q recovery %#v, want status %q recovery=%t", got.Status, got.Recovery, tt.wantStatus, tt.wantRecovery)
			}
			if tt.wantRecovery && (len(got.Recovery.Worktrees) != 1 || got.Recovery.Worktrees[0].State != tt.rowState) {
				t.Fatalf("recovery worktrees = %#v, want exact state %q", got.Recovery.Worktrees, tt.rowState)
			}
		})
	}
}

func TestSessionRecoveryReportsMissingProviderIdentityWithoutExposingIt(t *testing.T) {
	st := newFakeStore()
	id := domain.SessionID("mer-missing-provider")
	st.sessions[id] = domain.SessionRecord{ID: id, ProjectID: "mer", IsTerminated: true, Metadata: domain.SessionMetadata{AgentSessionID: " \t "}}
	st.projects["mer"] = domain.ProjectRecord{ID: "mer", Config: domain.ProjectConfig{StartupRestorePolicy: domain.StartupRestorePreserveOnly}}
	fakeRecoveryWorktrees[id] = []domain.SessionWorktreeRecord{{SessionID: id, RepoName: domain.RootWorkspaceRepoName, State: domain.SessionWorktreeStatePreserved}}
	t.Cleanup(func() { delete(fakeRecoveryWorktrees, id) })

	got, err := (&Service{store: st}).Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Recovery == nil || got.Recovery.ProviderSessionSaved {
		t.Fatalf("recovery = %#v, want providerSessionSaved=false", got.Recovery)
	}
}

func TestSessionRecoveryPartialJournalReportsRuntimeUnknown(t *testing.T) {
	st := newFakeStore()
	id := domain.SessionID("mer-runtime-unknown")
	st.sessions[id] = domain.SessionRecord{ID: id, ProjectID: "mer", IsTerminated: false, Metadata: domain.SessionMetadata{RuntimeHandleID: "possibly-live"}}
	fakeRecoveryWorktrees[id] = []domain.SessionWorktreeRecord{{SessionID: id, RepoName: domain.RootWorkspaceRepoName, State: domain.SessionWorktreeStatePreservedPartial}}
	t.Cleanup(func() { delete(fakeRecoveryWorktrees, id) })
	got, err := (&Service{store: st}).Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Recovery == nil || got.Recovery.RuntimeState != domain.SessionRecoveryRuntimeUnknown || !got.IsTerminated || got.TerminalHandleID != "" {
		t.Fatalf("partial recovery projection = %#v", got)
	}
}

func TestTeardownProjectRefusesToStrandManagedRecovery(t *testing.T) {
	st := newFakeStore()
	id := domain.SessionID("mer-preserved")
	st.sessions[id] = domain.SessionRecord{ID: id, ProjectID: "mer", IsTerminated: true}
	fakeRecoveryWorktrees[id] = []domain.SessionWorktreeRecord{{SessionID: id, RepoName: domain.RootWorkspaceRepoName, State: domain.SessionWorktreeStatePreserved}}
	t.Cleanup(func() { delete(fakeRecoveryWorktrees, id) })
	err := (&Service{store: st}).TeardownProject(context.Background(), "mer")
	var got *apierr.Error
	if !errors.As(err, &got) || got.Code != "PROJECT_HAS_MANAGED_RECOVERY" {
		t.Fatalf("TeardownProject error = %T %v", err, err)
	}
}
