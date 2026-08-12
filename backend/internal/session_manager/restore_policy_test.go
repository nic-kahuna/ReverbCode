package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type exactFakeWorkspace struct {
	*fakeWorkspace
	preflightErr    error
	restoreExactErr error
	preflights      []ports.WorkspaceConfig
	restores        []ports.WorkspaceConfig
	dispositions    []ports.WorkspaceRecoveryDisposition
	restoreStarted  chan struct{}
	restoreRelease  chan struct{}
	restoreOnce     sync.Once
}

// fakeStore lives in the historical manager test file. Keep the new batch
// journal method beside the managed-recovery tests so SessionStart-owned test
// fixtures remain untouched.
func (f *fakeStore) UpsertSessionWorktrees(ctx context.Context, rows []domain.SessionWorktreeRecord) error {
	if f.upsertWTErr != nil {
		return f.upsertWTErr
	}
	for _, row := range rows {
		if err := f.UpsertSessionWorktree(ctx, row); err != nil {
			return err
		}
	}
	return nil
}

type recoveryJournalFaultStore struct {
	*fakeStore
	batchCalls     int
	failBatchAfter int
	deleteErr      error
}

type contextAwareRuntime struct {
	*fakeRuntime
	cancelOnCreate context.CancelFunc
}

func (r *contextAwareRuntime) Create(ctx context.Context, cfg ports.RuntimeConfig) (ports.RuntimeHandle, error) {
	handle, err := r.fakeRuntime.Create(ctx, cfg)
	if err == nil && r.cancelOnCreate != nil {
		r.cancelOnCreate()
	}
	return handle, err
}

func (r *contextAwareRuntime) Destroy(ctx context.Context, handle ports.RuntimeHandle) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.fakeRuntime.Destroy(ctx, handle)
}

type cleanupListSignalStore struct {
	*fakeStore
	listed  chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *cleanupListSignalStore) ListSessions(ctx context.Context, project domain.ProjectID) ([]domain.SessionRecord, error) {
	rows, err := s.fakeStore.ListSessions(ctx, project)
	block := false
	s.once.Do(func() {
		close(s.listed)
		block = true
	})
	if block {
		<-s.release
	}
	return rows, err
}

func (s *recoveryJournalFaultStore) UpsertSessionWorktrees(ctx context.Context, rows []domain.SessionWorktreeRecord) error {
	s.batchCalls++
	if s.failBatchAfter > 0 && s.batchCalls > s.failBatchAfter {
		return errors.New("injected atomic journal failure")
	}
	return s.fakeStore.UpsertSessionWorktrees(ctx, rows)
}

func (s *recoveryJournalFaultStore) DeleteSessionWorktrees(ctx context.Context, id domain.SessionID) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	return s.fakeStore.DeleteSessionWorktrees(ctx, id)
}

func (w *exactFakeWorkspace) ApplyPreservedExact(ctx context.Context, info ports.WorkspaceInfo, ref string) error {
	return w.ApplyPreserved(ctx, info, ref)
}

func (w *exactFakeWorkspace) DeletePreservedRef(context.Context, ports.WorkspaceInfo, string) error {
	return nil
}

func (w *exactFakeWorkspace) PreflightExactRestore(_ context.Context, cfg ports.WorkspaceConfig, disposition ports.WorkspaceRecoveryDisposition, _ string) error {
	w.preflights = append(w.preflights, cfg)
	w.dispositions = append(w.dispositions, disposition)
	return w.preflightErr
}

func (w *exactFakeWorkspace) RestoreExact(_ context.Context, cfg ports.WorkspaceConfig, _ ports.WorkspaceRecoveryDisposition, _ string) (ports.WorkspaceInfo, error) {
	w.restores = append(w.restores, cfg)
	if w.restoreStarted != nil {
		w.restoreOnce.Do(func() { close(w.restoreStarted) })
	}
	if w.restoreRelease != nil {
		<-w.restoreRelease
	}
	if w.restoreExactErr != nil {
		return ports.WorkspaceInfo{}, w.restoreExactErr
	}
	return ports.WorkspaceInfo{Path: cfg.Path, Branch: cfg.Branch, SessionID: cfg.SessionID, ProjectID: cfg.ProjectID, RepoPath: cfg.RepoPath}, nil
}

type managedSpyAgent struct {
	mu           sync.Mutex
	launchCalls  int
	restoreCalls int
	hookCalls    int
	restoreOK    bool
	restoreErr   error
	lastRestore  ports.RestoreConfig
}

func (a *managedSpyAgent) GetConfigSpec(context.Context) (ports.ConfigSpec, error) {
	return ports.ConfigSpec{}, nil
}

func (a *managedSpyAgent) GetLaunchCommand(context.Context, ports.LaunchConfig) ([]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.launchCalls++
	return []string{"fresh-launch"}, nil
}

func (a *managedSpyAgent) GetPromptDeliveryStrategy(context.Context, ports.LaunchConfig) (ports.PromptDeliveryStrategy, error) {
	return ports.PromptDeliveryAfterStart, nil
}

func (a *managedSpyAgent) GetAgentHooks(context.Context, ports.WorkspaceHookConfig) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.hookCalls++
	return nil
}

func (a *managedSpyAgent) GetRestoreCommand(_ context.Context, cfg ports.RestoreConfig) ([]string, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.restoreCalls++
	a.lastRestore = cfg
	if a.restoreErr != nil {
		return nil, false, a.restoreErr
	}
	if !a.restoreOK {
		return nil, false, nil
	}
	return []string{"native-resume", cfg.Session.Metadata[ports.MetadataKeyAgentSessionID]}, true, nil
}

func (a *managedSpyAgent) SessionInfo(context.Context, ports.SessionRef) (ports.SessionInfo, bool, error) {
	return ports.SessionInfo{}, false, nil
}

func (a *managedSpyAgent) ValidateAgentConfig(_ context.Context, cfg ports.AgentConfig) error {
	return cfg.Validate()
}

func (a *managedSpyAgent) counts() (launch, restore, hooks int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.launchCalls, a.restoreCalls, a.hookCalls
}

func newManagedPolicyManager() (*Manager, *fakeStore, *fakeRuntime, *exactFakeWorkspace, *managedSpyAgent, *fakeMessenger) {
	st := newFakeStore()
	st.projects["mer"] = domain.ProjectRecord{
		ID: "mer", Path: "/repos/mer",
		Config: domain.ProjectConfig{
			StartupRestorePolicy: domain.StartupRestorePreserveOnly,
			Worker:               domain.RoleOverride{Harness: domain.HarnessCodex},
			Orchestrator:         domain.RoleOverride{Harness: domain.HarnessCodex},
		},
	}
	rt := &fakeRuntime{aliveByHandle: map[string]bool{}}
	ws := &exactFakeWorkspace{fakeWorkspace: &fakeWorkspace{}}
	agent := &managedSpyAgent{restoreOK: true}
	msg := &fakeMessenger{}
	lookPath := func(string) (string, error) { return "/bin/true", nil }
	m := New(Deps{
		Runtime: rt, Agents: singleAgent{agent: agent}, Workspace: ws, Store: st,
		Messenger: msg, Lifecycle: &fakeLCM{store: st}, LookPath: lookPath,
	})
	return m, st, rt, ws, agent, msg
}

func managedSession(id domain.SessionID, terminated bool) domain.SessionRecord {
	requested := &domain.AgentRoute{Harness: domain.HarnessCodex, Model: "gpt-5.6-sol", ReasoningEffort: domain.ReasoningEffortHigh}
	launch := &domain.AgentLaunchRoute{Harness: requested.Harness, Model: requested.Model, ReasoningEffort: requested.ReasoningEffort}
	return domain.SessionRecord{
		ID: id, ProjectID: "mer", Kind: domain.KindWorker, Harness: domain.HarnessCodex,
		IsTerminated: terminated,
		Metadata: domain.SessionMetadata{
			Branch: "ao/" + string(id), WorkspacePath: "/managed/mer/" + string(id), RuntimeHandleID: "runtime-" + string(id),
			AgentSessionID: "provider-" + string(id), Prompt: "must never be replayed",
			RequestedRoute: requested, LaunchRoute: launch,
		},
	}
}

func managedRow(rec domain.SessionRecord, state string) domain.SessionWorktreeRecord {
	return domain.SessionWorktreeRecord{
		SessionID: rec.ID, RepoName: domain.RootWorkspaceRepoName, Branch: rec.Metadata.Branch,
		WorktreePath: rec.Metadata.WorkspacePath, State: state,
	}
}

func TestReconcilePreserveOnlyQuarantinesDeadRuntimeWithoutWorkspaceOrLaunch(t *testing.T) {
	m, st, rt, ws, agent, msg := newManagedPolicyManager()
	rec := managedSession("mer-1", false)
	st.sessions[rec.ID] = rec
	row := managedRow(rec, "active")
	row.BaseSHA = "base-before"
	row.PreservedRef = "refs/ao/preserved/mer-1"
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{row}

	if err := m.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := st.sessions[rec.ID]
	if !got.IsTerminated {
		t.Fatal("dead managed session was not quarantined")
	}
	rows := st.worktrees[rec.ID]
	if len(rows) != 1 || rows[0].State != domain.SessionWorktreeStatePreserved || rows[0].PreservedRef != row.PreservedRef || rows[0].WorktreePath != row.WorktreePath || rows[0].BaseSHA != row.BaseSHA {
		t.Fatalf("preserved journal = %#v, want physically proven in-place state", rows)
	}
	launches, restores, hooks := agent.counts()
	if rt.created != 0 || rt.destroyed != 0 || launches != 0 || restores != 0 || hooks != 0 || len(msg.msgs) != 0 {
		t.Fatalf("managed boot launched/mutated runtime: created=%d destroyed=%d launch=%d restore=%d hooks=%d messages=%v", rt.created, rt.destroyed, launches, restores, hooks, msg.msgs)
	}
	if ws.stashCalls != 0 || ws.destroyed != 0 || len(ws.calls) != 0 || len(ws.preflights) != 1 || len(ws.restores) != 0 {
		t.Fatalf("managed boot touched workspace: stash=%d destroy=%d calls=%v preflight=%d restore=%d", ws.stashCalls, ws.destroyed, ws.calls, len(ws.preflights), len(ws.restores))
	}
}

func TestReconcilePreserveOnlyProvesLegacyRemovedMarkerWithoutRestoring(t *testing.T) {
	m, st, rt, ws, agent, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", true)
	st.sessions[rec.ID] = rec
	row := managedRow(rec, "removed")
	row.PreservedRef = "refs/ao/preserved/mer-1"
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{row}

	if err := m.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := st.worktrees[rec.ID]; len(got) != 1 || got[0].State != domain.SessionWorktreeStatePreservedRemoved || got[0].PreservedRef != row.PreservedRef {
		t.Fatalf("journal = %#v, want physically proven preserved_removed with ref intact", got)
	}
	launches, restores, hooks := agent.counts()
	if rt.created != 0 || launches != 0 || restores != 0 || hooks != 0 || len(ws.preflights) != 1 || len(ws.restores) != 0 || len(ws.calls) != 0 {
		t.Fatalf("removed managed marker was consumed: runtime=%d launch=%d restore=%d hooks=%d workspace=%v", rt.created, launches, restores, hooks, ws.calls)
	}
}

func TestReconcilePreserveOnlyDoesNotPromoteTerminalActiveWorkspaceInventory(t *testing.T) {
	m, st, rt, ws, agent, _ := newManagedPolicyManager()
	project := st.projects["mer"]
	project.Kind = domain.ProjectKindWorkspace
	st.projects["mer"] = project
	st.workspaceRepo["mer"] = []domain.WorkspaceRepoRecord{{ProjectID: "mer", Name: "api", RelativePath: "api"}}
	rec := managedSession("mer-ended", true)
	st.sessions[rec.ID] = rec
	root := managedRow(rec, "active")
	child := root
	child.RepoName = "api"
	child.WorktreePath += "/api"
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{root, child}

	if err := m.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := st.worktrees[rec.ID]
	if len(got) != 2 || got[0].State != "active" || got[1].State != "active" || !st.sessions[rec.ID].IsTerminated {
		t.Fatalf("ended workspace inventory changed: session=%#v rows=%#v", st.sessions[rec.ID], got)
	}
	launches, restores, hooks := agent.counts()
	if rt.created != 0 || rt.destroyed != 0 || ws.stashCalls != 0 || ws.destroyed != 0 || len(ws.preflights) != 0 || len(ws.restores) != 0 || len(ws.calls) != 0 || launches != 0 || restores != 0 || hooks != 0 {
		t.Fatal("ordinary terminal active inventory was treated as managed recovery")
	}
}

func TestReconcilePreserveOnlyPhysicalProofFailurePublishesPartial(t *testing.T) {
	m, st, rt, ws, agent, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", false)
	st.sessions[rec.ID] = rec
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{managedRow(rec, "active")}
	ws.preflightErr = ports.ErrWorkspaceRestoreAmbiguous
	if err := m.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	rows := st.worktrees[rec.ID]
	if len(rows) != 1 || rows[0].State != domain.SessionWorktreeStatePreservedPartial || !st.sessions[rec.ID].IsTerminated {
		t.Fatalf("ambiguous disposition = session %#v rows %#v", st.sessions[rec.ID], rows)
	}
	launches, restores, hooks := agent.counts()
	if rt.created != 0 || rt.destroyed != 0 || launches != 0 || restores != 0 || hooks != 0 || len(ws.restores) != 0 || len(ws.calls) != 0 {
		t.Fatal("ambiguous physical proof crossed a mutation boundary")
	}
}

func TestReconcilePreserveOnlySynthesizesAndProvesCleanInPlaceJournal(t *testing.T) {
	m, st, _, ws, _, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", false)
	st.sessions[rec.ID] = rec
	if err := m.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	rows := st.worktrees[rec.ID]
	if len(rows) != 1 || rows[0].State != domain.SessionWorktreeStatePreserved || rows[0].Branch != rec.Metadata.Branch || rows[0].WorktreePath != rec.Metadata.WorkspacePath {
		t.Fatalf("synthetic in-place journal = %#v", rows)
	}
	if len(ws.preflights) != 1 || len(ws.restores) != 0 || len(ws.calls) != 0 {
		t.Fatalf("physical proof = preflight %d restore %d calls %v", len(ws.preflights), len(ws.restores), ws.calls)
	}
}

func TestReconcilePreserveOnlyAdoptsExactLiveRuntime(t *testing.T) {
	m, st, rt, ws, agent, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", false)
	st.sessions[rec.ID] = rec
	rt.aliveByHandle[rec.Metadata.RuntimeHandleID] = true

	if err := m.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if st.sessions[rec.ID].IsTerminated {
		t.Fatal("surviving exact runtime must be adopted")
	}
	launches, restores, hooks := agent.counts()
	if rt.created != 0 || launches != 0 || restores != 0 || hooks != 0 || len(ws.calls) != 0 {
		t.Fatalf("adoption performed launch/workspace work: runtime=%d launch=%d restore=%d hooks=%d workspace=%v", rt.created, launches, restores, hooks, ws.calls)
	}
}

func TestReconcilePreserveOnlyProbeErrorJournalsRuntimeUnknownWithoutWorkspaceMutation(t *testing.T) {
	m, st, rt, ws, agent, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", false)
	st.sessions[rec.ID] = rec
	rt.aliveErr = errors.New("probe unavailable")

	if err := m.Reconcile(ctx); err == nil {
		t.Fatal("Reconcile succeeded despite runtime probe uncertainty")
	}
	if st.sessions[rec.ID].IsTerminated || len(st.worktrees[rec.ID]) != 1 || st.worktrees[rec.ID][0].State != domain.SessionWorktreeStatePreservedPartial {
		t.Fatalf("probe error journal = session %#v rows %#v, want raw-live + partial", st.sessions[rec.ID], st.worktrees[rec.ID])
	}
	launches, restores, hooks := agent.counts()
	if rt.created != 0 || launches != 0 || restores != 0 || hooks != 0 || len(ws.calls) != 0 {
		t.Fatalf("probe error launched/touched workspace: runtime=%d launch=%d restore=%d hooks=%d calls=%v", rt.created, launches, restores, hooks, ws.calls)
	}
}

func TestReconcilePreserveOnlyTerminalLegacyMarkerNeverReapsAmbiguousRuntime(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*domain.SessionRecord, *fakeRuntime)
	}{
		{name: "runtime alive", mutate: func(rec *domain.SessionRecord, rt *fakeRuntime) {
			rt.aliveByHandle[rec.Metadata.RuntimeHandleID] = true
		}},
		{name: "runtime probe error", mutate: func(_ *domain.SessionRecord, rt *fakeRuntime) {
			rt.aliveErr = errors.New("probe unavailable")
		}},
		{name: "runtime handle missing", mutate: func(rec *domain.SessionRecord, _ *fakeRuntime) {
			rec.Metadata.RuntimeHandleID = ""
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, st, rt, ws, agent, _ := newManagedPolicyManager()
			rec := managedSession("mer-terminal", true)
			tt.mutate(&rec, rt)
			st.sessions[rec.ID] = rec
			row := managedRow(rec, "removed")
			row.PreservedRef = "refs/ao/preserved/mer-terminal"
			st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{row}

			if err := m.Reconcile(ctx); err == nil {
				t.Fatal("Reconcile succeeded without proving runtime absence")
			}
			got := st.worktrees[rec.ID]
			if len(got) != 1 || got[0].State != domain.SessionWorktreeStatePreservedPartial || got[0].PreservedRef != row.PreservedRef {
				t.Fatalf("ambiguous runtime journal = %#v, want preserved_partial", got)
			}
			launches, restores, hooks := agent.counts()
			if rt.destroyed != 0 || rt.created != 0 || launches != 0 || restores != 0 || hooks != 0 || len(ws.preflights) != 0 || len(ws.restores) != 0 || len(ws.calls) != 0 {
				t.Fatalf("ambiguous terminal runtime crossed mutation boundary: runtime=%d/%d provider=%d/%d/%d preflight=%d restore=%d calls=%v", rt.created, rt.destroyed, launches, restores, hooks, len(ws.preflights), len(ws.restores), ws.calls)
			}
		})
	}
}

func TestReconcilePreserveOnlyQuarantinesAmbiguousJournalStatesPermanently(t *testing.T) {
	for _, initial := range []string{"retry_remove", "unavailable", "stray_moved"} {
		t.Run(initial, func(t *testing.T) {
			m, st, rt, ws, agent, _ := newManagedPolicyManager()
			rec := managedSession("mer-1", false)
			st.sessions[rec.ID] = rec
			row := managedRow(rec, initial)
			row.PreservedRef = "refs/ao/preserved/mer-1"
			st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{row}

			if err := m.Reconcile(ctx); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if !st.sessions[rec.ID].IsTerminated {
				t.Fatal("dead managed session was not marked terminal")
			}
			got := st.worktrees[rec.ID]
			if len(got) != 1 || got[0].State != domain.SessionWorktreeStatePreservedPartial || got[0].PreservedRef != row.PreservedRef {
				t.Fatalf("ambiguous journal = %#v, want preserved_partial/ref", got)
			}
			launches, restores, hooks := agent.counts()
			if rt.created != 0 || rt.destroyed != 0 || launches != 0 || restores != 0 || hooks != 0 || len(ws.calls) != 0 {
				t.Fatalf("ambiguous journal caused mutation: runtime=%d/%d provider=%d/%d/%d workspace=%v", rt.created, rt.destroyed, launches, restores, hooks, ws.calls)
			}

			// Mutable policy loss cannot demote managed provenance into historical
			// automatic restore or routine cleanup permission.
			project := st.projects["mer"]
			project.Config.StartupRestorePolicy = domain.StartupRestoreAutomatic
			st.projects["mer"] = project
			if _, err := m.Restore(ctx, rec.ID); err == nil {
				t.Fatal("partial managed journal restored after policy rewrite")
			}
			if err := m.RestoreAll(ctx); err != nil {
				t.Fatalf("RestoreAll: %v", err)
			}
			cleanup, err := m.Cleanup(ctx, rec.ProjectID)
			if err != nil || len(cleanup.Cleaned) != 0 || len(cleanup.Skipped) != 1 {
				t.Fatalf("Cleanup = %#v, %v", cleanup, err)
			}
			if rt.created != 0 || rt.destroyed != 0 || ws.destroyed != 0 || len(st.worktrees[rec.ID]) != 1 {
				t.Fatalf("policy rewrite consumed partial marker: runtime=%d/%d workspace=%d rows=%#v", rt.created, rt.destroyed, ws.destroyed, st.worktrees[rec.ID])
			}
		})
	}
}

func TestReconcilePreserveOnlyJournalFailureDoesNotClaimRecovery(t *testing.T) {
	m, st, rt, ws, agent, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", false)
	st.sessions[rec.ID] = rec
	st.upsertWTErr = errors.New("storage unavailable")

	if err := m.Reconcile(ctx); err == nil {
		t.Fatal("Reconcile succeeded despite journal failure")
	}
	if st.sessions[rec.ID].IsTerminated || len(st.worktrees[rec.ID]) != 0 {
		t.Fatalf("failed journal claimed recovery: session=%#v rows=%#v", st.sessions[rec.ID], st.worktrees[rec.ID])
	}
	launches, restores, hooks := agent.counts()
	if rt.created != 0 || rt.destroyed != 0 || launches != 0 || restores != 0 || hooks != 0 || len(ws.calls) != 0 {
		t.Fatalf("journal failure caused mutation: runtime=%d/%d provider=%d/%d/%d workspace=%v", rt.created, rt.destroyed, launches, restores, hooks, ws.calls)
	}
}

func TestReconcileCorruptProjectPolicyFailsClosed(t *testing.T) {
	m, st, rt, ws, agent, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", false)
	st.sessions[rec.ID] = rec
	project := st.projects["mer"]
	project.Config = domain.ProjectConfig{}
	project.ConfigDecodeError = "invalid stored JSON"
	st.projects["mer"] = project

	if err := m.Reconcile(ctx); err == nil {
		t.Fatal("Reconcile succeeded despite corrupt policy")
	}
	if st.sessions[rec.ID].IsTerminated || len(st.worktrees[rec.ID]) != 0 {
		t.Fatalf("corrupt policy changed state: session=%#v rows=%#v", st.sessions[rec.ID], st.worktrees[rec.ID])
	}
	launches, restores, hooks := agent.counts()
	if rt.created != 0 || launches != 0 || restores != 0 || hooks != 0 || len(ws.calls) != 0 {
		t.Fatalf("corrupt policy launched/touched workspace: runtime=%d launch=%d restore=%d hooks=%d calls=%v", rt.created, launches, restores, hooks, ws.calls)
	}
}

func TestReconcilePreserveOnlyMissingRuntimeHandleJournalsPartial(t *testing.T) {
	m, st, rt, ws, agent, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", false)
	rec.Metadata.RuntimeHandleID = ""
	st.sessions[rec.ID] = rec
	if err := m.Reconcile(ctx); err == nil {
		t.Fatal("Reconcile succeeded despite missing runtime handle")
	}
	rows := st.worktrees[rec.ID]
	if len(rows) != 1 || rows[0].State != domain.SessionWorktreeStatePreservedPartial || st.sessions[rec.ID].IsTerminated {
		t.Fatalf("missing-handle recovery = session %#v rows %#v", st.sessions[rec.ID], rows)
	}
	launches, restores, hooks := agent.counts()
	if rt.created != 0 || rt.destroyed != 0 || launches != 0 || restores != 0 || hooks != 0 || len(ws.calls) != 0 {
		t.Fatalf("missing-handle recovery mutated runtime/provider/workspace")
	}
}

func TestReconcilePreserveOnlyIncompleteWorkspaceIdentityHoldsStartupGate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*domain.SessionRecord)
		withRow bool
	}{
		{name: "missing path", mutate: func(rec *domain.SessionRecord) { rec.Metadata.WorkspacePath = "" }},
		{name: "missing branch", mutate: func(rec *domain.SessionRecord) { rec.Metadata.Branch = "" }},
		{name: "missing path and branch", mutate: func(rec *domain.SessionRecord) {
			rec.Metadata.WorkspacePath = ""
			rec.Metadata.Branch = ""
		}},
		{name: "existing row with incomplete metadata", withRow: true, mutate: func(rec *domain.SessionRecord) { rec.Metadata.WorkspacePath = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, st, rt, ws, agent, _ := newManagedPolicyManager()
			rec := managedSession("mer-incomplete", false)
			row := managedRow(rec, "active")
			tt.mutate(&rec)
			st.sessions[rec.ID] = rec
			if tt.withRow {
				st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{row}
			}

			if err := m.Reconcile(ctx); err == nil {
				t.Fatal("Reconcile succeeded with incomplete managed identity")
			}
			gotRows := st.worktrees[rec.ID]
			if len(gotRows) != 1 || gotRows[0].State != domain.SessionWorktreeStatePreservedPartial || gotRows[0].RepoName != domain.RootWorkspaceRepoName {
				t.Fatalf("journal = %#v, want root preserved_partial evidence", gotRows)
			}
			launches, restores, hooks := agent.counts()
			if st.sessions[rec.ID].IsTerminated || rt.created != 0 || rt.destroyed != 0 || ws.stashCalls != 0 || ws.destroyed != 0 || len(ws.preflights) != 0 || len(ws.restores) != 0 || launches != 0 || restores != 0 || hooks != 0 {
				t.Fatal("incomplete identity crossed a mutation boundary")
			}
		})
	}
}

func TestManagedAdmissionBlocksOnlyCollidingFreshWork(t *testing.T) {
	t.Run("canonical orchestrator", func(t *testing.T) {
		m, st, rt, ws, agent, _ := newManagedPolicyManager()
		rec := managedSession("mer-old-orchestrator", true)
		rec.Kind = domain.KindOrchestrator
		st.sessions[rec.ID] = rec
		st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{managedRow(rec, domain.SessionWorktreeStatePreserved)}
		_, err := m.Spawn(ctx, ports.SpawnConfig{ProjectID: "mer", Kind: domain.KindOrchestrator})
		if !errors.Is(err, ErrRecoveryAdmission) {
			t.Fatalf("Spawn error = %v", err)
		}
		launches, _, hooks := agent.counts()
		if rt.created != 0 || ws.lastCfg.SessionID != "" || launches != 0 || hooks != 0 || len(st.sessions) != 1 {
			t.Fatal("colliding orchestrator admission mutated state")
		}
	})
	t.Run("same issue worker blocked unrelated worker allowed", func(t *testing.T) {
		m, st, rt, _, _, _ := newManagedPolicyManager()
		rec := managedSession("mer-old-worker", true)
		rec.IssueID = "ticket-1"
		st.sessions[rec.ID] = rec
		st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{managedRow(rec, domain.SessionWorktreeStatePreserved)}
		if _, err := m.Spawn(ctx, ports.SpawnConfig{ProjectID: "mer", Kind: domain.KindWorker, IssueID: "ticket-1"}); !errors.Is(err, ErrRecoveryAdmission) {
			t.Fatalf("same-issue Spawn error = %v", err)
		}
		if _, err := m.Spawn(ctx, ports.SpawnConfig{ProjectID: "mer", Kind: domain.KindWorker, IssueID: "ticket-2"}); err != nil {
			t.Fatalf("unrelated Spawn: %v", err)
		}
		if rt.created != 1 {
			t.Fatalf("runtime creates = %d, want unrelated worker only", rt.created)
		}
	})
}

func TestManagedAdmissionRejectsCorruptProjectConfigBeforeSpawnWrites(t *testing.T) {
	m, st, rt, ws, agent, _ := newManagedPolicyManager()
	project := st.projects["mer"]
	project.ConfigDecodeError = "unknown startupRestorePolicy"
	st.projects["mer"] = project
	_, err := m.Spawn(ctx, ports.SpawnConfig{ProjectID: "mer", Kind: domain.KindWorker, Harness: domain.HarnessCodex})
	if !errors.Is(err, ErrProjectConfigInvalid) {
		t.Fatalf("Spawn error = %v", err)
	}
	launches, _, hooks := agent.counts()
	if len(st.sessions) != 0 || rt.created != 0 || ws.lastCfg.SessionID != "" || launches != 0 || hooks != 0 {
		t.Fatal("corrupt project config fell back into spawn")
	}
}

func TestManagedAdmissionBlocksRetireAndSendWithoutMutation(t *testing.T) {
	m, st, rt, ws, _, msg := newManagedPolicyManager()
	rec := managedSession("mer-orchestrator", false)
	rec.Kind = domain.KindOrchestrator
	st.sessions[rec.ID] = rec
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{managedRow(rec, domain.SessionWorktreeStatePreservedPartial)}
	if err := m.RetireForReplacement(ctx, rec.ID); !errors.Is(err, ErrRecoveryAdmission) {
		t.Fatalf("RetireForReplacement error = %v", err)
	}
	if err := m.Send(ctx, rec.ID, "must not land"); !errors.Is(err, ErrRecoveryAdmission) {
		t.Fatalf("Send error = %v", err)
	}
	if rt.destroyed != 0 || ws.destroyed != 0 || ws.stashCalls != 0 || len(msg.msgs) != 0 || len(st.worktrees[rec.ID]) != 1 {
		t.Fatal("managed admission mutated recovery evidence")
	}
}

func TestReconcileUnknownProjectPolicyNeverReapsOrRestores(t *testing.T) {
	m, st, rt, ws, agent, _ := newManagedPolicyManager()
	project := st.projects["mer"]
	project.Config.StartupRestorePolicy = domain.StartupRestorePolicy("future_policy")
	st.projects["mer"] = project
	live := managedSession("mer-live", false)
	terminal := managedSession("mer-terminal", true)
	st.sessions[live.ID], st.sessions[terminal.ID] = live, terminal
	st.worktrees[terminal.ID] = []domain.SessionWorktreeRecord{managedRow(terminal, "removed")}
	rt.aliveByHandle[terminal.Metadata.RuntimeHandleID] = true

	if err := m.Reconcile(ctx); err == nil {
		t.Fatal("Reconcile succeeded despite unknown policy")
	}
	if st.sessions[live.ID].IsTerminated || !st.sessions[terminal.ID].IsTerminated {
		t.Fatalf("unknown policy changed sessions: live=%#v terminal=%#v", st.sessions[live.ID], st.sessions[terminal.ID])
	}
	if rt.created != 0 || rt.destroyed != 0 || len(ws.calls) != 0 {
		t.Fatalf("unknown policy mutated runtime/workspace: created=%d destroyed=%d calls=%v", rt.created, rt.destroyed, ws.calls)
	}
	launches, restores, hooks := agent.counts()
	if launches != 0 || restores != 0 || hooks != 0 {
		t.Fatalf("unknown policy touched provider: launch=%d restore=%d hooks=%d", launches, restores, hooks)
	}
}

func TestReconcileManagedJournalWithMissingProjectNeverReapsOrRestores(t *testing.T) {
	m, st, rt, ws, agent, _ := newManagedPolicyManager()
	delete(st.projects, "mer")
	rec := managedSession("mer-1", true)
	st.sessions[rec.ID] = rec
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{managedRow(rec, domain.SessionWorktreeStatePreserved)}
	rt.aliveByHandle[rec.Metadata.RuntimeHandleID] = true

	if err := m.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !st.sessions[rec.ID].IsTerminated || rt.created != 0 || rt.destroyed != 0 || len(ws.calls) != 0 {
		t.Fatalf("missing project changed recovery: session=%#v runtime=%d/%d workspace=%v", st.sessions[rec.ID], rt.created, rt.destroyed, ws.calls)
	}
	launches, restores, hooks := agent.counts()
	if launches != 0 || restores != 0 || hooks != 0 {
		t.Fatalf("missing project touched provider: launch=%d restore=%d hooks=%d", launches, restores, hooks)
	}

	_, err := m.Restore(ctx, rec.ID)
	var failure *RecoveryFailure
	if !errors.As(err, &failure) || failure.Code != "project_not_exact" {
		t.Fatalf("Restore error = %v / %#v, want project_not_exact", err, failure)
	}
	if rt.created != 0 || rt.destroyed != 0 || len(ws.restores) != 0 {
		t.Fatal("missing-project explicit restore mutated recovery state")
	}
}

func TestReconcileAutomaticDefaultRetainsLegacyBulkRestore(t *testing.T) {
	m, st, rt, ws := newManager()
	rec := managedSession("mer-1", false)
	rec.Harness = domain.HarnessClaudeCode
	rec.Metadata.RequestedRoute = nil
	rec.Metadata.LaunchRoute = &domain.AgentLaunchRoute{Harness: domain.HarnessClaudeCode}
	st.sessions[rec.ID] = rec
	ws.stashRef = "refs/ao/preserved/mer-1"

	if err := m.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if st.sessions[rec.ID].IsTerminated || rt.created != 1 {
		t.Fatalf("automatic legacy recovery = terminated %v runtime creates %d, want live/1", st.sessions[rec.ID].IsTerminated, rt.created)
	}
	if ws.stashCalls != 1 || !containsCall(ws.calls, "ForceDestroy:mer-1") {
		t.Fatalf("automatic legacy workspace sequence = stash %d calls %v", ws.stashCalls, ws.calls)
	}
}

func TestReconcileAutomaticDefaultRetainsLegacyMissingProjectBehavior(t *testing.T) {
	m, st, rt, ws := newManager()
	delete(st.projects, "mer")
	rec := managedSession("mer-1", false)
	rec.Harness = domain.HarnessClaudeCode
	rec.Metadata.RequestedRoute = nil
	rec.Metadata.LaunchRoute = &domain.AgentLaunchRoute{Harness: domain.HarnessClaudeCode}
	st.sessions[rec.ID] = rec
	ws.stashRef = "refs/ao/preserved/mer-1"

	if err := m.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if st.sessions[rec.ID].IsTerminated || rt.created != 1 || ws.stashCalls != 1 || !containsCall(ws.calls, "ForceDestroy:mer-1") {
		t.Fatalf("legacy missing-project recovery changed: session=%#v runtime=%d stash=%d calls=%v", st.sessions[rec.ID], rt.created, ws.stashCalls, ws.calls)
	}
}

func TestManagedExplicitRestoreResumesOnlyNamedExactSession(t *testing.T) {
	m, st, rt, ws, agent, msg := newManagedPolicyManager()
	rec := managedSession("mer-1", true)
	other := managedSession("mer-2", true)
	st.sessions[rec.ID], st.sessions[other.ID] = rec, other
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{managedRow(rec, domain.SessionWorktreeStatePreserved)}
	st.worktrees[other.ID] = []domain.SessionWorktreeRecord{managedRow(other, domain.SessionWorktreeStatePreserved)}

	got, err := m.Restore(ctx, rec.ID)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got.IsTerminated || rt.created != 1 || rt.lastCfg.SessionID != rec.ID {
		t.Fatalf("restored session/runtime = %#v / %#v", got, rt.lastCfg)
	}
	if !st.sessions[other.ID].IsTerminated || len(st.worktrees[other.ID]) != 1 {
		t.Fatal("explicit restore touched an unselected session")
	}
	if len(st.worktrees[rec.ID]) != 0 {
		t.Fatalf("successful single-repo restore did not consume marker: %#v", st.worktrees[rec.ID])
	}
	launches, restores, hooks := agent.counts()
	if launches != 0 || restores != 1 || hooks != 1 || len(msg.msgs) != 0 {
		t.Fatalf("provider calls launch=%d restore=%d hooks=%d messages=%v", launches, restores, hooks, msg.msgs)
	}
	if got.Metadata.AgentSessionID != rec.Metadata.AgentSessionID || got.Metadata.RequestedRoute == nil || *got.Metadata.RequestedRoute != *rec.Metadata.RequestedRoute || got.Metadata.LaunchRoute == nil || *got.Metadata.LaunchRoute != *rec.Metadata.LaunchRoute {
		t.Fatalf("persisted provider/route identity changed: %#v", got.Metadata)
	}
	if len(ws.preflights) != 1 || len(ws.restores) != 1 || ws.dispositions[0] != ports.WorkspaceRecoveryInPlace {
		t.Fatalf("exact workspace calls preflight=%d restore=%d dispositions=%v", len(ws.preflights), len(ws.restores), ws.dispositions)
	}
	if len(rt.lastCfg.Argv) != 2 || rt.lastCfg.Argv[0] != "native-resume" || rt.lastCfg.Argv[1] != rec.Metadata.AgentSessionID {
		t.Fatalf("runtime argv = %v, want native saved-provider resume", rt.lastCfg.Argv)
	}
}

func TestManagedExplicitRestoreNormalizesStaleLiveFactOnlyAfterRuntimeAbsenceProof(t *testing.T) {
	m, st, rt, _, _, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", false)
	st.sessions[rec.ID] = rec
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{managedRow(rec, domain.SessionWorktreeStatePreserved)}
	got, err := m.Restore(ctx, rec.ID)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got.IsTerminated || rt.created != 1 || len(st.worktrees[rec.ID]) != 0 {
		t.Fatalf("stale-live restore = session %#v runtime %d rows %#v", got, rt.created, st.worktrees[rec.ID])
	}
}

func TestManagedExplicitRestoreStaleLiveRuntimeStillAliveFailsBeforeMutation(t *testing.T) {
	m, st, rt, ws, agent, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", false)
	st.sessions[rec.ID] = rec
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{managedRow(rec, domain.SessionWorktreeStatePreserved)}
	rt.aliveByHandle[rec.Metadata.RuntimeHandleID] = true
	_, err := m.Restore(ctx, rec.ID)
	var failure *RecoveryFailure
	if !errors.As(err, &failure) || failure.Code != "runtime_still_alive" || failure.Phase != "preflight" {
		t.Fatalf("Restore error = %v / %#v", err, failure)
	}
	launches, restores, hooks := agent.counts()
	if rt.created != 0 || len(ws.preflights) != 0 || launches != 0 || restores != 0 || hooks != 0 || st.sessions[rec.ID].IsTerminated {
		t.Fatal("live-runtime preflight mutated recovery")
	}
}

func TestManagedExplicitRestoreStaleLivePreflightFailureDoesNotTerminalize(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*fakeStore, *domain.SessionRecord, *domain.SessionWorktreeRecord)
		wantCode string
	}{
		{name: "partial journal", mutate: func(_ *fakeStore, _ *domain.SessionRecord, row *domain.SessionWorktreeRecord) {
			row.State = domain.SessionWorktreeStatePreservedPartial
		}, wantCode: "recovery_journal_partial"},
		{name: "missing project", mutate: func(st *fakeStore, _ *domain.SessionRecord, _ *domain.SessionWorktreeRecord) {
			delete(st.projects, "mer")
		}, wantCode: "project_not_exact"},
		{name: "missing launch route", mutate: func(_ *fakeStore, rec *domain.SessionRecord, _ *domain.SessionWorktreeRecord) {
			rec.Metadata.LaunchRoute = nil
		}, wantCode: "launch_route_missing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, st, rt, ws, agent, _ := newManagedPolicyManager()
			rec := managedSession("mer-stale", false)
			row := managedRow(rec, domain.SessionWorktreeStatePreserved)
			tt.mutate(st, &rec, &row)
			st.sessions[rec.ID] = rec
			st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{row}

			_, err := m.Restore(ctx, rec.ID)
			var failure *RecoveryFailure
			if !errors.As(err, &failure) || failure.Code != tt.wantCode || failure.Phase != "preflight" {
				t.Fatalf("Restore error = %v / %#v, want preflight %q", err, failure, tt.wantCode)
			}
			launches, restores, hooks := agent.counts()
			if st.sessions[rec.ID].IsTerminated || rt.created != 0 || rt.destroyed != 0 || len(ws.restores) != 0 || len(ws.calls) != 0 || launches != 0 || restores != 0 || hooks != 0 {
				t.Fatal("failed stale-live preflight terminalized or mutated recovery")
			}
		})
	}
}

func TestManagedExplicitRestoreAllowsNoRequestedRoute(t *testing.T) {
	m, st, rt, _, agent, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", true)
	rec.Metadata.RequestedRoute = nil
	st.sessions[rec.ID] = rec
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{managedRow(rec, domain.SessionWorktreeStatePreserved)}

	got, err := m.Restore(ctx, rec.ID)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got.Metadata.RequestedRoute != nil || got.Metadata.LaunchRoute == nil || rt.created != 1 {
		t.Fatalf("restore route/runtime = requested %#v launch %#v creates %d", got.Metadata.RequestedRoute, got.Metadata.LaunchRoute, rt.created)
	}
	launches, restores, _ := agent.counts()
	if launches != 0 || restores != 1 {
		t.Fatalf("provider calls launch=%d restore=%d, want native restore only", launches, restores)
	}
}

func TestManagedExplicitRestorePreservesDistinctRequestedAndLaunchRoutes(t *testing.T) {
	m, st, rt, _, agent, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", true)
	rec.Metadata.RequestedRoute.Model = "requested-model"
	rec.Metadata.RequestedRoute.ReasoningEffort = domain.ReasoningEffortHigh
	rec.Metadata.LaunchRoute.Model = "launched-model"
	rec.Metadata.LaunchRoute.ReasoningEffort = domain.ReasoningEffortMedium
	st.sessions[rec.ID] = rec
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{managedRow(rec, domain.SessionWorktreeStatePreserved)}

	got, err := m.Restore(ctx, rec.ID)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got.Metadata.RequestedRoute == nil || *got.Metadata.RequestedRoute != *rec.Metadata.RequestedRoute || got.Metadata.LaunchRoute == nil || *got.Metadata.LaunchRoute != *rec.Metadata.LaunchRoute {
		t.Fatalf("route facts changed: requested=%#v launch=%#v", got.Metadata.RequestedRoute, got.Metadata.LaunchRoute)
	}
	if rt.created != 1 || agent.lastRestore.Config.Model != "launched-model" || agent.lastRestore.Config.ReasoningEffort != domain.ReasoningEffortMedium {
		t.Fatalf("restore route = runtime %d config %#v, want persisted launch observation", rt.created, agent.lastRestore.Config)
	}
}

func TestManagedExplicitRestoreRejectsUnknownProjectKind(t *testing.T) {
	m, st, rt, ws, _, _ := newManagedPolicyManager()
	project := st.projects["mer"]
	project.Kind = domain.ProjectKind("future_kind")
	st.projects["mer"] = project
	rec := managedSession("mer-1", true)
	st.sessions[rec.ID] = rec
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{managedRow(rec, domain.SessionWorktreeStatePreserved)}

	_, err := m.Restore(ctx, rec.ID)
	var failure *RecoveryFailure
	if !errors.As(err, &failure) || failure.Code != "project_kind_invalid" {
		t.Fatalf("Restore error = %v / %#v, want project_kind_invalid", err, failure)
	}
	if rt.created != 0 || len(ws.preflights) != 0 || len(ws.restores) != 0 || len(ws.calls) != 0 {
		t.Fatalf("unknown project kind mutated state: runtime=%d preflight=%d restore=%d calls=%v", rt.created, len(ws.preflights), len(ws.restores), ws.calls)
	}
}

func TestManagedExplicitRestoreFailClosedPreflightMatrix(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*domain.SessionRecord, *domain.SessionWorktreeRecord, *fakeRuntime, *exactFakeWorkspace, *managedSpyAgent)
		wantCode string
	}{
		{name: "saved provider id missing", mutate: func(rec *domain.SessionRecord, _ *domain.SessionWorktreeRecord, _ *fakeRuntime, _ *exactFakeWorkspace, _ *managedSpyAgent) {
			rec.Metadata.AgentSessionID = ""
		}, wantCode: "provider_session_missing"},
		{name: "runtime handle missing", mutate: func(rec *domain.SessionRecord, _ *domain.SessionWorktreeRecord, _ *fakeRuntime, _ *exactFakeWorkspace, _ *managedSpyAgent) {
			rec.Metadata.RuntimeHandleID = ""
		}, wantCode: "runtime_handle_missing"},
		{name: "runtime probe fails", mutate: func(_ *domain.SessionRecord, _ *domain.SessionWorktreeRecord, rt *fakeRuntime, _ *exactFakeWorkspace, _ *managedSpyAgent) {
			rt.aliveErr = errors.New("probe unavailable")
		}, wantCode: "runtime_probe_failed"},
		{name: "session root identity missing", mutate: func(rec *domain.SessionRecord, _ *domain.SessionWorktreeRecord, _ *fakeRuntime, _ *exactFakeWorkspace, _ *managedSpyAgent) {
			rec.Metadata.WorkspacePath = ""
		}, wantCode: "worktree_identity_missing"},
		{name: "session root identity mismatch", mutate: func(rec *domain.SessionRecord, _ *domain.SessionWorktreeRecord, _ *fakeRuntime, _ *exactFakeWorkspace, _ *managedSpyAgent) {
			rec.Metadata.WorkspacePath += "-different"
		}, wantCode: "worktree_identity_mismatch"},
		{name: "legacy launch route null", mutate: func(rec *domain.SessionRecord, _ *domain.SessionWorktreeRecord, _ *fakeRuntime, _ *exactFakeWorkspace, _ *managedSpyAgent) {
			rec.Metadata.LaunchRoute = nil
		}, wantCode: "launch_route_missing"},
		{name: "launch harness mismatch", mutate: func(rec *domain.SessionRecord, _ *domain.SessionWorktreeRecord, _ *fakeRuntime, _ *exactFakeWorkspace, _ *managedSpyAgent) {
			rec.Metadata.LaunchRoute.Harness = domain.HarnessClaudeCode
		}, wantCode: "launch_route_mismatch"},
		{name: "requested route invalid", mutate: func(rec *domain.SessionRecord, _ *domain.SessionWorktreeRecord, _ *fakeRuntime, _ *exactFakeWorkspace, _ *managedSpyAgent) {
			rec.Metadata.RequestedRoute.Model = ""
		}, wantCode: "requested_route_invalid"},
		{name: "runtime still alive", mutate: func(rec *domain.SessionRecord, _ *domain.SessionWorktreeRecord, rt *fakeRuntime, _ *exactFakeWorkspace, _ *managedSpyAgent) {
			rt.aliveByHandle[rec.Metadata.RuntimeHandleID] = true
		}, wantCode: "runtime_still_alive"},
		{name: "partial worktree journal", mutate: func(_ *domain.SessionRecord, row *domain.SessionWorktreeRecord, _ *fakeRuntime, _ *exactFakeWorkspace, _ *managedSpyAgent) {
			row.State = "active"
		}, wantCode: "recovery_journal_partial"},
		{name: "worktree conflict", mutate: func(_ *domain.SessionRecord, _ *domain.SessionWorktreeRecord, _ *fakeRuntime, ws *exactFakeWorkspace, _ *managedSpyAgent) {
			ws.preflightErr = fmt.Errorf("%w: stray path", ports.ErrWorkspaceRestoreAmbiguous)
		}, wantCode: "worktree_preflight_failed"},
		{name: "provider cannot resume", mutate: func(_ *domain.SessionRecord, _ *domain.SessionWorktreeRecord, _ *fakeRuntime, _ *exactFakeWorkspace, agent *managedSpyAgent) {
			agent.restoreOK = false
		}, wantCode: "provider_session_incompatible"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, st, rt, ws, agent, msg := newManagedPolicyManager()
			rec := managedSession("mer-1", true)
			row := managedRow(rec, domain.SessionWorktreeStatePreserved)
			tt.mutate(&rec, &row, rt, ws, agent)
			st.sessions[rec.ID] = rec
			st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{row}

			_, err := m.Restore(ctx, rec.ID)
			var failure *RecoveryFailure
			if !errors.Is(err, ErrManagedRestore) || !errors.As(err, &failure) || failure.Code != tt.wantCode {
				t.Fatalf("Restore error = %v / %#v, want managed code %q", err, failure, tt.wantCode)
			}
			launches, _, hooks := agent.counts()
			if rt.created != 0 || rt.destroyed != 0 || launches != 0 || hooks != 0 || len(msg.msgs) != 0 || len(ws.restores) != 0 || ws.stashCalls != 0 || ws.destroyed != 0 {
				t.Fatalf("preflight failure mutated state: runtime=%d/%d launch=%d hooks=%d messages=%v restores=%d stash=%d destroy=%d", rt.created, rt.destroyed, launches, hooks, msg.msgs, len(ws.restores), ws.stashCalls, ws.destroyed)
			}
			if !st.sessions[rec.ID].IsTerminated || len(st.worktrees[rec.ID]) != 1 {
				t.Fatal("preflight failure consumed preserved recovery state")
			}
		})
	}
}

func TestManagedExplicitRestorePartialApplyCannotReplay(t *testing.T) {
	m, st, rt, ws, _, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", true)
	row := managedRow(rec, domain.SessionWorktreeStatePreservedRemoved)
	row.PreservedRef = "refs/ao/preserved/mer-1"
	st.sessions[rec.ID] = rec
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{row}
	ws.applyErr = ports.ErrPreservedConflict

	if _, err := m.Restore(ctx, rec.ID); err == nil {
		t.Fatal("Restore succeeded despite preserved apply conflict")
	}
	got := st.worktrees[rec.ID]
	if len(got) != 1 || got[0].State != domain.SessionWorktreeStatePreservedPartial || got[0].PreservedRef != row.PreservedRef || rt.created != 0 {
		t.Fatalf("partial recovery journal/runtime = %#v / %d, want preserved_partial+ref and no runtime", got, rt.created)
	}
	applyCalls := len(ws.calls)
	_, err := m.Restore(ctx, rec.ID)
	var failure *RecoveryFailure
	if !errors.As(err, &failure) || failure.Code != "recovery_journal_partial" {
		t.Fatalf("retry error = %v / %#v, want recovery_journal_partial", err, failure)
	}
	if len(ws.calls) != applyCalls || len(ws.restores) != 1 || rt.created != 0 {
		t.Fatal("partial recovery retry replayed or launched")
	}
}

func TestManagedExplicitRestoreInitialJournalFailureReportsRecoveryMutationTruthfully(t *testing.T) {
	m, st, rt, ws, agent, _ := newManagedPolicyManager()
	rec := managedSession("mer-stale", false)
	row := managedRow(rec, domain.SessionWorktreeStatePreserved)
	row.PreservedRef = "refs/ao/preserved/mer-stale"
	st.sessions[rec.ID] = rec
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{row}
	st.upsertWTErr = errors.New("storage unavailable")

	_, err := m.Restore(ctx, rec.ID)
	var failure *RecoveryFailure
	if !errors.As(err, &failure) || failure.Code != "recovery_journal_failed" || failure.Phase != "recovery" || failure.RuntimeStopped == nil || !*failure.RuntimeStopped {
		t.Fatalf("Restore error = %v / %#v, want recovery phase with stopped runtime", err, failure)
	}
	got := st.worktrees[rec.ID]
	if !st.sessions[rec.ID].IsTerminated || len(got) != 1 || got[0].State != domain.SessionWorktreeStatePreserved || got[0].PreservedRef != row.PreservedRef {
		t.Fatalf("failed journal transition changed durable evidence: session=%#v rows=%#v", st.sessions[rec.ID], got)
	}
	launches, restores, hooks := agent.counts()
	if rt.created != 0 || rt.destroyed != 0 || len(ws.restores) != 0 || len(ws.calls) != 0 || launches != 0 || restores != 1 || hooks != 0 {
		t.Fatalf("initial journal failure crossed mutation boundary: runtime=%d/%d restoreExact=%d workspace=%v provider=%d/%d/%d", rt.created, rt.destroyed, len(ws.restores), ws.calls, launches, restores, hooks)
	}
}

func TestManagedExplicitRestoreJournalCommitFailureStopsRuntimeAndKeepsPartialEvidence(t *testing.T) {
	tests := []struct {
		name      string
		workspace bool
	}{
		{name: "single repo marker delete", workspace: false},
		{name: "workspace atomic active transition", workspace: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, st, rt, ws, _, _ := newManagedPolicyManager()
			rec := managedSession("mer-1", true)
			st.sessions[rec.ID] = rec
			root := managedRow(rec, domain.SessionWorktreeStatePreserved)
			root.PreservedRef = "refs/ao/preserved/mer-1"
			st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{root}
			faultStore := &recoveryJournalFaultStore{fakeStore: st}
			m.store = faultStore
			if tt.workspace {
				project := st.projects["mer"]
				project.Kind = domain.ProjectKindWorkspace
				st.projects["mer"] = project
				st.workspaceRepo["mer"] = []domain.WorkspaceRepoRecord{{ProjectID: "mer", Name: "api", RelativePath: "api"}}
				child := root
				child.RepoName = "api"
				child.WorktreePath += "/api"
				st.worktrees[rec.ID] = append(st.worktrees[rec.ID], child)
				// Allow the pre-mutation partial journal publish, then fail only
				// the post-spawn atomic active transition.
				ws.restoreRelease = nil
			}

			if tt.workspace {
				// fakeStore has one coarse injection point; fail the third batch
				// operation (partial publish is first, active transition is second)
				// by wrapping it with the test-only hook below.
				faultStore.failBatchAfter = 1
			} else {
				faultStore.deleteErr = errors.New("database unavailable")
			}
			_, err := m.Restore(ctx, rec.ID)
			var failure *RecoveryFailure
			if !errors.As(err, &failure) || failure.Code != "recovery_journal_failed" {
				t.Fatalf("Restore error = %v / %#v, want recovery_journal_failed", err, failure)
			}
			if rt.created != 1 || rt.destroyed != 1 || !st.sessions[rec.ID].IsTerminated {
				t.Fatalf("runtime/session = created %d destroyed %d terminal %v, want 1/1/true", rt.created, rt.destroyed, st.sessions[rec.ID].IsTerminated)
			}
			rows := st.worktrees[rec.ID]
			if len(rows) == 0 {
				t.Fatal("journal failure erased managed evidence")
			}
			for _, row := range rows {
				if row.State != domain.SessionWorktreeStatePreservedPartial || row.PreservedRef == "" {
					t.Fatalf("journal row = %#v, want preserved_partial with ref retained", row)
				}
			}
		})
	}
}

func TestManagedExplicitRestoreRollbackUsesDetachedContext(t *testing.T) {
	m, st, rt, _, _, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", true)
	st.sessions[rec.ID] = rec
	row := managedRow(rec, domain.SessionWorktreeStatePreserved)
	row.PreservedRef = "refs/ao/preserved/mer-1"
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{row}
	faultStore := &recoveryJournalFaultStore{fakeStore: st, deleteErr: errors.New("database unavailable")}
	m.store = faultStore
	reqCtx, cancel := context.WithCancel(context.Background())
	m.runtime = &contextAwareRuntime{fakeRuntime: rt, cancelOnCreate: cancel}
	_, err := m.Restore(reqCtx, rec.ID)
	var failure *RecoveryFailure
	if !errors.As(err, &failure) || failure.Code != "recovery_journal_failed" || failure.Phase != "recovery" || failure.RuntimeStopped == nil || !*failure.RuntimeStopped {
		t.Fatalf("Restore error = %v / %#v", err, failure)
	}
	if rt.destroyed != 1 || !st.sessions[rec.ID].IsTerminated {
		t.Fatalf("detached rollback = destroyed %d terminal %v", rt.destroyed, st.sessions[rec.ID].IsTerminated)
	}
}

func TestManagedExplicitRestoreDestroyFailureLeavesRuntimeUnknownAndDoesNotTerminalize(t *testing.T) {
	m, st, rt, _, _, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", true)
	st.sessions[rec.ID] = rec
	row := managedRow(rec, domain.SessionWorktreeStatePreserved)
	row.PreservedRef = "refs/ao/preserved/mer-1"
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{row}
	faultStore := &recoveryJournalFaultStore{fakeStore: st, deleteErr: errors.New("database unavailable")}
	m.store = faultStore
	rt.destroyErr = errors.New("runtime stop failed")
	_, err := m.Restore(context.Background(), rec.ID)
	var failure *RecoveryFailure
	if !errors.As(err, &failure) || failure.Code != "rollback_failed" || failure.Phase != "rollback" || failure.RuntimeStopped == nil || *failure.RuntimeStopped {
		t.Fatalf("Restore error = %v / %#v", err, failure)
	}
	if rt.destroyed != 1 || st.sessions[rec.ID].IsTerminated || st.worktrees[rec.ID][0].State != domain.SessionWorktreeStatePreservedPartial {
		t.Fatalf("failed rollback = destroyed %d session %#v rows %#v", rt.destroyed, st.sessions[rec.ID], st.worktrees[rec.ID])
	}
}

func TestManagedCleanupSkipsExactRecoveryWithoutRuntimeOrWorkspaceMutation(t *testing.T) {
	for _, state := range []string{domain.SessionWorktreeStatePreserved, domain.SessionWorktreeStatePreservedRemoved, domain.SessionWorktreeStatePreservedPartial} {
		t.Run(state, func(t *testing.T) {
			m, st, rt, ws, _, _ := newManagedPolicyManager()
			rec := managedSession("mer-1", true)
			st.sessions[rec.ID] = rec
			st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{managedRow(rec, state)}

			got, err := m.Cleanup(ctx, rec.ProjectID)
			if err != nil {
				t.Fatalf("Cleanup: %v", err)
			}
			if len(got.Cleaned) != 0 || len(got.Skipped) != 1 || got.Skipped[0].Reason != "session is awaiting explicit recovery" {
				t.Fatalf("cleanup result = %#v", got)
			}
			if rt.destroyed != 0 || ws.destroyed != 0 || ws.stashCalls != 0 || len(st.worktrees[rec.ID]) != 1 {
				t.Fatalf("cleanup mutated managed state: runtime=%d workspace=%d stash=%d rows=%#v", rt.destroyed, ws.destroyed, ws.stashCalls, st.worktrees[rec.ID])
			}
		})
	}
}

func TestManagedCleanupReReadsSessionAfterRestoreLifecycleLock(t *testing.T) {
	m, st, _, ws, _, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", true)
	st.sessions[rec.ID] = rec
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{managedRow(rec, domain.SessionWorktreeStatePreserved)}
	ws.restoreStarted = make(chan struct{})
	ws.restoreRelease = make(chan struct{})
	listStore := &cleanupListSignalStore{fakeStore: st, listed: make(chan struct{}), release: make(chan struct{})}
	m.store = listStore

	cleanupDone := make(chan CleanupResult, 1)
	cleanupErr := make(chan error, 1)
	go func() {
		got, err := m.Cleanup(context.Background(), rec.ProjectID)
		cleanupDone <- got
		cleanupErr <- err
	}()
	// Cleanup has copied the terminal record but cannot yet acquire its
	// lifecycle lock. This is the stale-enumeration interleaving under test.
	<-listStore.listed

	restoreErr := make(chan error, 1)
	go func() {
		_, err := m.Restore(context.Background(), rec.ID)
		restoreErr <- err
	}()
	<-ws.restoreStarted
	close(listStore.release)
	close(ws.restoreRelease)
	if err := <-restoreErr; err != nil {
		t.Fatalf("Restore: %v", err)
	}
	got := <-cleanupDone
	if err := <-cleanupErr; err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if len(got.Cleaned) != 0 || len(got.Skipped) != 0 || ws.destroyed != 0 {
		t.Fatalf("stale cleanup reclaimed restored session: result=%#v destroyed=%d", got, ws.destroyed)
	}
}

func TestManagedWorkspaceProjectRestoreRequiresCompleteExactJournal(t *testing.T) {
	m, st, rt, ws, _, _ := newManagedPolicyManager()
	project := st.projects["mer"]
	project.Kind = domain.ProjectKindWorkspace
	st.projects["mer"] = project
	st.workspaceRepo["mer"] = []domain.WorkspaceRepoRecord{{ProjectID: "mer", Name: "api", RelativePath: "api"}}
	rec := managedSession("mer-1", true)
	st.sessions[rec.ID] = rec
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{managedRow(rec, domain.SessionWorktreeStatePreserved)}

	_, err := m.Restore(ctx, rec.ID)
	var failure *RecoveryFailure
	if !errors.As(err, &failure) || failure.Code != "worktree_identity_mismatch" {
		t.Fatalf("Restore error = %v / %#v, want incomplete workspace journal", err, failure)
	}
	if rt.created != 0 || len(ws.preflights) != 0 || len(ws.restores) != 0 || len(ws.calls) != 0 {
		t.Fatalf("incomplete workspace journal mutated state: runtime=%d preflight=%d restore=%d calls=%v", rt.created, len(ws.preflights), len(ws.restores), ws.calls)
	}
}

func TestManagedExplicitRestoreConcurrentCallsCreateOneRuntime(t *testing.T) {
	m, st, rt, _, _, _ := newManagedPolicyManager()
	rec := managedSession("mer-1", true)
	st.sessions[rec.ID] = rec
	st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{managedRow(rec, domain.SessionWorktreeStatePreserved)}

	start := make(chan struct{})
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			_, err := m.Restore(context.Background(), rec.ID)
			errs <- err
		}()
	}
	close(start)
	var successes, notRestorable int
	for i := 0; i < 2; i++ {
		err := <-errs
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrNotRestorable):
			notRestorable++
		default:
			t.Fatalf("concurrent restore error = %v", err)
		}
	}
	if successes != 1 || notRestorable != 1 || rt.created != 1 {
		t.Fatalf("successes=%d notRestorable=%d runtime creates=%d, want 1/1/1", successes, notRestorable, rt.created)
	}
}

func containsCall(calls []string, want string) bool {
	for _, call := range calls {
		if call == want {
			return true
		}
	}
	return false
}
