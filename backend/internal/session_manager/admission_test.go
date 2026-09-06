package sessionmanager

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/admission"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/sessionguard"
)

func TestAdmissionPausePreservesEveryLaunchEntry(t *testing.T) {
	for _, op := range []string{"worker", "orchestrator", "restore", "restore-all", "reconcile"} {
		t.Run(op, func(t *testing.T) {
			m, st, rt, ws := newManager()
			project := st.projects["mer"]
			project.Config.AdmissionPaused = true
			st.projects["mer"] = project
			rec := domain.SessionRecord{ID: "mer-1", ProjectID: "mer", Kind: domain.KindWorker, IsTerminated: op != "reconcile", Metadata: domain.SessionMetadata{WorkspacePath: "/ws/mer-1", Branch: "b", RuntimeHandleID: "mer-1", AgentSessionID: "native", Prompt: "saved"}}
			st.sessions[rec.ID] = rec
			marker := domain.SessionWorktreeRecord{SessionID: rec.ID, RepoName: domain.RootWorkspaceRepoName, WorktreePath: rec.Metadata.WorkspacePath, Branch: "b", PreservedRef: "refs/ao/preserved/mer-1", State: "removed"}
			st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{marker}
			var err error
			switch op {
			case "worker", "orchestrator":
				_, err = m.Spawn(ctx, ports.SpawnConfig{ProjectID: "mer", Kind: domain.SessionKind(op)})
			case "restore":
				_, err = m.Restore(ctx, rec.ID)
			case "restore-all":
				err = m.RestoreAll(ctx)
			case "reconcile":
				err = m.Reconcile(ctx)
			}
			if op == "worker" || op == "orchestrator" || op == "restore" {
				if !errors.Is(err, admission.ErrPaused) {
					t.Fatalf("error %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if rt.created != 0 || rt.destroyed != 0 || ws.stashCalls != 0 || ws.destroyed != 0 || len(ws.calls) != 0 {
				t.Fatalf("pause mutated runtime/workspace: %+v %+v", rt, ws)
			}
			if !reflect.DeepEqual(st.sessions[rec.ID], rec) || !reflect.DeepEqual(st.worktrees[rec.ID], []domain.SessionWorktreeRecord{marker}) {
				t.Fatal("pause altered preserved evidence")
			}
		})
	}
}

type heldRuntime struct {
	*fakeRuntime
	entered chan struct{}
	release chan struct{}
}

type fakeWorkerAdmitter struct {
	calls  []ports.WorkerAdmissionRequest
	result ports.WorkerAdmissionResult
	err    error
}

func (a *fakeWorkerAdmitter) AdmitWorker(_ context.Context, req ports.WorkerAdmissionRequest) (ports.WorkerAdmissionResult, error) {
	a.calls = append(a.calls, req)
	return a.result, a.err
}

func managedWorkerProject(st *fakeStore) domain.AgentRoute {
	project := st.projects["mer"]
	project.RepoOriginURL = "https://github.com/owner/repo.git"
	project.Config.DesktopProjectsAdmission = true
	st.projects["mer"] = project
	return domain.AgentRoute{Harness: domain.HarnessClaudeCode, Model: "claude-test", ReasoningEffort: domain.ReasoningEffortMedium}
}

func TestScopedWorkerAdmissionGuardsSpawnBeforeWorkspaceEffects(t *testing.T) {
	for _, tc := range []struct {
		name        string
		admitErr    error
		wantSuccess bool
	}{
		{name: "accepted", wantSuccess: true},
		{name: "refused", admitErr: errors.New("scope conflict")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, st, rt, ws := newManager()
			route := managedWorkerProject(st)
			admitter := &fakeWorkerAdmitter{result: ports.WorkerAdmissionResult{IssueBody: "authoritative body"}, err: tc.admitErr}
			m.admitter = admitter
			got, err := m.Spawn(ctx, ports.SpawnConfig{ProjectID: "mer", IssueID: "github:owner/repo#18", Kind: domain.KindWorker, Route: &route, Prompt: "stale body"})
			if tc.wantSuccess {
				if err != nil {
					t.Fatal(err)
				}
				if got.Metadata.Prompt != "authoritative body" || rt.created != 1 || ws.lastCfg.SessionID != got.ID {
					t.Fatalf("spawn = %#v runtime=%d workspace=%#v", got, rt.created, ws.lastCfg)
				}
			} else {
				if !errors.Is(err, ErrWorkerAdmission) {
					t.Fatalf("error = %v", err)
				}
				if rt.created != 0 || ws.lastCfg.SessionID != "" {
					t.Fatalf("refused admission reached worker effects: runtime=%d workspace=%#v", rt.created, ws.lastCfg)
				}
				seed := st.sessions["mer-1"]
				if !seed.IsTerminated {
					t.Fatalf("uncertain reservation seed was not retained terminal: %#v", seed)
				}
			}
			if len(admitter.calls) != 1 {
				t.Fatalf("admission calls = %d", len(admitter.calls))
			}
			req := admitter.calls[0]
			if req.ProjectID != "mer" || req.Repository != "https://github.com/owner/repo.git" || req.SessionID != "mer-1" || req.IssueID != "github:owner/repo#18" || req.Operation != ports.WorkerAdmissionSpawn || req.Route != route {
				t.Fatalf("admission request = %#v", req)
			}
		})
	}
}

func TestScopedWorkerAdmissionOptOutAndControllerCompatibility(t *testing.T) {
	for _, kind := range []domain.SessionKind{domain.KindWorker, domain.KindOrchestrator} {
		m, st, rt, _ := newManager()
		if kind == domain.KindOrchestrator {
			managedWorkerProject(st)
		}
		admitter := &fakeWorkerAdmitter{err: errors.New("must not be called")}
		m.admitter = admitter
		if _, err := m.Spawn(ctx, ports.SpawnConfig{ProjectID: "mer", Kind: kind}); err != nil {
			t.Fatalf("kind %s: %v", kind, err)
		}
		if len(admitter.calls) != 0 || rt.created != 1 {
			t.Fatalf("kind %s calls=%d runtime=%d", kind, len(admitter.calls), rt.created)
		}
	}
}

func TestScopedWorkerAdmissionGuardsRestorePathsBeforeWorkspaceEffects(t *testing.T) {
	for _, op := range []string{"restore", "restore-all"} {
		t.Run(op, func(t *testing.T) {
			m, st, rt, ws := newManager()
			route := managedWorkerProject(st)
			admitter := &fakeWorkerAdmitter{err: errors.New("existing owner mismatch")}
			m.admitter = admitter
			rec := domain.SessionRecord{
				ID: "mer-1", ProjectID: "mer", IssueID: "github:owner/repo#18", Kind: domain.KindWorker,
				Harness: domain.HarnessClaudeCode, IsTerminated: true,
				Metadata: domain.SessionMetadata{WorkspacePath: "/ws/mer-1", Branch: "b", Prompt: "authoritative body", RequestedRoute: &route, LaunchRoute: &domain.AgentLaunchRoute{Harness: route.Harness, Model: route.Model, ReasoningEffort: route.ReasoningEffort}},
			}
			st.sessions[rec.ID] = rec
			if op == "restore-all" {
				st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{{SessionID: rec.ID, RepoName: domain.RootWorkspaceRepoName, WorktreePath: rec.Metadata.WorkspacePath, Branch: rec.Metadata.Branch, State: "removed"}}
				if err := m.RestoreAll(ctx); err != nil {
					t.Fatal(err)
				}
			} else if _, err := m.Restore(ctx, rec.ID); !errors.Is(err, ErrWorkerAdmission) {
				t.Fatalf("error = %v", err)
			}
			if len(admitter.calls) != 1 || admitter.calls[0].Operation != ports.WorkerAdmissionRestore {
				t.Fatalf("admission calls = %#v", admitter.calls)
			}
			if rt.created != 0 || ws.lastCfg.SessionID != "" || !st.sessions[rec.ID].IsTerminated {
				t.Fatalf("refused restore mutated worker: runtime=%d workspace=%#v session=%#v", rt.created, ws.lastCfg, st.sessions[rec.ID])
			}
		})
	}
}

func TestHeldManagedWorkerBlocksRestoreBeforeScopedAdmission(t *testing.T) {
	m, st, rt, ws := newManager()
	route := managedWorkerProject(st)
	admitter := &fakeWorkerAdmitter{}
	m.admitter = admitter
	rec := domain.SessionRecord{ID: "mer-1", ProjectID: "mer", IssueID: "github:owner/repo#18", Kind: domain.KindWorker, Harness: domain.HarnessClaudeCode, IsTerminated: true, Metadata: domain.SessionMetadata{WorkspacePath: "/ws/mer-1", Branch: "b", Prompt: "body", RequestedRoute: &route}}
	st.sessions[rec.ID] = rec
	st.holds[rec.ID] = domain.WorkerSchedulingHold{SessionID: rec.ID, HeldAt: time.Now()}
	if _, err := m.Restore(ctx, rec.ID); !errors.Is(err, ErrWorkerHeld) {
		t.Fatalf("error = %v", err)
	}
	if len(admitter.calls) != 0 || rt.created != 0 || ws.lastCfg.SessionID != "" {
		t.Fatal("held restore reached admission or worker effects")
	}
}

func (r *heldRuntime) Create(ctx context.Context, cfg ports.RuntimeConfig) (ports.RuntimeHandle, error) {
	close(r.entered)
	select {
	case <-ctx.Done():
		return ports.RuntimeHandle{}, ctx.Err()
	case <-r.release:
	}
	return r.fakeRuntime.Create(ctx, cfg)
}
func TestAdmissionPauseWaitsForAdmittedSpawn(t *testing.T) {
	m, st, rt, _ := newManager()
	held := &heldRuntime{fakeRuntime: rt, entered: make(chan struct{}), release: make(chan struct{})}
	m.runtime = held
	done := make(chan error, 1)
	go func() {
		_, err := m.Spawn(ctx, ports.SpawnConfig{ProjectID: "mer", Kind: domain.KindWorker})
		done <- err
	}()
	<-held.entered
	pauseDone := make(chan error, 1)
	go func() {
		unlock, err := m.admission.Lock(ctx, "mer")
		if err != nil {
			pauseDone <- err
			return
		}
		defer unlock()
		p := st.projects["mer"]
		p.Config.AdmissionPaused = true
		st.projects["mer"] = p
		pauseDone <- nil
	}()
	select {
	case <-pauseDone:
		t.Fatal("pause completed during admitted spawn")
	case <-time.After(20 * time.Millisecond):
	}
	close(held.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-pauseDone; err != nil {
		t.Fatal(err)
	}
	if _, err := m.Spawn(ctx, ports.SpawnConfig{ProjectID: "mer", Kind: domain.KindWorker}); !errors.Is(err, admission.ErrPaused) {
		t.Fatalf("late launch: %v", err)
	}
	if rt.created != 1 {
		t.Fatalf("created=%d", rt.created)
	}
}
func TestUnknownAdmissionDoesNotRestore(t *testing.T) {
	m, st, rt, ws := newManager()
	p := st.projects["mer"]
	p.ConfigDecodeError = "bad persisted JSON"
	st.projects["mer"] = p
	seedTerminal(st, "mer-1", domain.SessionMetadata{WorkspacePath: "/ws/mer-1", Branch: "b"})
	if _, err := m.Restore(ctx, "mer-1"); !errors.Is(err, admission.ErrUncertain) {
		t.Fatal(err)
	}
	if rt.created != 0 || len(ws.calls) != 0 {
		t.Fatal("mutated on unknown admission")
	}
}

func TestPausedDisabledRecoveryStopsRuntimeAndPreservesCandidate(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		for _, unknown := range []bool{false, true} {
			m, st, rt, ws := newManager()
			p := st.projects["mer"]
			p.Config.AdmissionPaused = !unknown
			if unknown {
				p.ConfigDecodeError = "bad persisted config"
			}
			st.projects["mer"] = p
			m.policy = domain.NewAgentPolicy([]domain.AgentHarness{domain.HarnessClaudeCode})
			rec := domain.SessionRecord{ID: "mer-1", ProjectID: "mer", Harness: domain.HarnessClaudeCode, IsTerminated: terminal, Metadata: domain.SessionMetadata{WorkspacePath: "/ws/mer-1", Branch: "b", RuntimeHandleID: "mer-1", AgentSessionID: "native-context"}}
			st.sessions[rec.ID] = rec
			rt.aliveByHandle = map[string]bool{"mer-1": true}
			marker := domain.SessionWorktreeRecord{SessionID: rec.ID, RepoName: domain.RootWorkspaceRepoName, WorktreePath: "/ws/mer-1", Branch: "b", PreservedRef: "refs/ao/preserved/existing", State: "removed"}
			st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{marker}
			if err := m.Reconcile(ctx); err != nil {
				t.Fatalf("terminal=%v unknown=%v: %v", terminal, unknown, err)
			}
			if rt.aliveByHandle["mer-1"] || !st.sessions[rec.ID].IsTerminated {
				t.Fatal("disabled runtime remained live")
			}
			if ws.stashCalls != 0 || len(ws.calls) != 0 || !reflect.DeepEqual(st.sessions[rec.ID].Metadata, rec.Metadata) || !reflect.DeepEqual(st.worktrees[rec.ID], []domain.SessionWorktreeRecord{marker}) {
				t.Fatal("disabled retirement changed preserved candidate")
			}
		}
	}
}
func TestPausedDisabledUnknownStopDoesNotPublishTermination(t *testing.T) {
	m, st, rt, ws := newManager()
	p := st.projects["mer"]
	p.Config.AdmissionPaused = true
	st.projects["mer"] = p
	m.policy = domain.NewAgentPolicy([]domain.AgentHarness{domain.HarnessClaudeCode})
	rec := domain.SessionRecord{ID: "mer-1", ProjectID: "mer", Harness: domain.HarnessClaudeCode, Metadata: domain.SessionMetadata{WorkspacePath: "/ws/mer-1", Branch: "b", RuntimeHandleID: "mer-1"}}
	st.sessions[rec.ID] = rec
	rt.aliveErr = errors.New("probe unavailable")
	if err := m.Reconcile(ctx); !errors.Is(err, ErrDisabledAgentRetirement) {
		t.Fatalf("unknown stop: %v", err)
	}
	if st.sessions[rec.ID].IsTerminated || ws.stashCalls != 0 || len(ws.calls) != 0 {
		t.Fatal("uncertain stop changed evidence")
	}
}

type blockedAdmissionMessenger struct {
	entered, release chan struct{}
	once             sync.Once
	calls            int
}

func (m *blockedAdmissionMessenger) Send(ctx context.Context, _ domain.SessionID, _ string) error {
	m.calls++
	m.once.Do(func() { close(m.entered) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.release:
		return nil
	}
}
func TestRequiredAdmissionSendSerializesWithPause(t *testing.T) {
	m, st, _, _ := newManager()
	st.sessions["mer-1"] = mkLive("mer-1")
	raw := &blockedAdmissionMessenger{entered: make(chan struct{}), release: make(chan struct{})}
	m.messenger = sessionguard.New(st, raw, nil)
	sent := make(chan error, 1)
	go func() { sent <- m.SendAdmitted(ctx, "mer-1", "background turn") }()
	<-raw.entered
	paused := make(chan error, 1)
	go func() {
		unlock, err := m.admission.Lock(ctx, "mer")
		if err != nil {
			paused <- err
			return
		}
		defer unlock()
		p := st.projects["mer"]
		p.Config.AdmissionPaused = true
		st.projects["mer"] = p
		paused <- nil
	}()
	select {
	case <-paused:
		t.Fatal("pause completed before admitted send")
	case <-time.After(20 * time.Millisecond):
	}
	close(raw.release)
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	if err := <-paused; err != nil {
		t.Fatal(err)
	}
	if err := m.SendAdmitted(ctx, "mer-1", "late background turn"); !errors.Is(err, admission.ErrPaused) {
		t.Fatal(err)
	}
	if raw.calls != 1 {
		t.Fatal("late background message reached pane")
	}
	if err := m.Send(ctx, "mer-1", "explicit user message"); err != nil {
		t.Fatal(err)
	}
	if raw.calls != 2 {
		t.Fatal("ordinary user send unexpectedly gated")
	}
}

func TestWorkerHoldWaitsForAdmittedPaneWrite(t *testing.T) {
	m, st, _, _ := newManager()
	worker := mkLive("mer-1")
	worker.Kind = domain.KindWorker
	st.sessions[worker.ID] = worker
	raw := &blockedAdmissionMessenger{entered: make(chan struct{}), release: make(chan struct{})}
	m.messenger = sessionguard.New(st, raw, nil)

	sent := make(chan error, 1)
	go func() { sent <- m.Send(ctx, worker.ID, "already admitted") }()
	<-raw.entered
	held := make(chan error, 1)
	go func() {
		_, err := m.HoldWorker(ctx, worker.ID)
		held <- err
	}()
	select {
	case err := <-held:
		t.Fatalf("hold returned before pane write finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(raw.release)
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	if err := m.Send(ctx, worker.ID, "after hold"); !errors.Is(err, ErrWorkerHeld) {
		t.Fatalf("post-hold send error = %v, want ErrWorkerHeld", err)
	}
	if raw.calls != 1 {
		t.Fatalf("pane writes = %d, want 1", raw.calls)
	}
}
