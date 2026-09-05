package sessionmanager

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/admission"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
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
