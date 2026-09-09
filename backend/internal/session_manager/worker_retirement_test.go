package sessionmanager

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func retirementFixture() (*Manager, *fakeStore, *fakeRuntime, *fakeWorkspace, domain.SessionRecord, WorkerRetirementExpectation) {
	m, st, rt, ws := newManager()
	at := time.Date(2026, 9, 9, 1, 2, 3, 123456789, time.UTC)
	m.clock = func() time.Time { return at.Add(time.Hour) }
	rec := domain.SessionRecord{ID: "mer-1", ProjectID: "mer", IssueID: "326", Kind: domain.KindWorker, Harness: domain.HarnessCodex, IsTerminated: true, CreatedAt: at.Add(-time.Hour), UpdatedAt: at, Activity: domain.Activity{State: domain.ActivityExited, LastActivityAt: at}, Metadata: domain.SessionMetadata{WorkspacePath: "/preserved/mer-1", Branch: "ao/mer-1/root", Prompt: "historical task", RuntimeHandleID: "mer-1", AgentSessionID: "old-provider-session"}}
	st.sessions[rec.ID] = rec
	st.projects["mer"] = domain.ProjectRecord{ID: "mer", Path: "/canonical", RepoOriginURL: "https://github.com/owner/repo.git", Config: domain.ProjectConfig{AdmissionPaused: true}}
	rt.verifiedStop = true
	expected := WorkerRetirementExpectation{ProjectID: "mer", Ticket: "github:owner/repo#326", UpdatedAt: at}
	return m, st, rt, ws, rec, expected
}

func TestWorkerRetirementPreservesAllSessionAndWorkspaceFacts(t *testing.T) {
	for _, missingHandle := range []bool{false, true} {
		t.Run(map[bool]string{false: "persisted handle", true: "derived missing handle"}[missingHandle], func(t *testing.T) {
			m, st, rt, ws, rec, expected := retirementFixture()
			if missingHandle {
				rec.Metadata.RuntimeHandleID = ""
				st.sessions[rec.ID] = rec
			}
			st.worktrees[rec.ID] = []domain.SessionWorktreeRecord{{SessionID: rec.ID, Branch: rec.Metadata.Branch, WorktreePath: rec.Metadata.WorkspacePath, PreservedRef: "refs/preserved/work"}}
			markers := append([]domain.SessionWorktreeRecord(nil), st.worktrees[rec.ID]...)
			rt.aliveByHandle = map[string]bool{"mer-1": true}
			out, err := m.RetireWorkerForRetry(ctx, rec.ID, expected)
			if err != nil || out.RuntimeTermination != RuntimeTerminationStopped || out.Hold.Retirement == nil {
				t.Fatalf("retire = %+v, %v", out, err)
			}
			if !reflect.DeepEqual(st.sessions[rec.ID], rec) || !reflect.DeepEqual(st.worktrees[rec.ID], markers) || len(ws.calls) != 0 || ws.stashCalls != 0 || ws.destroyed != 0 || rt.created != 0 {
				t.Fatal("retirement changed preserved state")
			}
			first := *out.Hold.Retirement
			m.clock = func() time.Time { return first.RetiredAt.Add(time.Hour) }
			again, err := m.RetireWorkerForRetry(ctx, rec.ID, expected)
			if err != nil || !reflect.DeepEqual(*again.Hold.Retirement, first) || rt.destroyed != 1 {
				t.Fatalf("replay changed fact/runtime: %+v, %v", again, err)
			}
			verified, err := m.VerifyWorkerRetirement(ctx, rec.ID, expected)
			if err != nil || !verified.Verification.Verified || verified.Verification.RuntimeTermination != "stopped" || verified.Verification.Scope != WorkerRetirementScope || rt.destroyed != 1 {
				t.Fatalf("read-only verification = %+v, %v", verified, err)
			}
			if _, err := m.Restore(ctx, rec.ID); err == nil {
				t.Fatal("retirement allowed restore")
			}
		})
	}
}

func TestWorkerRetirementRefusesContradictionsBeforeEffects(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*fakeStore, *domain.SessionRecord, *WorkerRetirementExpectation)
	}{
		{"wrong project", func(_ *fakeStore, _ *domain.SessionRecord, e *WorkerRetirementExpectation) { e.ProjectID = "other" }},
		{"wrong ticket", func(_ *fakeStore, _ *domain.SessionRecord, e *WorkerRetirementExpectation) {
			e.Ticket = "github:owner/repo#327"
		}},
		{"noncanonical ticket", func(_ *fakeStore, _ *domain.SessionRecord, e *WorkerRetirementExpectation) {
			e.Ticket = "github:Owner/repo#326"
		}},
		{"wrong version", func(_ *fakeStore, _ *domain.SessionRecord, e *WorkerRetirementExpectation) {
			e.UpdatedAt = e.UpdatedAt.Add(time.Nanosecond)
		}},
		{"live worker", func(_ *fakeStore, r *domain.SessionRecord, _ *WorkerRetirementExpectation) { r.IsTerminated = false }},
		{"contradictory activity", func(_ *fakeStore, r *domain.SessionRecord, _ *WorkerRetirementExpectation) {
			r.Activity.State = domain.ActivityActive
		}},
		{"controller", func(_ *fakeStore, r *domain.SessionRecord, _ *WorkerRetirementExpectation) {
			r.Kind = domain.KindOrchestrator
		}},
		{"different repository", func(s *fakeStore, _ *domain.SessionRecord, _ *WorkerRetirementExpectation) {
			p := s.projects["mer"]
			p.RepoOriginURL = "https://github.com/other/repo.git"
			s.projects["mer"] = p
		}},
		{"open PR", func(s *fakeStore, r *domain.SessionRecord, _ *WorkerRetirementExpectation) {
			s.pr[r.ID] = domain.PRFacts{URL: "pr"}
		}},
		{"merged PR", func(s *fakeStore, r *domain.SessionRecord, _ *WorkerRetirementExpectation) {
			s.pr[r.ID] = domain.PRFacts{URL: "pr", Merged: true, Closed: true}
		}},
		{"unresolved comments", func(s *fakeStore, r *domain.SessionRecord, _ *WorkerRetirementExpectation) {
			s.pr[r.ID] = domain.PRFacts{URL: "pr", Closed: true, ReviewComments: true}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, s, rt, ws, r, e := retirementFixture()
			tc.change(s, &r, &e)
			s.sessions[r.ID] = r
			if _, err := m.RetireWorkerForRetry(ctx, r.ID, e); err == nil {
				t.Fatal("accepted contradictory target")
			}
			if len(s.holds) != 0 || rt.destroyed != 0 || len(ws.calls) != 0 || !reflect.DeepEqual(s.sessions[r.ID], r) {
				t.Fatal("refusal changed target")
			}
		})
	}
}

func TestWorkerRetirementUnknownKeepsHoldWithoutProof(t *testing.T) {
	for _, tc := range []struct {
		name       string
		change     func(*fakeStore, *fakeRuntime, *domain.SessionRecord)
		wantErr    bool
		wantStatus string
	}{
		{"unsupported", func(_ *fakeStore, rt *fakeRuntime, _ *domain.SessionRecord) { rt.verifiedStop = false }, false, "unsupported"},
		{"probe error", func(_ *fakeStore, rt *fakeRuntime, _ *domain.SessionRecord) { rt.aliveErr = errors.New("probe") }, false, "unknown"},
		{"destroy error", func(_ *fakeStore, rt *fakeRuntime, _ *domain.SessionRecord) { rt.destroyErr = errors.New("destroy") }, false, "unknown"},
		{"surviving runtime", func(_ *fakeStore, rt *fakeRuntime, _ *domain.SessionRecord) { rt.destroyLeavesAlive = true }, false, "unknown"},
		{"noncanonical handle", func(_ *fakeStore, _ *fakeRuntime, r *domain.SessionRecord) { r.Metadata.RuntimeHandleID = "foreign" }, true, "unknown"},
		{"handle collision", func(s *fakeStore, _ *fakeRuntime, _ *domain.SessionRecord) {
			s.sessions["other-1"] = domain.SessionRecord{ID: "other-1", Metadata: domain.SessionMetadata{RuntimeHandleID: "mer-1"}}
		}, true, "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, s, rt, ws, r, e := retirementFixture()
			rt.aliveByHandle = map[string]bool{"mer-1": true}
			tc.change(s, rt, &r)
			s.sessions[r.ID] = r
			out, err := m.RetireWorkerForRetry(ctx, r.ID, e)
			if (err != nil) != tc.wantErr || out.RuntimeTermination != tc.wantStatus || out.Hold.Retirement != nil {
				t.Fatalf("out=%+v err=%v", out, err)
			}
			if _, held := s.holds[r.ID]; !held {
				t.Fatal("failure lost hold")
			}
			if !reflect.DeepEqual(s.sessions[r.ID], r) || len(ws.calls) != 0 || ws.stashCalls != 0 || ws.destroyed != 0 {
				t.Fatal("uncertainty changed preservation")
			}
		})
	}
}

func TestWorkerRetirementVerificationNeverCreatesAuthority(t *testing.T) {
	m, s, rt, _, r, e := retirementFixture()
	for _, held := range []bool{false, true} {
		if held {
			if _, err := m.HoldWorker(ctx, r.ID); err != nil {
				t.Fatal(err)
			}
		}
		out, err := m.VerifyWorkerRetirement(ctx, r.ID, e)
		if err != nil || out.Verification.Verified || out.Hold.Retirement != nil || rt.destroyed != 0 {
			t.Fatalf("unretired verification=%+v %v", out, err)
		}
	}
	if _, err := m.StopWorkerRetainingWorktree(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	if s.holds[r.ID].Retirement != nil {
		t.Fatal("plain stop manufactured retirement")
	}
	if _, err := m.RetireWorkerForRetry(ctx, r.ID, e); err != nil {
		t.Fatal(err)
	}
	before := rt.destroyed
	rt.aliveByHandle = map[string]bool{"mer-1": true}
	out, err := m.VerifyWorkerRetirement(ctx, r.ID, e)
	if err != nil || out.Verification.Verified || rt.destroyed != before {
		t.Fatal("verification stopped revived runtime")
	}
	if _, err := m.RetireWorkerForRetry(ctx, r.ID, e); err == nil || rt.destroyed != before {
		t.Fatal("replay retired revived runtime")
	}
	rt.aliveByHandle["mer-1"] = false
	changed := r
	changed.UpdatedAt = changed.UpdatedAt.Add(time.Nanosecond)
	s.sessions[r.ID] = changed
	if _, err := m.VerifyWorkerRetirement(ctx, r.ID, e); err == nil {
		t.Fatal("verified stale version")
	}
	e.UpdatedAt = changed.UpdatedAt
	if _, err := m.VerifyWorkerRetirement(ctx, r.ID, e); err == nil {
		t.Fatal("rebound proof to changed version")
	}
}

type retirementProbeRuntime struct {
	*fakeRuntime
	afterProbe func()
	handleErr  error
}

func (r *retirementProbeRuntime) HandleFor(c ports.RuntimeConfig) (ports.RuntimeHandle, error) {
	if r.handleErr != nil {
		return ports.RuntimeHandle{}, r.handleErr
	}
	return r.fakeRuntime.HandleFor(c)
}
func (r *retirementProbeRuntime) IsAlive(ctx context.Context, h ports.RuntimeHandle) (bool, error) {
	alive, err := r.fakeRuntime.IsAlive(ctx, h)
	if r.afterProbe != nil {
		r.afterProbe()
	}
	return alive, err
}

func TestWorkerRetirementRechecksSessionAfterProbe(t *testing.T) {
	m, s, rt, _, r, e := retirementFixture()
	m.runtime = &retirementProbeRuntime{fakeRuntime: rt, afterProbe: func() { changed := s.sessions[r.ID]; changed.Metadata.Branch = "changed"; s.sessions[r.ID] = changed }}
	if _, err := m.RetireWorkerForRetry(ctx, r.ID, e); err == nil {
		t.Fatal("accepted session drift")
	}
	if s.holds[r.ID].Retirement != nil {
		t.Fatal("recorded proof after drift")
	}
}

type retirementFailingStore struct {
	*fakeStore
	writeErr error
}

func (s *retirementFailingStore) SetWorkerRetirement(ctx context.Context, rec domain.SessionRecord, fact domain.WorkerRetirement) (domain.WorkerSchedulingHold, error) {
	if s.writeErr != nil {
		return domain.WorkerSchedulingHold{}, s.writeErr
	}
	return s.fakeStore.SetWorkerRetirement(ctx, rec, fact)
}

func TestWorkerRetirementInterruptedPersistencePreservesHoldAndCanRetry(t *testing.T) {
	m, s, rt, _, r, e := retirementFixture()
	rt.aliveByHandle = map[string]bool{"mer-1": true}
	failed := &retirementFailingStore{fakeStore: s, writeErr: errors.New("storage unavailable")}
	m.store = failed
	if _, err := m.RetireWorkerForRetry(ctx, r.ID, e); err == nil {
		t.Fatal("claimed retirement after failed persistence")
	}
	if s.holds[r.ID].Retirement != nil || s.holds[r.ID].HeldAt.IsZero() || !reflect.DeepEqual(s.sessions[r.ID], r) {
		t.Fatal("failed persistence changed session or lost fence")
	}
	out, err := m.VerifyWorkerRetirement(ctx, r.ID, e)
	if err != nil || out.Verification.Verified {
		t.Fatal("absence alone granted authority")
	}
	failed.writeErr = nil
	if _, err := m.RetireWorkerForRetry(ctx, r.ID, e); err != nil {
		t.Fatal(err)
	}
	if s.holds[r.ID].Retirement == nil || rt.destroyed != 1 {
		t.Fatal("retry repeated stop or failed to persist proof")
	}
}

func TestWorkerRetirementUnknownCanonicalHandleRefuses(t *testing.T) {
	m, s, rt, _, r, e := retirementFixture()
	m.runtime = &retirementProbeRuntime{fakeRuntime: rt, handleErr: errors.New("unknown handle")}
	if _, err := m.RetireWorkerForRetry(ctx, r.ID, e); err == nil {
		t.Fatal("accepted unknown runtime identity")
	}
	if s.holds[r.ID].Retirement != nil || rt.destroyed != 0 {
		t.Fatal("unknown runtime changed authority")
	}
}
