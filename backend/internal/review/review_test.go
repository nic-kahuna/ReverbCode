package review

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// --- fakes ---

type fakeStore struct {
	review               *domain.Review
	runs                 []domain.ReviewRun
	listAllReviewRunHits int
	upsertCalls          int
	upsertErrAt          int
	upsertErr            error
	// insertErr, when set, makes the next InsertReviewRun model a concurrent
	// writer that already recorded a run for this commit: it records that
	// winner (so a follow-up GetReviewRunBySessionAndSHA finds it) and returns
	// insertErr instead of recording the caller's run.
	insertErr error
}

func (f *fakeStore) UpsertReview(_ context.Context, r domain.Review) error {
	f.upsertCalls++
	if f.upsertErrAt == f.upsertCalls {
		return f.upsertErr
	}
	cp := r
	f.review = &cp
	return nil
}
func (f *fakeStore) GetReviewBySession(_ context.Context, _ domain.SessionID) (domain.Review, bool, error) {
	if f.review == nil {
		return domain.Review{}, false, nil
	}
	return *f.review, true, nil
}
func (f *fakeStore) InsertReviewRun(_ context.Context, r domain.ReviewRun) error {
	if f.insertErr != nil {
		winner := r
		winner.ID = "winner-" + r.ID
		f.runs = append(f.runs, winner)
		return f.insertErr
	}
	for _, existing := range f.runs {
		if existing.SessionID == r.SessionID &&
			existing.PRURL == r.PRURL &&
			existing.TargetSHA == r.TargetSHA &&
			existing.TargetSHA != "" &&
			existing.Status != domain.ReviewRunFailed &&
			existing.Status != domain.ReviewRunCancelled &&
			(existing.Status == domain.ReviewRunRunning ||
				(existing.Verdict != domain.VerdictNone && existing.Verdict != domain.VerdictChangesRequested)) {
			return domain.ErrDuplicateReviewRun
		}
	}
	f.runs = append(f.runs, r)
	return nil
}
func (f *fakeStore) UpdateReviewRunResult(_ context.Context, id string, status domain.ReviewRunStatus, verdict domain.ReviewVerdict, body, githubReviewID string) (bool, error) {
	for i := range f.runs {
		if f.runs[i].ID == id {
			if f.runs[i].Status != domain.ReviewRunRunning {
				return false, nil
			}
			f.runs[i].Status = status
			f.runs[i].Verdict = verdict
			f.runs[i].Body = body
			f.runs[i].GithubReviewID = githubReviewID
			return true, nil
		}
	}
	return false, nil
}
func (f *fakeStore) SupersedeStaleRunningReviewRuns(_ context.Context, sessionID domain.SessionID, prURL, targetSHA, body string) (int64, error) {
	var n int64
	for i := range f.runs {
		if f.runs[i].SessionID == sessionID && f.runs[i].PRURL == prURL && f.runs[i].TargetSHA != targetSHA && f.runs[i].Status == domain.ReviewRunRunning && f.runs[i].Verdict == domain.VerdictNone {
			f.runs[i].Status = domain.ReviewRunFailed
			f.runs[i].Body = body
			n++
		}
	}
	return n, nil
}
func (f *fakeStore) CancelRunningReviewRunsBySession(_ context.Context, sessionID domain.SessionID, body string) (int64, error) {
	var n int64
	for i := range f.runs {
		if f.runs[i].SessionID == sessionID && f.runs[i].Status == domain.ReviewRunRunning && f.runs[i].Verdict == domain.VerdictNone {
			f.runs[i].Status = domain.ReviewRunCancelled
			f.runs[i].Body = body
			n++
		}
	}
	return n, nil
}
func (f *fakeStore) GetReviewRun(_ context.Context, id string) (domain.ReviewRun, bool, error) {
	for _, r := range f.runs {
		if r.ID == id {
			return r, true, nil
		}
	}
	return domain.ReviewRun{}, false, nil
}
func (f *fakeStore) GetReviewRunBySessionPRAndSHA(_ context.Context, sessionID domain.SessionID, prURL, sha string) (domain.ReviewRun, bool, error) {
	for i := len(f.runs) - 1; i >= 0; i-- {
		if f.runs[i].SessionID == sessionID && f.runs[i].PRURL == prURL && f.runs[i].TargetSHA == sha {
			return f.runs[i], true, nil
		}
	}
	return domain.ReviewRun{}, false, nil
}
func (f *fakeStore) ListReviewRunsBySession(_ context.Context, _ domain.SessionID) ([]domain.ReviewRun, error) {
	f.listAllReviewRunHits++
	return f.runs, nil
}
func (f *fakeStore) ListRunningReviewRunsBySession(_ context.Context, sessionID domain.SessionID) ([]domain.ReviewRun, error) {
	out := make([]domain.ReviewRun, 0)
	for _, run := range f.runs {
		if run.SessionID == sessionID && run.Status == domain.ReviewRunRunning && run.Verdict == domain.VerdictNone {
			out = append(out, run)
		}
	}
	return out, nil
}

type fakeSessions struct {
	rec     domain.SessionRecord
	ok      bool
	listErr error
}

func (f fakeSessions) GetSession(_ context.Context, _ domain.SessionID) (domain.SessionRecord, bool, error) {
	return f.rec, f.ok, nil
}
func (f fakeSessions) ListAllSessions(_ context.Context) ([]domain.SessionRecord, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	if !f.ok {
		return nil, nil
	}
	return []domain.SessionRecord{f.rec}, nil
}

type fakePRs struct{ prs []domain.PullRequest }

func (f fakePRs) ListPRsBySession(_ context.Context, _ domain.SessionID) ([]domain.PullRequest, error) {
	return f.prs, nil
}

type fakeProjects struct{ cfg domain.ProjectConfig }

func (f fakeProjects) GetProject(_ context.Context, id string) (domain.ProjectRecord, bool, error) {
	return domain.ProjectRecord{ID: id, Config: f.cfg}, true, nil
}

type fakeLauncher struct {
	handle           string
	canonicalHandle  string
	handleErr        error
	alive            bool
	spawnErr         error
	notifyErr        error
	spawned          bool
	spawnCount       int
	notified         bool
	cancelled        bool
	cancelErr        error
	stopErr          error
	aliveErr         error
	gotSpec          LaunchSpec
	gotHandle        string
	aliveHandle      string
	cancelledHandle  string
	cancelledHarness domain.ReviewerHarness
	stoppedHandle    string
	specs            []LaunchSpec
	handles          []string
}

func (f *fakeLauncher) HandleFor(workerID domain.SessionID) (string, error) {
	if f.handleErr != nil {
		return "", f.handleErr
	}
	if f.canonicalHandle != "" {
		return f.canonicalHandle, nil
	}
	return reviewerSessionID(workerID), nil
}
func (f *fakeLauncher) Spawn(_ context.Context, spec LaunchSpec) (string, error) {
	f.spawned = true
	f.spawnCount++
	f.gotSpec = spec
	f.specs = append(f.specs, spec)
	if f.spawnErr != nil {
		return "", f.spawnErr
	}
	return f.handle, nil
}
func (f *fakeLauncher) Notify(_ context.Context, handleID string, spec LaunchSpec) error {
	f.notified = true
	f.gotHandle = handleID
	f.gotSpec = spec
	f.handles = append(f.handles, handleID)
	f.specs = append(f.specs, spec)
	return f.notifyErr
}
func (f *fakeLauncher) Alive(_ context.Context, handleID string) (bool, error) {
	f.aliveHandle = handleID
	return f.alive || f.spawned, f.aliveErr
}
func (f *fakeLauncher) Cancel(_ context.Context, handleID string, harness domain.ReviewerHarness) error {
	f.cancelled = true
	f.cancelledHandle = handleID
	f.cancelledHarness = harness
	return f.cancelErr
}
func (f *fakeLauncher) Stop(_ context.Context, handleID string) error {
	f.stoppedHandle = handleID
	if f.stopErr == nil {
		f.alive = false
		f.spawned = false
	}
	return f.stopErr
}

func liveWorker() domain.SessionRecord {
	return domain.SessionRecord{
		ID:        "mer-1",
		ProjectID: "mer",
		Harness:   domain.HarnessClaudeCode,
		Metadata:  domain.SessionMetadata{WorkspacePath: "/ws/mer-1"},
	}
}

func newEngineForTest(store Store, sessions Sessions, prs PRs, projects Projects, launcher Launcher) *Engine {
	ids := 0
	return New(Deps{
		Store: store, Sessions: sessions, PRs: prs, Projects: projects, Launcher: launcher,
		Clock: func() time.Time { return time.Unix(0, 0).UTC() },
		NewID: func() string { ids++; return "id-" + string(rune('0'+ids)) },
	})
}

func prAt(sha string) fakePRs {
	return fakePRs{prs: []domain.PullRequest{{URL: "https://github.com/o/r/pull/1", Number: 1, HeadSHA: sha}}}
}

func TestReconcileDisabledStopsReviewerAndPreservesHistory(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "review-mer-1"},
		runs: []domain.ReviewRun{
			{ID: "run-running", SessionID: "mer-1", Status: domain.ReviewRunRunning, Harness: domain.ReviewerClaudeCode},
			{ID: "run-complete", SessionID: "mer-1", Status: domain.ReviewRunComplete, Verdict: domain.VerdictApproved, Harness: domain.ReviewerClaudeCode},
		},
	}
	launcher := &fakeLauncher{alive: true}
	eng := New(Deps{
		Store: store, Sessions: fakeSessions{rec: domain.SessionRecord{ID: "mer-1"}, ok: true}, Launcher: launcher,
		Policy: domain.NewAgentPolicy([]domain.AgentHarness{domain.HarnessClaudeCode}),
	})

	if err := eng.ReconcileDisabled(context.Background()); err != nil {
		t.Fatalf("ReconcileDisabled: %v", err)
	}
	if launcher.stoppedHandle != "review-mer-1" {
		t.Fatalf("stopped handle = %q, want review-mer-1", launcher.stoppedHandle)
	}
	if store.review == nil || store.review.ID != "rev-1" || store.review.ReviewerHandleID != "review-mer-1" {
		t.Fatalf("review history changed: %+v", store.review)
	}
	if store.runs[0].Status != domain.ReviewRunCancelled || !strings.Contains(store.runs[0].Body, "disabled by policy") {
		t.Fatalf("running review was not cancelled with policy reason: %+v", store.runs[0])
	}
	if store.runs[1].Status != domain.ReviewRunComplete || store.runs[1].Verdict != domain.VerdictApproved {
		t.Fatalf("completed review history changed: %+v", store.runs[1])
	}
}

func TestReconcileDisabledStopFailureLeavesRunRunningAndFailsClosed(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "review-mer-1"},
		runs:   []domain.ReviewRun{{ID: "run-1", SessionID: "mer-1", Status: domain.ReviewRunRunning}},
	}
	launcher := &fakeLauncher{alive: true, stopErr: errors.New("destroy failed")}
	eng := New(Deps{
		Store: store, Sessions: fakeSessions{rec: domain.SessionRecord{ID: "mer-1"}, ok: true}, Launcher: launcher,
		Policy: domain.NewAgentPolicy([]domain.AgentHarness{domain.HarnessClaudeCode}),
	})

	err := eng.ReconcileDisabled(context.Background())
	if !errors.Is(err, ErrDisabledAgentRetirement) {
		t.Fatalf("ReconcileDisabled error = %v, want ErrDisabledAgentRetirement", err)
	}
	if store.runs[0].Status != domain.ReviewRunRunning {
		t.Fatalf("run status = %q, want running while runtime may still be alive", store.runs[0].Status)
	}
}

func TestReconcileDisabledSkipsInventoryWhenPolicyInactive(t *testing.T) {
	eng := New(Deps{
		Store:    &fakeStore{},
		Sessions: fakeSessions{listErr: errors.New("inventory unavailable")},
		Launcher: &fakeLauncher{},
	})

	if err := eng.ReconcileDisabled(context.Background()); err != nil {
		t.Fatalf("inactive policy reconciliation = %v, want nil without inventory", err)
	}
}

// --- tests ---

func TestTriggerSpawnsNewReviewerAndRecordsRunAfterLaunch(t *testing.T) {
	store := &fakeStore{}
	launcher := &fakeLauncher{handle: "review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if !res.Created || res.ReviewerHandleID != "review-mer-1" {
		t.Fatalf("result = %+v", res)
	}
	if !launcher.spawned || launcher.notified {
		t.Fatalf("expected spawn (no live reviewer): %+v", launcher)
	}
	if res.Run.TargetSHA != "sha1" || res.Run.Status != domain.ReviewRunRunning || res.Run.Harness != domain.ReviewerClaudeCode {
		t.Fatalf("run = %+v", res.Run)
	}
	if launcher.gotSpec.RunID != res.Run.ID {
		t.Fatalf("launch spec run id %q != run id %q", launcher.gotSpec.RunID, res.Run.ID)
	}
	if len(store.runs) != 1 || store.review == nil || store.review.ReviewerHandleID != "review-mer-1" {
		t.Fatalf("persisted review=%+v runs=%+v", store.review, store.runs)
	}
}

func TestCancelInterruptsReviewerAndCancelsRunningRuns(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerCodex, ReviewerHandleID: "review-mer-1"},
		runs: []domain.ReviewRun{
			{ID: "run-1", ReviewID: "rev-1", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1", Status: domain.ReviewRunRunning},
			{ID: "run-2", ReviewID: "rev-1", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/2", TargetSHA: "sha2", Status: domain.ReviewRunComplete, Verdict: domain.VerdictApproved},
		},
	}
	launcher := &fakeLauncher{}
	prs := fakePRs{prs: []domain.PullRequest{
		{URL: "https://github.com/o/r/pull/1", Number: 1, HeadSHA: "sha1"},
		{URL: "https://github.com/o/r/pull/2", Number: 2, HeadSHA: "sha2"},
	}}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prs, fakeProjects{}, launcher)

	res, err := eng.Cancel(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !launcher.cancelled || launcher.cancelledHandle != "review-mer-1" {
		t.Fatalf("launcher cancel = %v handle=%q", launcher.cancelled, launcher.cancelledHandle)
	}
	if launcher.cancelledHarness != domain.ReviewerCodex {
		t.Fatalf("cancel harness = %q, want codex", launcher.cancelledHarness)
	}
	if len(res.CancelledRuns) != 1 || res.CancelledRuns[0].ID != "run-1" {
		t.Fatalf("cancelled runs = %+v", res.CancelledRuns)
	}
	if store.runs[0].Status != domain.ReviewRunCancelled || !strings.Contains(store.runs[0].Body, "cancelled") {
		t.Fatalf("run not marked cancelled: %+v", store.runs[0])
	}
	if store.runs[1].Status != domain.ReviewRunComplete {
		t.Fatalf("non-running run was changed: %+v", store.runs[1])
	}
	if store.listAllReviewRunHits != 1 {
		t.Fatalf("full review run list calls = %d, want 1 for final plan refresh only", store.listAllReviewRunHits)
	}
	if res.Reviews[0].Status == ReviewStateRunning {
		t.Fatalf("review state still running: %+v", res.Reviews[0])
	}
}

func TestCancelMarksRunsCancelledWhenReviewerHandleIsGone(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerCodex, ReviewerHandleID: "review-mer-1"},
		runs: []domain.ReviewRun{{
			ID: "run-1", ReviewID: "rev-1", SessionID: "mer-1",
			PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1", Status: domain.ReviewRunRunning,
		}},
	}
	launcher := &fakeLauncher{cancelErr: errors.New("runtime: session not found")}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	res, err := eng.Cancel(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !launcher.cancelled {
		t.Fatal("expected launcher cancellation to be attempted")
	}
	if got := store.runs[0]; got.Status != domain.ReviewRunCancelled {
		t.Fatalf("run not marked cancelled after stale handle: %+v", got)
	}
	if len(res.CancelledRuns) != 1 || res.CancelledRuns[0].ID != "run-1" {
		t.Fatalf("cancelled runs = %+v", res.CancelledRuns)
	}
}

func TestCancelKeepsRunsRunningWhenReviewerCancelFailsAndHandleIsAlive(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerCodex, ReviewerHandleID: "review-mer-1"},
		runs: []domain.ReviewRun{{
			ID: "run-1", ReviewID: "rev-1", SessionID: "mer-1",
			PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1", Status: domain.ReviewRunRunning,
		}},
	}
	launcher := &fakeLauncher{alive: true, cancelErr: errors.New("interrupt failed")}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	if _, err := eng.Cancel(context.Background(), "mer-1"); err == nil {
		t.Fatal("Cancel err = nil, want interrupt failure")
	}
	if got := store.runs[0]; got.Status != domain.ReviewRunRunning {
		t.Fatalf("run should remain running when reviewer is still alive: %+v", got)
	}
}

func TestTriggerConcurrentSameWorkerSpawnsOnce(t *testing.T) {
	store := &fakeStore{}
	launcher := &fakeLauncher{handle: "review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	const n = 8
	var wg sync.WaitGroup
	results := make([]TriggerResult, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = eng.Trigger(context.Background(), "mer-1")
		}(i)
	}
	wg.Wait()

	created := 0
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("Trigger[%d]: %v", i, errs[i])
		}
		if results[i].Created {
			created++
		}
	}
	if created != 1 {
		t.Errorf("Created=true count = %d, want exactly 1", created)
	}
	if launcher.spawnCount != 1 {
		t.Errorf("reviewer spawn count = %d, want 1", launcher.spawnCount)
	}
	if len(store.runs) != 1 {
		t.Errorf("recorded review runs = %d, want 1", len(store.runs))
	}
}

func TestTriggerFallsBackToExistingRunOnUniqueConflict(t *testing.T) {
	// The idempotency check passes (no run yet), but the insert loses to a
	// concurrent writer the unique index already accepted.
	store := &fakeStore{insertErr: domain.ErrDuplicateReviewRun}
	launcher := &fakeLauncher{handle: "review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if res.Created {
		t.Fatalf("expected Created=false on unique conflict: %+v", res)
	}
	if res.Run.TargetSHA != "sha1" || !strings.HasPrefix(res.Run.ID, "winner-") {
		t.Fatalf("expected the recorded winner run, got %+v", res.Run)
	}
	if launcher.spawnCount != 0 {
		t.Fatalf("reviewer should not launch after unique conflict: %+v", launcher)
	}
}

func TestTriggerIsIdempotentForSameCommit(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "review-mer-1"},
		runs: []domain.ReviewRun{{
			ID: "run-1", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1",
			Status: domain.ReviewRunComplete, Verdict: domain.VerdictApproved,
		}},
	}
	launcher := &fakeLauncher{alive: true}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if res.Created || res.Run.ID != "run-1" || res.ReviewerHandleID != "review-mer-1" {
		t.Fatalf("expected reuse of existing run: %+v", res)
	}
	if launcher.spawned || launcher.notified {
		t.Fatalf("should not launch for an already-reviewed commit: %+v", launcher)
	}
	if len(store.runs) != 1 {
		t.Fatalf("should not insert another run: %+v", store.runs)
	}
}

func TestTriggerReusesRunningRowWithNoVerdict(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "review-mer-1"},
		runs:   []domain.ReviewRun{{ID: "run-1", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1", Status: domain.ReviewRunRunning}},
	}
	launcher := &fakeLauncher{alive: false, handle: "review-mer-2"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if res.Created || res.Run.ID != "run-1" {
		t.Fatalf("expected reuse of the running review for the same commit: %+v", res)
	}
	if launcher.spawned || launcher.notified {
		t.Fatalf("running same-commit review should not relaunch: %+v", launcher)
	}
	if got := store.runs[0]; got.Status != domain.ReviewRunRunning {
		t.Fatalf("running row should remain running, got %+v", got)
	}
}

func TestTriggerRetriesTerminalRowWithNoVerdict(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "review-mer-1"},
		runs: []domain.ReviewRun{{
			ID: "run-empty-verdict", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1",
			Status: domain.ReviewRunComplete, Verdict: domain.VerdictNone,
		}},
	}
	launcher := &fakeLauncher{handle: "review-mer-2"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if !res.Created || res.Run.ID == "run-empty-verdict" {
		t.Fatalf("expected retry to create a new run, got %+v", res)
	}
	if len(store.runs) != 2 || !launcher.spawned {
		t.Fatalf("expected new launch/run after terminal empty-verdict row: launched=%v runs=%+v", launcher.spawned, store.runs)
	}
}

func TestTriggerNotifiesLiveReviewerOnNewCommit(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "review-mer-1"},
		runs:   []domain.ReviewRun{{ID: "run-0", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha0", Status: domain.ReviewRunComplete}},
	}
	launcher := &fakeLauncher{alive: true}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if !launcher.notified || launcher.spawned {
		t.Fatalf("expected notify on live reviewer: %+v", launcher)
	}
	if launcher.gotHandle != "review-mer-1" {
		t.Fatalf("notify handle = %q", launcher.gotHandle)
	}
	if !res.Created || res.Run.TargetSHA != "sha1" || len(store.runs) != 2 {
		t.Fatalf("expected a new run for sha1: res=%+v runs=%+v", res, store.runs)
	}
}

func TestTriggerSupersedesOlderRunningRunOnNewCommit(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "review-mer-1"},
		runs:   []domain.ReviewRun{{ID: "run-old", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha0", Status: domain.ReviewRunRunning}},
	}
	launcher := &fakeLauncher{alive: true, handle: "review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if !res.Created || res.Run.TargetSHA != "sha1" {
		t.Fatalf("expected new run for new commit, got %+v", res)
	}
	if old := store.runs[0]; old.ID != "run-old" || old.Status != domain.ReviewRunFailed {
		t.Fatalf("expected older running run to be failed, got %+v", old)
	}
	if !launcher.notified || launcher.spawned {
		t.Fatalf("expected live reviewer pane reused for new commit: %+v", launcher)
	}
}

func TestTriggerSpawnsWhenReviewerDead(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "review-mer-1"},
		runs:   []domain.ReviewRun{{ID: "run-0", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha0", Status: domain.ReviewRunComplete}},
	}
	launcher := &fakeLauncher{alive: false, handle: "review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	if _, err := eng.Trigger(context.Background(), "mer-1"); err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if !launcher.spawned || launcher.notified {
		t.Fatalf("expected spawn when reviewer dead: %+v", launcher)
	}
}

func TestTriggerLaunchFailureRecordsFailedRun(t *testing.T) {
	store := &fakeStore{}
	launcher := &fakeLauncher{spawnErr: fmt.Errorf("claude: %w", ports.ErrAgentBinaryNotFound)}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	if _, err := eng.Trigger(context.Background(), "mer-1"); !errors.Is(err, ports.ErrAgentBinaryNotFound) {
		t.Fatalf("err = %v, want ports.ErrAgentBinaryNotFound", err)
	}
	if store.review == nil || len(store.runs) != 1 {
		t.Fatalf("expected persisted failed review/run: review=%+v runs=%+v", store.review, store.runs)
	}
	run := store.runs[0]
	if run.Status != domain.ReviewRunFailed || run.Verdict != domain.VerdictNone {
		t.Fatalf("run = %+v, want failed with no verdict", run)
	}
	if !strings.Contains(run.Body, "claude") || !strings.Contains(run.Body, ports.ErrAgentBinaryNotFound.Error()) {
		t.Fatalf("run body = %q, want launch cause", run.Body)
	}
}

func TestTriggerBindingFailureStopsFreshReviewerAndFailsRun(t *testing.T) {
	store := &fakeStore{upsertErrAt: 2, upsertErr: errors.New("database unavailable")}
	launcher := &fakeLauncher{handle: "review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	_, err := eng.Trigger(context.Background(), "mer-1")
	if err == nil || !strings.Contains(err.Error(), "bind launched reviewer handle") {
		t.Fatalf("Trigger error = %v, want binding failure", err)
	}
	if launcher.spawnCount != 1 || launcher.stoppedHandle != "review-mer-1" {
		t.Fatalf("partial launch cleanup = %+v, want spawned handle strictly stopped", launcher)
	}
	if store.review == nil || store.review.Harness != domain.ReviewerClaudeCode || store.review.ReviewerHandleID != "" {
		t.Fatalf("pre-launch identity changed unexpectedly: %+v", store.review)
	}
	if len(store.runs) != 1 || store.runs[0].Status != domain.ReviewRunFailed || !strings.Contains(store.runs[0].Body, "database unavailable") {
		t.Fatalf("run after safely stopped partial launch = %+v, want failed with binding cause", store.runs)
	}
}

func TestTriggerBindingAndStopFailureRemainsDiscoverableToDisabledReconcile(t *testing.T) {
	store := &fakeStore{upsertErrAt: 2, upsertErr: errors.New("database unavailable")}
	launcher := &fakeLauncher{handle: "runtime-review-mer-1", canonicalHandle: "runtime-review-mer-1", stopErr: errors.New("destroy failed")}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	_, err := eng.Trigger(context.Background(), "mer-1")
	if !errors.Is(err, ErrReviewerBindingIncomplete) || !strings.Contains(err.Error(), "database unavailable") || !strings.Contains(err.Error(), "destroy failed") {
		t.Fatalf("Trigger error = %v, want fail-closed binding and stop evidence", err)
	}
	if store.review == nil || store.review.Harness != domain.ReviewerClaudeCode || store.review.ReviewerHandleID != "" {
		t.Fatalf("partial-launch identity = %+v, want desired harness with canonical handle recoverable from session", store.review)
	}
	if len(store.runs) != 1 || store.runs[0].Status != domain.ReviewRunRunning {
		t.Fatalf("live undisposed reviewer run = %+v, want running until runtime is stopped", store.runs)
	}

	// The explicit handle binding is empty, but policy reconciliation resolves
	// the runtime's canonical handle and still retires the live Claude
	// reviewer before cancelling its running record.
	launcher.stopErr = nil
	launcher.stoppedHandle = ""
	eng.policy = domain.NewAgentPolicy([]domain.AgentHarness{domain.HarnessClaudeCode})
	if err := eng.ReconcileDisabled(context.Background()); err != nil {
		t.Fatalf("ReconcileDisabled: %v", err)
	}
	if launcher.stoppedHandle != "runtime-review-mer-1" {
		t.Fatalf("canonical reviewer handle was not stopped: %q", launcher.stoppedHandle)
	}
	if store.runs[0].Status != domain.ReviewRunCancelled || !strings.Contains(store.runs[0].Body, "disabled by policy") {
		t.Fatalf("reconciled partial-launch run = %+v, want cancelled history", store.runs[0])
	}
}

func TestTriggerRecoversLiveReviewerWithIncompleteBindingWithoutDuplicateLaunch(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode},
		runs:   []domain.ReviewRun{{ID: "run-1", ReviewID: "rev-1", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1", Status: domain.ReviewRunRunning}},
	}
	launcher := &fakeLauncher{alive: true, canonicalHandle: "runtime-review-mer-1", handle: "review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if res.Created || res.ReviewerHandleID != "runtime-review-mer-1" || launcher.spawned || launcher.notified {
		t.Fatalf("recovered trigger = %+v launcher=%+v, want existing run and reviewer", res, launcher)
	}
	if store.review == nil || store.review.ReviewerHandleID != "runtime-review-mer-1" || launcher.aliveHandle != "runtime-review-mer-1" {
		t.Fatalf("review binding = %+v, want recovered canonical handle", store.review)
	}
	if len(store.runs) != 1 || store.runs[0].Status != domain.ReviewRunRunning {
		t.Fatalf("review run history changed during recovery: %+v", store.runs)
	}
}

func TestTriggerRecoversLiveReviewerAndNotifiesNewRun(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode},
		runs:   []domain.ReviewRun{{ID: "run-old", ReviewID: "rev-1", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha0", Status: domain.ReviewRunComplete, Verdict: domain.VerdictApproved}},
	}
	launcher := &fakeLauncher{alive: true, canonicalHandle: "runtime-review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if !res.Created || !launcher.notified || launcher.gotHandle != "runtime-review-mer-1" || launcher.spawned {
		t.Fatalf("recovered trigger = %+v launcher=%+v, want canonical-handle notify without spawn", res, launcher)
	}
	if store.review == nil || store.review.ReviewerHandleID != "runtime-review-mer-1" {
		t.Fatalf("review binding = %+v, want recovered canonical handle", store.review)
	}
	if len(store.runs) != 2 || store.runs[0].ID != "run-old" || store.runs[0].Status != domain.ReviewRunComplete || store.runs[1].Status != domain.ReviewRunRunning {
		t.Fatalf("review history after recovered notify = %+v", store.runs)
	}
}

func TestTriggerIncompleteBindingProbeFailureDoesNotSpawn(t *testing.T) {
	store := &fakeStore{review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode}}
	launcher := &fakeLauncher{aliveErr: errors.New("runtime unavailable"), handle: "review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	if _, err := eng.Trigger(context.Background(), "mer-1"); err == nil || !strings.Contains(err.Error(), "probe canonical reviewer handle") {
		t.Fatalf("Trigger error = %v, want derived-handle probe failure", err)
	}
	if launcher.spawned || launcher.notified || len(store.runs) != 0 {
		t.Fatalf("failed recovery caused reviewer work: launcher=%+v runs=%+v", launcher, store.runs)
	}
}

func TestTriggerRetriesAfterFailedRunForSameCommit(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "review-mer-1"},
		runs:   []domain.ReviewRun{{ID: "run-failed", ReviewID: "rev-1", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1", Status: domain.ReviewRunFailed}},
	}
	launcher := &fakeLauncher{handle: "review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if !res.Created || res.Run.ID == "run-failed" {
		t.Fatalf("expected retry to create a new run, got %+v", res)
	}
	if len(store.runs) != 2 || !launcher.spawned {
		t.Fatalf("expected new launch/run after failed pass: launched=%v runs=%+v", launcher.spawned, store.runs)
	}
}

func TestTriggerRetriesAfterCancelledRunForSameCommit(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "review-mer-1"},
		runs:   []domain.ReviewRun{{ID: "run-cancelled", ReviewID: "rev-1", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1", Status: domain.ReviewRunCancelled}},
	}
	launcher := &fakeLauncher{handle: "review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if !res.Created || res.Run.ID == "run-cancelled" {
		t.Fatalf("expected retry to create a new run, got %+v", res)
	}
	if len(store.runs) != 2 || !launcher.spawned {
		t.Fatalf("expected new launch/run after cancelled pass: launched=%v runs=%+v", launcher.spawned, store.runs)
	}
}

func TestTriggerCreatesRunsForMultipleEligiblePRsWithOneReviewer(t *testing.T) {
	store := &fakeStore{}
	launcher := &fakeLauncher{handle: "review-mer-1"}
	prs := fakePRs{prs: []domain.PullRequest{
		{URL: "https://github.com/o/r/pull/1", Number: 1, HeadSHA: "sha1"},
		{URL: "https://github.com/o/r/pull/2", Number: 2, HeadSHA: "sha2"},
	}}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prs, fakeProjects{}, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if !res.Created || len(res.CreatedRuns) != 2 || len(store.runs) != 2 {
		t.Fatalf("created batch = %+v runs=%+v", res, store.runs)
	}
	if res.CreatedRuns[0].BatchID == "" || res.CreatedRuns[0].BatchID != res.CreatedRuns[1].BatchID {
		t.Fatalf("created runs should share one batch id: %+v", res.CreatedRuns)
	}
	if launcher.spawnCount != 1 || len(launcher.handles) != 0 {
		t.Fatalf("expected one spawn and no extra notify, launcher=%+v", launcher)
	}
	if len(launcher.specs) != 1 {
		t.Fatalf("launch specs = %d, want 1: %+v", len(launcher.specs), launcher.specs)
	}
	spec := launcher.specs[0]
	if spec.ReviewIndex != 0 || len(spec.ReviewQueue) != 2 {
		t.Fatalf("spec queue context = index %d queue %+v", spec.ReviewIndex, spec.ReviewQueue)
	}
	if spec.ReviewQueue[0].PRURL != "https://github.com/o/r/pull/1" || spec.ReviewQueue[1].PRURL != "https://github.com/o/r/pull/2" {
		t.Fatalf("spec queue URLs = %+v", spec.ReviewQueue)
	}
	if store.review == nil || store.review.ReviewerHandleID != "review-mer-1" || store.review.PRURL != "" {
		t.Fatalf("review row = %+v, want shared handle and no behavioral pr_url", store.review)
	}
}

func TestTriggerAllowsTwoPRsWithSameHeadSHA(t *testing.T) {
	store := &fakeStore{}
	launcher := &fakeLauncher{handle: "review-mer-1"}
	prs := fakePRs{prs: []domain.PullRequest{
		{URL: "https://github.com/o/r/pull/1", Number: 1, HeadSHA: "same"},
		{URL: "https://github.com/o/r/pull/2", Number: 2, HeadSHA: "same"},
	}}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prs, fakeProjects{}, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if len(res.CreatedRuns) != 2 {
		t.Fatalf("created runs = %d, want 2: %+v", len(res.CreatedRuns), res.CreatedRuns)
	}
	if store.runs[0].PRURL == store.runs[1].PRURL || store.runs[0].TargetSHA != store.runs[1].TargetSHA {
		t.Fatalf("runs should differ by PR only: %+v", store.runs)
	}
}

func TestTriggerSkipsApprovedAndRunningCurrentHead(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "review-mer-1"},
		runs: []domain.ReviewRun{
			{ID: "approved", ReviewID: "rev-1", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1", Status: domain.ReviewRunComplete, Verdict: domain.VerdictApproved, CreatedAt: time.Unix(1, 0)},
			{ID: "running", ReviewID: "rev-1", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/2", TargetSHA: "sha2", Status: domain.ReviewRunRunning, CreatedAt: time.Unix(2, 0)},
		},
	}
	launcher := &fakeLauncher{alive: true}
	prs := fakePRs{prs: []domain.PullRequest{
		{URL: "https://github.com/o/r/pull/1", Number: 1, HeadSHA: "sha1"},
		{URL: "https://github.com/o/r/pull/2", Number: 2, HeadSHA: "sha2"},
	}}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prs, fakeProjects{}, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if res.Created || len(res.CreatedRuns) != 0 || launcher.spawned || launcher.notified {
		t.Fatalf("expected no new work: res=%+v launcher=%+v", res, launcher)
	}
	if len(res.Reviews) != 2 || res.Reviews[0].Status != ReviewStateUpToDate || res.Reviews[1].Status != ReviewStateRunning {
		t.Fatalf("review states = %+v", res.Reviews)
	}
}

func TestTriggerCreatesRunForChangesRequestedCurrentHead(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "review-mer-1"},
		runs: []domain.ReviewRun{{
			ID: "changes", ReviewID: "rev-1", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1",
			Status: domain.ReviewRunComplete, Verdict: domain.VerdictChangesRequested, CreatedAt: time.Unix(1, 0),
		}},
	}
	launcher := &fakeLauncher{alive: true, handle: "review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if !res.Created || len(res.CreatedRuns) != 1 || !launcher.notified || launcher.spawned {
		t.Fatalf("expected rerun on changes_requested current head: res=%+v launcher=%+v", res, launcher)
	}
}

func TestTriggerUsesConfiguredReviewerHarness(t *testing.T) {
	store := &fakeStore{}
	projects := fakeProjects{cfg: domain.ProjectConfig{Reviewers: []domain.ReviewerConfig{{Harness: domain.ReviewerHarness("greptile")}}}}
	launcher := &fakeLauncher{handle: "review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), projects, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if res.Run.Harness != domain.ReviewerHarness("greptile") || launcher.gotSpec.Harness != domain.ReviewerHarness("greptile") {
		t.Fatalf("harness not used: run=%+v spec=%+v", res.Run, launcher.gotSpec)
	}
}

func TestTriggerStopsLiveClaudeBeforeSwitchingPersistedHandleToCodex(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "review-mer-1"},
		runs: []domain.ReviewRun{{
			ID: "run-claude", ReviewID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode,
			PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1", Status: domain.ReviewRunRunning,
		}},
	}
	projects := fakeProjects{cfg: domain.ProjectConfig{Reviewers: []domain.ReviewerConfig{{Harness: domain.ReviewerCodex}}}}
	launcher := &fakeLauncher{alive: true, handle: "review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), projects, launcher)

	res, err := eng.Trigger(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if launcher.stoppedHandle != "review-mer-1" || !launcher.spawned || launcher.notified {
		t.Fatalf("harness switch runtime actions = %+v, want stop then fresh spawn", launcher)
	}
	if launcher.gotSpec.Harness != domain.ReviewerCodex || res.Run.Harness != domain.ReviewerCodex {
		t.Fatalf("new reviewer harness: spec=%q run=%q, want codex", launcher.gotSpec.Harness, res.Run.Harness)
	}
	if store.review == nil || store.review.Harness != domain.ReviewerCodex || store.review.ReviewerHandleID != "review-mer-1" {
		t.Fatalf("persisted review = %+v, want codex bound to replacement handle", store.review)
	}
	if store.runs[0].Status != domain.ReviewRunCancelled || store.runs[0].Harness != domain.ReviewerClaudeCode {
		t.Fatalf("Claude review history changed incorrectly: %+v", store.runs[0])
	}
	if len(store.runs) != 2 || store.runs[1].Harness != domain.ReviewerCodex || store.runs[1].Status != domain.ReviewRunRunning {
		t.Fatalf("replacement run history = %+v, want retained Claude run plus running Codex run", store.runs)
	}
}

func TestTriggerHarnessSwitchStopFailureKeepsClaudeIdentityForPolicyReconcile(t *testing.T) {
	original := domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "review-mer-1"}
	store := &fakeStore{review: &original}
	projects := fakeProjects{cfg: domain.ProjectConfig{Reviewers: []domain.ReviewerConfig{{Harness: domain.ReviewerCodex}}}}
	launcher := &fakeLauncher{alive: true, handle: "review-mer-1", stopErr: errors.New("destroy failed")}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), projects, launcher)

	if _, err := eng.Trigger(context.Background(), "mer-1"); err == nil || !strings.Contains(err.Error(), "destroy failed") {
		t.Fatalf("Trigger error = %v, want stop failure", err)
	}
	if store.review == nil || *store.review != original {
		t.Fatalf("stop failure relabelled persisted reviewer: got %+v want %+v", store.review, original)
	}
	if len(store.runs) != 0 || launcher.spawned || launcher.notified {
		t.Fatalf("stop failure created replacement side effects: runs=%+v launcher=%+v", store.runs, launcher)
	}

	// Because Trigger left the row bound to Claude, enabling the disabled policy
	// still discovers and retires the old runtime on the boot reconciliation path.
	launcher.stopErr = nil
	launcher.stoppedHandle = ""
	eng.policy = domain.NewAgentPolicy([]domain.AgentHarness{domain.HarnessClaudeCode})
	if err := eng.ReconcileDisabled(context.Background()); err != nil {
		t.Fatalf("ReconcileDisabled after switch failure: %v", err)
	}
	if launcher.stoppedHandle != "review-mer-1" {
		t.Fatalf("disabled policy missed persisted Claude runtime: stopped=%q", launcher.stoppedHandle)
	}
}

func TestTriggerRejectsDisabledReviewerBeforePersistenceOrLaunch(t *testing.T) {
	store := &fakeStore{}
	launcher := &fakeLauncher{handle: "review-mer-1"}
	projects := fakeProjects{cfg: domain.ProjectConfig{Reviewers: []domain.ReviewerConfig{{Harness: domain.ReviewerClaudeCode}}}}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), projects, launcher)
	eng.policy = domain.NewAgentPolicy([]domain.AgentHarness{domain.HarnessClaudeCode})

	_, err := eng.Trigger(context.Background(), "mer-1")
	if !errors.Is(err, ErrAgentDisabled) {
		t.Fatalf("Trigger error = %v, want ErrAgentDisabled", err)
	}
	if store.review != nil || len(store.runs) != 0 || launcher.spawned || launcher.notified {
		t.Fatalf("disabled reviewer caused side effects: review=%+v runs=%d launcher=%+v", store.review, len(store.runs), launcher)
	}
}

func TestTriggerRejectsBadWorkerState(t *testing.T) {
	t.Run("unknown worker", func(t *testing.T) {
		eng := newEngineForTest(&fakeStore{}, fakeSessions{ok: false}, prAt("sha1"), fakeProjects{}, &fakeLauncher{})
		if _, err := eng.Trigger(context.Background(), "mer-1"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
	t.Run("no pr", func(t *testing.T) {
		eng := newEngineForTest(&fakeStore{}, fakeSessions{rec: liveWorker(), ok: true}, fakePRs{}, fakeProjects{}, &fakeLauncher{})
		if _, err := eng.Trigger(context.Background(), "mer-1"); !errors.Is(err, ErrInvalid) {
			t.Fatalf("err = %v, want ErrInvalid", err)
		}
	})
}

func TestListReturnsHandleAndRuns(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", ReviewerHandleID: "review-mer-1"},
		runs:   []domain.ReviewRun{{ID: "run-1", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1"}},
	}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, &fakeLauncher{})
	got, err := eng.List(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got.ReviewerHandleID != "review-mer-1" || len(got.Runs) != 1 {
		t.Fatalf("list = %+v", got)
	}
}

func TestListRecoversLiveReviewerWithIncompleteBinding(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerCodex},
		runs:   []domain.ReviewRun{{ID: "run-1", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1", Status: domain.ReviewRunRunning}},
	}
	launcher := &fakeLauncher{alive: true, canonicalHandle: "runtime-review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	got, err := eng.List(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got.ReviewerHandleID != "runtime-review-mer-1" || store.review == nil || store.review.ReviewerHandleID != "runtime-review-mer-1" {
		t.Fatalf("recovered list = %+v review=%+v", got, store.review)
	}
	if len(got.Runs) != 1 || got.Runs[0].ID != "run-1" || store.runs[0].Status != domain.ReviewRunRunning {
		t.Fatalf("review history changed during list recovery: got=%+v stored=%+v", got.Runs, store.runs)
	}
}

func TestCancelRecoversLiveReviewerWithIncompleteBinding(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerCodex},
		runs:   []domain.ReviewRun{{ID: "run-1", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1", Status: domain.ReviewRunRunning}},
	}
	launcher := &fakeLauncher{alive: true, canonicalHandle: "runtime-review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	res, err := eng.Cancel(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !launcher.cancelled || launcher.cancelledHandle != "runtime-review-mer-1" || launcher.cancelledHarness != domain.ReviewerCodex {
		t.Fatalf("recovered cancel did not target canonical reviewer: %+v", launcher)
	}
	if store.review == nil || store.review.ReviewerHandleID != "runtime-review-mer-1" || len(res.CancelledRuns) != 1 || store.runs[0].Status != domain.ReviewRunCancelled {
		t.Fatalf("cancel recovery result=%+v review=%+v runs=%+v", res, store.review, store.runs)
	}
}

func TestCancelIncompleteBindingWithAbsentRuntimeCancelsStaleRuns(t *testing.T) {
	store := &fakeStore{
		review: &domain.Review{ID: "rev-1", SessionID: "mer-1", Harness: domain.ReviewerCodex},
		runs:   []domain.ReviewRun{{ID: "run-1", SessionID: "mer-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1", Status: domain.ReviewRunRunning}},
	}
	launcher := &fakeLauncher{alive: false}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)

	res, err := eng.Cancel(context.Background(), "mer-1")
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if launcher.cancelled || res.ReviewerHandleID != "" {
		t.Fatalf("absent runtime should not be interrupted: launcher=%+v result=%+v", launcher, res)
	}
	if len(res.CancelledRuns) != 1 || store.runs[0].Status != domain.ReviewRunCancelled || store.review.ReviewerHandleID != "" {
		t.Fatalf("stale running review not safely cancelled: result=%+v review=%+v runs=%+v", res, store.review, store.runs)
	}
}
