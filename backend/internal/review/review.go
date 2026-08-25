// Package review holds the core code-review logic: triggering a reviewer over a
// worker's worktree, recording review runs, and accepting submitted results.
//
// It is independent of any transport. The daemon's HTTP service
// (internal/service/review) is a thin boundary over this engine today, and the
// same engine can back an in-process CLI trigger later without going through the
// API. Transport-specific concerns (DTOs, error→status mapping) stay in the
// service/controller layers; the orchestration and run-id generation live here.
package review

import (
	stdctx "context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// ErrInvalid and ErrNotFound let the transport layer map failures to 422/404.
var (
	ErrInvalid       = errors.New("review: invalid input")
	ErrNotFound      = errors.New("review: not found")
	ErrAgentDisabled = errors.New("review: agent disabled by policy")
	// ErrReviewerBindingIncomplete means a freshly launched reviewer could not
	// be durably bound to its canonical handle and could not be stopped.
	// Boot reconciliation can still discover it from the persisted session id
	// and harness.
	ErrReviewerBindingIncomplete = errors.New("review: reviewer runtime binding incomplete")
	// ErrDisabledAgentRetirement means boot reconciliation could not prove that
	// every persisted reviewer runtime for a disabled harness was stopped.
	ErrDisabledAgentRetirement = errors.New("review: disabled agent retirement incomplete")
)

// Store is the persistence surface the engine needs. *sqlite.Store satisfies it
// in production; tests use a fake.
type Store interface {
	UpsertReview(ctx stdctx.Context, r domain.Review) error
	GetReviewBySession(ctx stdctx.Context, id domain.SessionID) (domain.Review, bool, error)
	InsertReviewRun(ctx stdctx.Context, r domain.ReviewRun) error
	UpdateReviewRunResult(ctx stdctx.Context, id string, status domain.ReviewRunStatus, verdict domain.ReviewVerdict, body, githubReviewID string) (bool, error)
	SupersedeStaleRunningReviewRuns(ctx stdctx.Context, sessionID domain.SessionID, prURL, targetSHA, body string) (int64, error)
	CancelRunningReviewRunsBySession(ctx stdctx.Context, sessionID domain.SessionID, body string) (int64, error)
	GetReviewRun(ctx stdctx.Context, id string) (domain.ReviewRun, bool, error)
	GetReviewRunBySessionPRAndSHA(ctx stdctx.Context, id domain.SessionID, prURL, targetSHA string) (domain.ReviewRun, bool, error)
	ListReviewRunsBySession(ctx stdctx.Context, id domain.SessionID) ([]domain.ReviewRun, error)
	ListRunningReviewRunsBySession(ctx stdctx.Context, id domain.SessionID) ([]domain.ReviewRun, error)
}

// Sessions resolves the worker session under review.
type Sessions interface {
	GetSession(ctx stdctx.Context, id domain.SessionID) (domain.SessionRecord, bool, error)
	ListAllSessions(ctx stdctx.Context) ([]domain.SessionRecord, error)
}

// PRs resolves the PR a worker owns.
type PRs interface {
	ListPRsBySession(ctx stdctx.Context, id domain.SessionID) ([]domain.PullRequest, error)
}

// Projects resolves the per-project reviewer config.
type Projects interface {
	GetProject(ctx stdctx.Context, id string) (domain.ProjectRecord, bool, error)
}

// Deps wires the engine.
type Deps struct {
	Store    Store
	Sessions Sessions
	PRs      PRs
	Projects Projects
	Launcher Launcher
	Policy   domain.AgentPolicy

	// Clock and NewID are injectable for deterministic tests.
	Clock func() time.Time
	NewID func() string
}

// Engine is the core code-review engine.
type Engine struct {
	store    Store
	sessions Sessions
	prs      PRs
	projects Projects
	launcher Launcher
	policy   domain.AgentPolicy
	clock    func() time.Time
	newID    func() string

	// triggerMu guards triggerLocks; triggerLocks holds one mutex per worker
	// session so reviewer operations for the same worker serialise (see
	// lockWorker). Distinct workers never contend.
	triggerMu    sync.Mutex
	triggerLocks map[domain.SessionID]*sync.Mutex
}

// New wires an Engine from its dependencies, defaulting the clock and id source.
func New(d Deps) *Engine {
	clock := d.Clock
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	newID := d.NewID
	if newID == nil {
		newID = uuid.NewString
	}
	return &Engine{
		store:        d.Store,
		sessions:     d.Sessions,
		prs:          d.PRs,
		projects:     d.Projects,
		launcher:     d.Launcher,
		policy:       d.Policy,
		clock:        clock,
		newID:        newID,
		triggerLocks: make(map[domain.SessionID]*sync.Mutex),
	}
}

// ReconcileDisabled stops reviewer runtimes whose persisted harness is now
// disabled. Reviewer panes are not session rows, so the session manager cannot
// discover them during its own reconciliation pass. Review and run rows remain
// intact; only still-running runs are marked cancelled after the runtime is
// confirmed absent.
func (e *Engine) ReconcileDisabled(ctx stdctx.Context) error {
	if !e.policy.HasDisabledAgents() {
		return nil
	}
	sessions, err := e.sessions.ListAllSessions(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDisabledAgentRetirement, err)
	}
	var failures []error
	for _, session := range sessions {
		review, ok, getErr := e.store.GetReviewBySession(ctx, session.ID)
		if getErr != nil {
			failures = append(failures, fmt.Errorf("session %s review lookup: %w", session.ID, getErr))
			continue
		}
		if !ok {
			continue
		}
		if !e.policy.IsDisabled(string(review.Harness)) {
			continue
		}
		handleID := review.ReviewerHandleID
		if handleID == "" {
			// A launch can succeed while the final handle-binding write fails, so
			// an empty persisted handle is not proof that no runtime exists. Ask the
			// runtime for its canonical handle and probe it fail-closed.
			handleID, getErr = e.launcher.HandleFor(review.SessionID)
			if getErr != nil {
				failures = append(failures, fmt.Errorf("reviewer %s canonical handle: %w", review.ID, getErr))
				continue
			}
		}
		if handleID != "" {
			alive, probeErr := e.launcher.Alive(ctx, handleID)
			if probeErr != nil {
				failures = append(failures, fmt.Errorf("reviewer %s runtime probe: %w", review.ID, probeErr))
				continue
			}
			if alive {
				if stopErr := e.launcher.Stop(ctx, handleID); stopErr != nil {
					failures = append(failures, fmt.Errorf("reviewer %s: %w", review.ID, stopErr))
					continue
				}
			}
		}
		if _, cancelErr := e.store.CancelRunningReviewRunsBySession(ctx, review.SessionID, "cancelled because reviewer agent is disabled by policy"); cancelErr != nil {
			failures = append(failures, fmt.Errorf("reviewer %s cancel running review runs: %w", review.ID, cancelErr))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("%w: %w", ErrDisabledAgentRetirement, errors.Join(failures...))
	}
	return nil
}

// lockWorker serialises reviewer operations for a single worker session and
// returns the unlock func. Without it, two concurrent triggers for the same
// worker can both pass the per-commit idempotency check and each spawn a reviewer
// against the same canonical handle, leaving two running runs for one commit
// (#242). List and Cancel share the lock because they may repair an incomplete
// handle binding left by a failed post-launch persistence write.
//
// The per-worker mutex is created on first use and kept for the lifetime of the
// engine; the entry is a single pointer, so the unbounded-by-session-count map
// is a negligible, bounded-in-practice cost.
func (e *Engine) lockWorker(id domain.SessionID) func() {
	e.triggerMu.Lock()
	mu, ok := e.triggerLocks[id]
	if !ok {
		mu = &sync.Mutex{}
		e.triggerLocks[id] = mu
	}
	e.triggerMu.Unlock()
	mu.Lock()
	return mu.Unlock
}

// TriggerResult is the outcome of a trigger: the (new or existing) run, the live
// reviewer pane's handle so the UI can attach its terminal, and whether a new
// pass was started (false when an existing run for the same commit was reused).
type TriggerResult struct {
	Run              domain.ReviewRun
	ReviewerHandleID string
	Created          bool
	Reviews          []PRReviewState
	CreatedRuns      []domain.ReviewRun
}

// SessionReviews is a worker's review state: the live reviewer handle plus its
// recorded passes, newest first.
type SessionReviews struct {
	ReviewerHandleID string
	Runs             []domain.ReviewRun
	Reviews          []PRReviewState
}

// CancelResult is the review state after a reviewer pane cancellation.
type CancelResult struct {
	ReviewerHandleID string
	Reviews          []PRReviewState
	CancelledRuns    []domain.ReviewRun
}

// Trigger starts reviews for every PR on the worker session that needs review.
// It reuses running/up-to-date runs, retries failed/current changes-requested
// heads, and uses one reviewer pane for every new run in the batch.
func (e *Engine) Trigger(ctx stdctx.Context, workerID domain.SessionID) (TriggerResult, error) {
	if workerID == "" {
		return TriggerResult{}, fmt.Errorf("%w: worker session id is required", ErrInvalid)
	}

	// Serialise concurrent triggers for this worker so the idempotency check
	// below (and the reviewer spawn that follows it) can't be raced into a
	// double-spawn. Held across the spawn deliberately: the loser then re-reads
	// the freshly-recorded run and short-circuits to Created:false.
	unlock := e.lockWorker(workerID)
	defer unlock()

	worker, ok, err := e.sessions.GetSession(ctx, workerID)
	if err != nil {
		return TriggerResult{}, err
	}
	if !ok {
		return TriggerResult{}, fmt.Errorf("%w: worker session %q", ErrNotFound, workerID)
	}
	if worker.IsTerminated {
		return TriggerResult{}, fmt.Errorf("%w: worker session %q is terminated", ErrInvalid, workerID)
	}
	if worker.Metadata.WorkspacePath == "" {
		return TriggerResult{}, fmt.Errorf("%w: worker session %q has no workspace to review", ErrInvalid, workerID)
	}

	prs, err := e.prs.ListPRsBySession(ctx, workerID)
	if err != nil {
		return TriggerResult{}, err
	}
	if len(prs) == 0 {
		return TriggerResult{}, fmt.Errorf("%w: worker %q has no PR to review", ErrInvalid, workerID)
	}
	reviewRow, hasReview, err := e.store.GetReviewBySession(ctx, workerID)
	if err != nil {
		return TriggerResult{}, err
	}
	harness, err := e.reviewerHarness(ctx, worker)
	if err != nil {
		return TriggerResult{}, err
	}
	if e.policy.IsDisabled(string(harness)) {
		return TriggerResult{}, fmt.Errorf("%w: %q", ErrAgentDisabled, harness)
	}
	if hasReview {
		reviewRow, err = e.recoverReviewerBinding(ctx, reviewRow)
		if err != nil {
			return TriggerResult{}, err
		}
	}

	handleID, harnessChanged, err := e.prepareReviewerHarness(ctx, reviewRow, hasReview, harness)
	if err != nil {
		return TriggerResult{}, err
	}
	if harnessChanged {
		if _, err := e.store.CancelRunningReviewRunsBySession(ctx, workerID, fmt.Sprintf("cancelled because reviewer harness changed from %q to %q", reviewRow.Harness, harness)); err != nil {
			return TriggerResult{}, err
		}
	}

	now := e.clock()
	reviewRow, err = e.upsertReview(ctx, worker, harness, handleID, now)
	if err != nil {
		return TriggerResult{}, err
	}
	runs, err := e.store.ListReviewRunsBySession(ctx, workerID)
	if err != nil {
		return TriggerResult{}, err
	}
	reviews := Plan(prs, runs)

	var created []domain.ReviewRun
	batchID := ""
	for _, reviewState := range reviews {
		if reviewState.Status != ReviewStateNeedsReview && reviewState.Status != ReviewStateChangesRequested {
			continue
		}
		if _, err := e.store.SupersedeStaleRunningReviewRuns(ctx, workerID, reviewState.PRURL, reviewState.TargetSHA, "superseded by a review trigger for a newer commit"); err != nil {
			return TriggerResult{}, err
		}
		if batchID == "" {
			batchID = e.newID()
		}
		run := domain.ReviewRun{
			ID:        e.newID(),
			ReviewID:  reviewRow.ID,
			SessionID: workerID,
			BatchID:   batchID,
			Harness:   harness,
			PRURL:     reviewState.PRURL,
			TargetSHA: reviewState.TargetSHA,
			Status:    domain.ReviewRunRunning,
			Verdict:   domain.VerdictNone,
			CreatedAt: now,
		}
		if err := e.store.InsertReviewRun(ctx, run); err != nil {
			if errors.Is(err, domain.ErrDuplicateReviewRun) {
				if existing, ok, getErr := e.store.GetReviewRunBySessionPRAndSHA(ctx, workerID, reviewState.PRURL, reviewState.TargetSHA); getErr != nil {
					return TriggerResult{}, getErr
				} else if ok {
					reviews = replaceReviewLatestRun(reviews, reviewState.PRURL, reviewState.TargetSHA, existing)
					continue
				}
			}
			return TriggerResult{}, err
		}
		created = append(created, run)
		reviews = replaceReviewLatestRun(reviews, reviewState.PRURL, reviewState.TargetSHA, run)
	}
	if len(created) == 0 {
		return TriggerResult{Run: firstReusableRun(reviews), ReviewerHandleID: reviewRow.ReviewerHandleID, Created: false, Reviews: reviews}, nil
	}

	failRuns := func(start int, err error) error {
		for _, run := range created[start:] {
			if _, updateErr := e.store.UpdateReviewRunResult(ctx, run.ID, domain.ReviewRunFailed, domain.VerdictNone, err.Error(), ""); updateErr != nil {
				return updateErr
			}
		}
		return err
	}

	queue := reviewQueue(created)
	launched := false
	if handleID == "" {
		h, err := e.launcher.Spawn(ctx, reviewLaunchSpec(worker, harness, created[0], queue, 0))
		if err != nil {
			return TriggerResult{}, failRuns(0, fmt.Errorf("launch reviewer: %w", err))
		}
		handleID = h
		launched = true
	} else {
		if err := e.launcher.Notify(ctx, handleID, reviewLaunchSpec(worker, harness, created[0], queue, 0)); err != nil {
			return TriggerResult{}, failRuns(0, fmt.Errorf("notify reviewer: %w", err))
		}
	}
	if launched {
		reviewRow, err = e.upsertReview(ctx, worker, harness, handleID, now)
		if err != nil {
			bindErr := fmt.Errorf("bind launched reviewer handle %q: %w", handleID, err)
			if stopErr := e.launcher.Stop(ctx, handleID); stopErr != nil {
				return TriggerResult{}, fmt.Errorf("%w: handle %q remains discoverable from reviewer session %q: %w", ErrReviewerBindingIncomplete, handleID, worker.ID, errors.Join(bindErr, fmt.Errorf("stop launched reviewer: %w", stopErr)))
			}
			return TriggerResult{}, failRuns(0, bindErr)
		}
	}
	for i := range created {
		created[i].ReviewID = reviewRow.ID
	}
	return TriggerResult{Run: created[0], ReviewerHandleID: handleID, Created: true, Reviews: reviews, CreatedRuns: created}, nil
}

// prepareReviewerHarness enforces the identity invariant for a persisted
// reviewer handle: the review row's harness names the runtime behind that
// handle. A live handle can only be reused when the desired harness matches.
// On a harness change, the old runtime is strictly stopped before callers may
// relabel the row or create work for the new reviewer.
func (e *Engine) prepareReviewerHarness(ctx stdctx.Context, review domain.Review, hasReview bool, desired domain.ReviewerHarness) (handleID string, harnessChanged bool, err error) {
	if !hasReview {
		return "", false, nil
	}
	harnessChanged = review.Harness != desired
	if review.ReviewerHandleID == "" {
		return "", harnessChanged, nil
	}
	alive, err := e.launcher.Alive(ctx, review.ReviewerHandleID)
	if err != nil {
		return "", false, err
	}
	if !alive {
		return "", harnessChanged, nil
	}
	if !harnessChanged {
		return review.ReviewerHandleID, false, nil
	}
	if err := e.launcher.Stop(ctx, review.ReviewerHandleID); err != nil {
		return "", false, fmt.Errorf("replace live reviewer harness %q with %q: %w", review.Harness, desired, err)
	}
	return "", true, nil
}

// recoverReviewerBinding repairs the narrow partial-launch state where a
// reviewer runtime was created under its canonical handle but persisting that
// handle failed. A positive runtime probe is required before binding, so a stale
// empty row never invents a live reviewer. Callers serialize this with other
// operations for the same worker to avoid racing a harness replacement.
func (e *Engine) recoverReviewerBinding(ctx stdctx.Context, review domain.Review) (domain.Review, error) {
	if review.ReviewerHandleID != "" {
		return review, nil
	}
	handleID, err := e.launcher.HandleFor(review.SessionID)
	if err != nil {
		return domain.Review{}, fmt.Errorf("resolve canonical reviewer handle: %w", err)
	}
	alive, err := e.launcher.Alive(ctx, handleID)
	if err != nil {
		return domain.Review{}, fmt.Errorf("probe canonical reviewer handle %q: %w", handleID, err)
	}
	if !alive {
		return review, nil
	}
	review.ReviewerHandleID = handleID
	review.UpdatedAt = e.clock()
	if err := e.store.UpsertReview(ctx, review); err != nil {
		return domain.Review{}, fmt.Errorf("recover reviewer handle binding %q: %w", handleID, err)
	}
	return review, nil
}

func reviewLaunchSpec(worker domain.SessionRecord, harness domain.ReviewerHarness, run domain.ReviewRun, queue []ports.ReviewTask, index int) LaunchSpec {
	return LaunchSpec{
		RunID:         run.ID,
		WorkerID:      worker.ID,
		Harness:       harness,
		WorkspacePath: worker.Metadata.WorkspacePath,
		PRURL:         run.PRURL,
		TargetSHA:     run.TargetSHA,
		ReviewQueue:   queue,
		ReviewIndex:   index,
	}
}

func reviewQueue(runs []domain.ReviewRun) []ports.ReviewTask {
	queue := make([]ports.ReviewTask, 0, len(runs))
	for _, run := range runs {
		queue = append(queue, ports.ReviewTask{
			RunID:     run.ID,
			PRURL:     run.PRURL,
			TargetSHA: run.TargetSHA,
		})
	}
	return queue
}

func replaceReviewLatestRun(reviews []PRReviewState, prURL, targetSHA string, run domain.ReviewRun) []PRReviewState {
	for i := range reviews {
		if reviews[i].PRURL == prURL && reviews[i].TargetSHA == targetSHA {
			reviews[i].LatestRun = &run
			if run.Status == domain.ReviewRunRunning {
				reviews[i].Status = ReviewStateRunning
			}
			break
		}
	}
	return reviews
}

func firstReusableRun(reviews []PRReviewState) domain.ReviewRun {
	// Legacy compatibility only: in the multi-PR model the authoritative state
	// is Reviews. When no run is created, this field is just a best-effort
	// non-empty run for older clients.
	for _, review := range reviews {
		if review.LatestRun != nil {
			return *review.LatestRun
		}
	}
	return domain.ReviewRun{}
}

// List returns a worker's review state: the live reviewer handle and its passes.
func (e *Engine) List(ctx stdctx.Context, workerID domain.SessionID) (SessionReviews, error) {
	if workerID == "" {
		return SessionReviews{}, fmt.Errorf("%w: worker session id is required", ErrInvalid)
	}
	unlock := e.lockWorker(workerID)
	defer unlock()

	runs, err := e.store.ListReviewRunsBySession(ctx, workerID)
	if err != nil {
		return SessionReviews{}, err
	}
	var handle string
	if review, ok, err := e.store.GetReviewBySession(ctx, workerID); err != nil {
		return SessionReviews{}, err
	} else if ok {
		review, err = e.recoverReviewerBinding(ctx, review)
		if err != nil {
			return SessionReviews{}, err
		}
		handle = review.ReviewerHandleID
	}
	prs, err := e.prs.ListPRsBySession(ctx, workerID)
	if err != nil {
		return SessionReviews{}, err
	}
	return SessionReviews{ReviewerHandleID: handle, Runs: runs, Reviews: Plan(prs, runs)}, nil
}

// Cancel interrupts the live reviewer pane for a worker and marks running
// review runs as cancelled so they no longer block a fresh trigger.
func (e *Engine) Cancel(ctx stdctx.Context, workerID domain.SessionID) (CancelResult, error) {
	if workerID == "" {
		return CancelResult{}, fmt.Errorf("%w: worker session id is required", ErrInvalid)
	}
	unlock := e.lockWorker(workerID)
	defer unlock()

	review, ok, err := e.store.GetReviewBySession(ctx, workerID)
	if err != nil {
		return CancelResult{}, err
	}
	if !ok {
		return CancelResult{}, fmt.Errorf("%w: reviewer for worker session %q", ErrNotFound, workerID)
	}
	review, err = e.recoverReviewerBinding(ctx, review)
	if err != nil {
		return CancelResult{}, err
	}
	running, err := e.store.ListRunningReviewRunsBySession(ctx, workerID)
	if err != nil {
		return CancelResult{}, err
	}
	if review.ReviewerHandleID != "" {
		if err := e.launcher.Cancel(ctx, review.ReviewerHandleID, review.Harness); err != nil {
			alive, aliveErr := e.launcher.Alive(ctx, review.ReviewerHandleID)
			if aliveErr != nil {
				return CancelResult{}, err
			}
			if alive {
				return CancelResult{}, err
			}
		}
	}
	if _, err := e.store.CancelRunningReviewRunsBySession(ctx, workerID, "cancelled by user"); err != nil {
		return CancelResult{}, err
	}
	cancelled := make([]domain.ReviewRun, 0, len(running))
	for _, run := range running {
		run.Status = domain.ReviewRunCancelled
		run.Verdict = domain.VerdictNone
		run.Body = "cancelled by user"
		run.GithubReviewID = ""
		cancelled = append(cancelled, run)
	}
	prs, err := e.prs.ListPRsBySession(ctx, workerID)
	if err != nil {
		return CancelResult{}, err
	}
	runs, err := e.store.ListReviewRunsBySession(ctx, workerID)
	if err != nil {
		return CancelResult{}, err
	}
	return CancelResult{ReviewerHandleID: review.ReviewerHandleID, Reviews: Plan(prs, runs), CancelledRuns: cancelled}, nil
}

// reviewerHarness resolves which harness reviews the worker's PR: a configured
// reviewer wins, otherwise worker's own harness is reused when it is a
// supported reviewer, otherwise fallback to codex.
func (e *Engine) reviewerHarness(ctx stdctx.Context, worker domain.SessionRecord) (domain.ReviewerHarness, error) {
	var cfg domain.ProjectConfig
	if e.projects != nil {
		if proj, ok, err := e.projects.GetProject(ctx, string(worker.ProjectID)); err != nil {
			return "", err
		} else if ok {
			cfg = proj.Config
		}
	}
	return cfg.ResolveReviewerHarness(worker.Harness), nil
}

func (e *Engine) upsertReview(ctx stdctx.Context, worker domain.SessionRecord, harness domain.ReviewerHarness, handleID string, now time.Time) (domain.Review, error) {
	existing, ok, err := e.store.GetReviewBySession(ctx, worker.ID)
	if err != nil {
		return domain.Review{}, err
	}
	review := domain.Review{
		ID:               e.newID(),
		SessionID:        worker.ID,
		ProjectID:        worker.ProjectID,
		Harness:          harness,
		PRURL:            "",
		ReviewerHandleID: handleID,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if ok {
		// Reuse the existing row's identity and creation time; UpsertReview
		// refreshes harness/pr_url/reviewer_handle_id/updated_at.
		review.ID = existing.ID
		review.CreatedAt = existing.CreatedAt
	}
	if err := e.store.UpsertReview(ctx, review); err != nil {
		return domain.Review{}, err
	}
	return review, nil
}
