package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// ErrWorkerRetirementMismatch refuses a changed or contradictory retirement target.
var ErrWorkerRetirementMismatch = errors.New("session: worker retirement identity or state differs")

// WorkerRetirementScope states the exact limit of native retirement evidence.
const WorkerRetirementScope = "named managed runtime only; detached or external jobs are not verified"

// WorkerRetirementExpectation is mandatory for both explicit retirement and
// current verification. It never supplies historical launch evidence.
type WorkerRetirementExpectation struct {
	ProjectID domain.ProjectID
	Ticket    domain.IssueID
	UpdatedAt time.Time
}

// WorkerRetirementVerification describes a current read-only runtime observation.
type WorkerRetirementVerification struct {
	Verified           bool      `json:"verified"`
	ObservedAt         time.Time `json:"observedAt"`
	RuntimeTermination string    `json:"runtimeTermination"`
	Scope              string    `json:"scope"`
}

// WorkerRetirementStatus is an observation of a held, version-bound retirement.
// Verified speaks only for the managed runtime, not custody or retry permission.
type WorkerRetirementStatus struct {
	Hold         domain.WorkerSchedulingHold
	Held         bool
	Verification WorkerRetirementVerification
}

var retirementTicketPattern = regexp.MustCompile(`^github:([a-z0-9_.-]+/[a-z0-9_.-]+)#([1-9]\d*)$`)

// RetireWorkerForRetry fences one already-terminal worker and records current
// cessation without changing the session or touching its workspace or refs.
func (m *Manager) RetireWorkerForRetry(ctx context.Context, id domain.SessionID, expected WorkerRetirementExpectation) (StopWorkerRetainedResult, error) {
	result := StopWorkerRetainedResult{SessionID: id, WorktreeRetained: true, ReconciliationRequired: true, RuntimeTermination: RuntimeTerminationUnknown}
	opCtx, unlock, err := m.admission.Enter(ctx, expected.ProjectID)
	if err != nil {
		return result, err
	}
	defer unlock()
	ctx = opCtx
	rec, err := m.retirementTarget(ctx, id, expected)
	if err != nil {
		return result, err
	}
	result.Hold, err = m.HoldWorker(ctx, id)
	if err != nil {
		return result, err
	}
	if prior := result.Hold.Retirement; prior != nil && (!retirementMatches(*prior, expected) || prior.RetiredAt.Before(result.Hold.HeldAt) || prior.RetiredAt.Before(rec.UpdatedAt) || prior.RetiredAt.After(m.clock())) {
		return result, ErrWorkerRetirementMismatch
	}
	handle, supported, err := m.retirementHandle(ctx, rec)
	if err != nil {
		return result, err
	}
	if !supported {
		result.RuntimeTermination = RuntimeTerminationUnsupported
		return result, nil
	}
	// Probe before acting. An unknown probe never authorizes a stop; an absent
	// canonical handle needs no destructive operation at all.
	alive, err := m.runtime.IsAlive(ctx, handle)
	if err != nil {
		return result, nil
	}
	if alive {
		if result.Hold.Retirement != nil {
			// A revived runtime invalidates the prior fact. Do not silently
			// refresh its immutable time or turn replay into another retirement.
			return result, ErrWorkerRetirementMismatch
		}
		if err := m.runtime.Destroy(ctx, handle); err != nil {
			return result, nil
		}
		alive, err = m.runtime.IsAlive(ctx, handle)
		if err != nil || alive {
			return result, nil
		}
	}
	current, err := m.retirementTarget(ctx, id, expected)
	if err != nil {
		return result, err
	}
	if !reflect.DeepEqual(current, rec) {
		return result, ErrWorkerRetirementMismatch
	}
	if result.Hold.Retirement == nil {
		fact := domain.WorkerRetirement{ProjectID: expected.ProjectID, Ticket: expected.Ticket, SessionUpdatedAt: rec.UpdatedAt, RetiredAt: m.clock().UTC()}
		if fact.RetiredAt.Before(rec.UpdatedAt) || fact.RetiredAt.Before(result.Hold.HeldAt) {
			return result, ErrWorkerRetirementMismatch
		}
		result.Hold, err = m.store.SetWorkerRetirement(ctx, rec, fact)
		if err != nil {
			return result, fmt.Errorf("retire worker %s: %w", id, err)
		}
	}
	result.RuntimeTermination = RuntimeTerminationStopped
	return result, nil
}

// VerifyWorkerRetirement performs a read-only, current runtime absence probe.
// It deliberately shares the existing project lane with restore and hold.
func (m *Manager) VerifyWorkerRetirement(ctx context.Context, id domain.SessionID, expected WorkerRetirementExpectation) (WorkerRetirementStatus, error) {
	result := WorkerRetirementStatus{Verification: WorkerRetirementVerification{RuntimeTermination: RuntimeTerminationUnknown, Scope: WorkerRetirementScope}}
	opCtx, unlock, err := m.admission.Enter(ctx, expected.ProjectID)
	if err != nil {
		return result, err
	}
	defer unlock()
	ctx = opCtx
	rec, err := m.retirementTarget(ctx, id, expected)
	if err != nil {
		return result, err
	}
	result.Hold, result.Held, err = m.WorkerHold(ctx, id)
	if err != nil {
		return result, err
	}
	result.Verification.ObservedAt = m.clock().UTC()
	if !result.Held || result.Hold.Retirement == nil {
		return result, nil
	}
	fact := *result.Hold.Retirement
	if !retirementMatches(fact, expected) || fact.RetiredAt.Before(result.Hold.HeldAt) || fact.RetiredAt.Before(rec.UpdatedAt) || fact.RetiredAt.After(result.Verification.ObservedAt) {
		return result, ErrWorkerRetirementMismatch
	}
	handle, supported, err := m.retirementHandle(ctx, rec)
	if err != nil {
		return result, err
	}
	if !supported {
		result.Verification.RuntimeTermination = RuntimeTerminationUnsupported
		return result, nil
	}
	alive, err := m.runtime.IsAlive(ctx, handle)
	result.Verification.ObservedAt = m.clock().UTC()
	if err != nil || alive {
		return result, nil
	}
	current, err := m.retirementTarget(ctx, id, expected)
	if err != nil {
		return result, err
	}
	hold, held, err := m.WorkerHold(ctx, id)
	if err != nil {
		return result, err
	}
	if !reflect.DeepEqual(rec, current) || !held || !reflect.DeepEqual(hold, result.Hold) {
		return result, ErrWorkerRetirementMismatch
	}
	result.Verification.ObservedAt = m.clock().UTC()
	result.Verification.RuntimeTermination = RuntimeTerminationStopped
	result.Verification.Verified = true
	return result, nil
}

func retirementMatches(fact domain.WorkerRetirement, expected WorkerRetirementExpectation) bool {
	return fact.ProjectID == expected.ProjectID && fact.Ticket == expected.Ticket && fact.SessionUpdatedAt.Equal(expected.UpdatedAt) && !fact.RetiredAt.IsZero()
}

func (m *Manager) retirementTarget(ctx context.Context, id domain.SessionID, expected WorkerRetirementExpectation) (domain.SessionRecord, error) {
	match := retirementTicketPattern.FindStringSubmatch(string(expected.Ticket))
	if expected.ProjectID == "" || strings.TrimSpace(string(expected.ProjectID)) != string(expected.ProjectID) || len(match) != 3 || expected.UpdatedAt.IsZero() {
		return domain.SessionRecord{}, ErrWorkerRetirementMismatch
	}
	rec, ok, err := m.store.GetSession(ctx, id)
	if err != nil {
		return rec, err
	}
	if !ok {
		return rec, ErrNotFound
	}
	if rec.ProjectID != expected.ProjectID || rec.Kind != domain.KindWorker || !rec.IsTerminated || rec.Activity.State != domain.ActivityExited || !rec.UpdatedAt.Equal(expected.UpdatedAt) || rec.UpdatedAt.After(m.clock()) {
		return rec, ErrWorkerRetirementMismatch
	}
	if rec.CreatedAt.IsZero() || rec.CreatedAt.After(rec.Activity.LastActivityAt) || rec.Activity.LastActivityAt.After(rec.UpdatedAt) {
		return rec, ErrWorkerRetirementMismatch
	}
	project, err := m.loadProject(ctx, rec.ProjectID)
	if err != nil {
		return rec, err
	}
	if project.ID != string(expected.ProjectID) || project.ConfigDecodeError != "" || project.Kind.WithDefault() != domain.ProjectKindSingleRepo || !retirementRepositoryMatches(project.RepoOriginURL, match[1]) {
		return rec, ErrWorkerRetirementMismatch
	}
	// Numeric legacy identities are canonical only within this exact registered
	// repository. Preserve the raw issue field; never rewrite historical rows.
	if string(rec.IssueID) != string(expected.Ticket) && string(rec.IssueID) != match[2] && string(rec.IssueID) != "https://github.com/"+match[1]+"/issues/"+match[2] {
		return rec, ErrWorkerRetirementMismatch
	}
	prs, err := m.store.ListPRFactsForSession(ctx, id)
	if err != nil {
		return rec, err
	}
	for _, pr := range prs {
		if pr.Merged || !pr.Closed || pr.ReviewComments {
			return rec, ErrWorkerRetirementMismatch
		}
	}
	return rec, nil
}

func retirementRepositoryMatches(raw, expected string) bool {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return false
	}
	var slug string
	if strings.HasPrefix(raw, "git@github.com:") {
		slug = strings.TrimPrefix(raw, "git@github.com:")
	} else {
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "https" && u.Scheme != "ssh") || u.Host != "github.com" || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" {
			return false
		}
		slug = strings.TrimPrefix(u.Path, "/")
	}
	return strings.ToLower(strings.TrimSuffix(slug, ".git")) == expected
}

func (m *Manager) retirementHandle(ctx context.Context, rec domain.SessionRecord) (ports.RuntimeHandle, bool, error) {
	verifier, ok := m.runtime.(retainedStopVerifier)
	if !ok || !verifier.SupportsVerifiedRetainedStop() {
		return ports.RuntimeHandle{}, false, nil
	}
	handle, err := m.runtime.HandleFor(ports.RuntimeConfig{SessionID: rec.ID})
	if err != nil || handle.ID == "" {
		return ports.RuntimeHandle{}, true, ErrWorkerRetirementMismatch
	}
	if rec.Metadata.RuntimeHandleID != "" && rec.Metadata.RuntimeHandleID != handle.ID {
		return ports.RuntimeHandle{}, true, ErrWorkerRetirementMismatch
	}
	all, err := m.store.ListAllSessions(ctx)
	if err != nil {
		return ports.RuntimeHandle{}, true, err
	}
	for _, other := range all {
		if other.ID == rec.ID {
			continue
		}
		canonical, err := m.runtime.HandleFor(ports.RuntimeConfig{SessionID: other.ID})
		if err != nil || canonical.ID == "" {
			return ports.RuntimeHandle{}, true, ErrWorkerRetirementMismatch
		}
		if other.Metadata.RuntimeHandleID == handle.ID || canonical.ID == handle.ID {
			return ports.RuntimeHandle{}, true, ErrWorkerRetirementMismatch
		}
	}
	return handle, true, nil
}
