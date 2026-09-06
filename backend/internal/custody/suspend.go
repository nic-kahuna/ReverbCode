package custody

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type CustodyRequest struct {
	Schema       string `json:"schema"`
	Project      string `json:"project"`
	SessionID    string `json:"session_id"`
	AttemptID    string `json:"attempt_id"`
	Generation   int64  `json:"generation"`
	RequestID    string `json:"request_id"`
	ForegroundID string `json:"foreground_id"`
}
type CustodyStatus struct {
	Schema       string       `json:"schema"`
	Project      string       `json:"project"`
	SessionID    string       `json:"session_id"`
	AttemptID    string       `json:"attempt_id"`
	Generation   int64        `json:"generation"`
	RequestID    string       `json:"request_id"`
	ForegroundID string       `json:"foreground_id"`
	Phase        string       `json:"phase"`
	Fence        string       `json:"fence"`
	Ready        bool         `json:"ready"`
	Blockers     []string     `json:"blockers"`
	Certificate  *Certificate `json:"certificate"`
}

func status(a Attempt) CustodyStatus {
	return CustodyStatus{Schema: "ao-custody-status/v1", Project: a.Project, SessionID: a.SessionID, AttemptID: a.AttemptID, Generation: a.Generation, RequestID: a.RequestID, ForegroundID: a.ForegroundID, Phase: a.Phase, Fence: a.Fence, Ready: false, Blockers: append([]string{}, a.Blockers...), Certificate: a.Certificate}
}
func (c *Coordinator) requested(ctx context.Context, q CustodyRequest) (Attempt, error) {
	a, ok, err := c.Store.GetAttempt(ctx, q.SessionID, q.AttemptID)
	if err != nil {
		return a, err
	}
	if q.Schema != "ao-custody-request/v1" || !ok || a.Project != q.Project || a.Generation != q.Generation || a.RequestID != q.RequestID || q.RequestID == "" || a.ForegroundID != q.ForegroundID || a.Retired {
		return a, ErrConflict
	}
	return a, nil
}
func (c *Coordinator) Request(ctx context.Context, q CustodyRequest) (CustodyStatus, error) {
	if q.Schema != "ao-custody-request/v1" || q.ForegroundID == "" {
		return CustodyStatus{}, ErrUnknown
	}
	a, ok, err := c.Store.CurrentAttempt(ctx, q.SessionID)
	if err != nil {
		return status(a), err
	}
	if !ok || a.Project != q.Project {
		return status(a), ErrConflict
	}
	a, err = c.Gate.Request(ctx, q.SessionID, q.AttemptID, q.RequestID, q.Generation, q.ForegroundID)
	return status(a), err
}
func (c *Coordinator) Status(ctx context.Context, q CustodyRequest) (CustodyStatus, error) {
	a, err := c.requested(ctx, q)
	if err != nil {
		return status(a), err
	}
	out := status(a)
	if a.Phase == "quiesced" {
		if err = c.VerifyStoredCertificate(ctx, a); err != nil {
			out.Blockers = append(out.Blockers, err.Error())
		} else {
			out.Ready = true
		}
	}
	return out, nil
}
func (c *Coordinator) pending(ctx context.Context, a Attempt, reason string) (CustodyStatus, error) {
	lane := c.Gate.lane(a.SessionID)
	lane.Lock()
	defer lane.Unlock()
	fresh, ok, err := c.Store.CurrentAttempt(ctx, a.SessionID)
	if err != nil {
		return status(a), err
	}
	if !ok || fresh.AttemptID != a.AttemptID {
		return status(a), ErrConflict
	}
	fresh.Blockers = []string{reason}
	fresh, err = c.Store.UpdateAttempt(ctx, fresh, fresh.Revision)
	return status(fresh), err
}

const checkpointPrompt = "Coordinator preserving foreground takeover: stop this task and checkpoint the current conversation. Promptly stop and drain all exact tool operations and child writers you started, retaining partial work. Verify original command completion and settle outstanding tools; an interrupted poll or Unknown process id is not proof. Use only scoped bookkeeping and verified own-process cancellation where safe. Do not start new background work, restart tools, reset, stash, clean, recreate, commit, or delete progress. Do not claim a remote/shared operation drained without actual evidence. If any operation cannot be safely verified, report BLOCKED and preserve it. End the turn with a concise factual handoff describing partial progress and unresolved operations. Native AO will independently verify the completed turn and exact process identities before transferring custody. Request: "

// Checkpoint makes at most one interrupt and one fixed coordinator submission.
// Intents are durable before I/O; an ambiguous delivery is never replayed.
// Bounded callers can retry observation while the same request remains pending.
func (c *Coordinator) Checkpoint(ctx context.Context, q CustodyRequest) (CustodyStatus, error) {
	a, err := c.requested(ctx, q)
	if err != nil {
		return status(a), err
	}
	if a.Phase == "quiesced" {
		return c.Status(ctx, q)
	}
	if a.Phase == "launching" && a.LaunchEffectDelivered && a.RuntimeHandleID != nil {
		if err = c.ObserveStarted(ctx, a.SessionID, ports.RuntimeHandle{ID: *a.RuntimeHandleID}); err != nil {
			return status(a), err
		}
		a, err = c.requested(ctx, q)
		if err != nil {
			return status(a), err
		}
	}
	if a.Phase == "preparing" && a.RuntimeHandleID == nil && a.ProviderID == "" && len(a.Operations) == 0 {
		return c.suspendPreparing(ctx, a)
	}
	if a.Phase != "running" || a.RuntimeHandleID == nil || a.Route == nil || a.Route.Harness != "codex" {
		return c.pending(ctx, a, "unsupported or incomplete native launch; candidate retained")
	}
	lane := c.Gate.lane(a.SessionID)
	lane.Lock()
	a, err = c.drainChildren(ctx, a)
	lane.Unlock()
	if err != nil {
		return status(a), err
	}
	if len(a.Operations) > 0 {
		return c.pending(ctx, a, "native preparation/reviewer operation still outstanding")
	}
	cp, probeErr := ReadCheckpoint(a)
	if probeErr == nil && cp.Complete && len(cp.Outstanding) == 0 {
		return c.suspend(ctx, a, cp)
	}
	if !a.InterruptSent && !cp.Interrupted && !cp.Complete {
		lane := c.Gate.lane(a.SessionID)
		lane.Lock()
		fresh, ok, e := c.Store.CurrentAttempt(ctx, a.SessionID)
		if e == nil && ok && fresh.AttemptID == a.AttemptID && !fresh.InterruptSent && fresh.Fence != "" {
			fresh.InterruptSent = true
			fresh.Fence = "draining"
			fresh, e = c.Store.UpdateAttempt(ctx, fresh, fresh.Revision)
			if e == nil {
				e = c.Runtime.raw.Interrupt(ctx, ports.RuntimeHandle{ID: *fresh.RuntimeHandleID})
			}
			a = fresh
		} else if e == nil {
			e = ErrConflict
		}
		lane.Unlock()
		if e != nil {
			return c.pending(ctx, a, "interrupt outcome requires observation: "+e.Error())
		}
	}
	// Wait only for a real completed/aborted boundary before submitting the
	// checkpoint. Never paste into an unclassified active provider state.
	deadline := time.Now().Add(3 * time.Second)
	for !cp.Complete && !cp.Interrupted && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return status(a), ctx.Err()
		case <-time.After(75 * time.Millisecond):
		}
		cp, probeErr = ReadCheckpoint(a)
	}
	if !cp.Complete && !cp.Interrupted {
		return c.pending(ctx, a, "provider has not acknowledged a terminal turn boundary")
	}
	if !a.CheckpointSent {
		lane := c.Gate.lane(a.SessionID)
		lane.Lock()
		fresh, ok, e := c.Store.CurrentAttempt(ctx, a.SessionID)
		if e == nil && ok && fresh.AttemptID == a.AttemptID && !fresh.CheckpointSent && fresh.Fence != "" {
			fresh.CheckpointSent = true
			fresh.Fence = "draining"
			fresh, e = c.Store.UpdateAttempt(ctx, fresh, fresh.Revision)
			if e == nil {
				e = c.Runtime.raw.SendMessage(ctx, ports.RuntimeHandle{ID: *fresh.RuntimeHandleID}, checkpointPrompt+fresh.RequestID)
			}
			a = fresh
		} else if e == nil {
			e = ErrConflict
		}
		lane.Unlock()
		if e != nil {
			return c.pending(ctx, a, "checkpoint delivery requires observation: "+e.Error())
		}
	}
	// No lock is held during provider work; a later invocation observes progress.
	reason := "checkpoint turn, original tools and callbacks are still draining"
	if probeErr != nil {
		reason += "; " + probeErr.Error()
	}
	return c.pending(ctx, a, reason)
}

func (c *Coordinator) suspend(ctx context.Context, a Attempt, cp Checkpoint) (CustodyStatus, error) {
	lane := c.Gate.lane(a.SessionID)
	lane.Lock()
	defer lane.Unlock()
	fresh, ok, err := c.Store.CurrentAttempt(ctx, a.SessionID)
	if err != nil {
		return status(a), err
	}
	if !ok || fresh.AttemptID != a.AttemptID || fresh.Fence == "" || fresh.Retired || fresh.TurnID != cp.TurnID || len(fresh.Operations) > 0 {
		return status(fresh), ErrConflict
	}
	a = fresh
	if err = c.verifyChildren(ctx, a); err != nil {
		return status(a), err
	}
	for _, callback := range a.Callbacks {
		gone, e := ProcessGone(callback)
		if e != nil {
			return status(a), e
		}
		if !gone {
			return status(a), fmt.Errorf("%w: callback has not exited", ErrUnknown)
		}
	}
	tree, err := c.runtimeTree(ctx, a)
	if err != nil {
		return status(a), err
	}
	if err = supportedIdleTree(tree); err != nil {
		return status(a), err
	}
	if err = sameProvider(a.Members, tree); err != nil {
		return status(a), err
	}

	// Persist the exact selected stop set before the first OS effect. A partial
	// stop survives a crash as pending custody, never an active foreground grant.
	a.Members = tree
	a.Fence = "draining"
	a.Blockers = []string{"exact process suspension in progress"}
	a, err = c.Store.UpdateAttempt(ctx, a, a.Revision)
	if err != nil {
		return status(a), err
	}
	for _, member := range tree {
		if member.Role != "pane_parent" {
			if err = SignalProcess(member, false); err != nil {
				return status(a), err
			}
		}
	}
	// Once the provider and its code host are stopped, re-observe the complete
	// topology and transcript. Any newly appearing member invalidates this proof.
	after, err := c.runtimeTree(ctx, a)
	if err != nil {
		return status(a), err
	}
	if err = sameMembers(tree, after, true); err != nil {
		return status(a), err
	}
	verified, err := ReadCheckpoint(a)
	if err != nil || !verified.Complete || verified.ContextSHA256 != cp.ContextSHA256 || len(verified.Outstanding) > 0 {
		return status(a), ErrUnknown
	}
	project, ok, err := c.Store.GetProject(ctx, a.Project)
	if err != nil || !ok {
		return status(a), ErrUnknown
	}
	candidate, err := CaptureCandidate(ctx, a, project.Config.TrackerIntake.Repo)
	if err != nil {
		return status(a), err
	}
	route, err := json.Marshal(a.Route)
	if err != nil {
		return status(a), err
	}
	payload := PreservationPayload{ExecutionOrigin: "retained_provider", Schema: "ao-custody-preservation/v1", Project: a.Project, SessionID: a.SessionID, AttemptID: a.AttemptID, Generation: a.Generation, RequestID: a.RequestID, ForegroundID: a.ForegroundID, Candidate: candidate, ProviderID: a.ProviderID, Route: route, Transcript: a.Transcript, TurnID: a.TurnID, CompletedContextSHA256: cp.ContextSHA256, Members: tree, Children: childPreservations(a)}
	cert, err := c.writeCertificate(a, payload)
	if err != nil {
		return status(a), err
	}
	a.Certificate = &cert
	a.Phase = "quiesced"
	a.Fence = "suspended"
	a.Blockers = []string{}
	a, err = c.Store.UpdateAttempt(ctx, a, a.Revision)
	if err != nil {
		return status(a), err
	}
	// Registry I/O must not run while holding this lane: the authority queries
	// evidence. It does not acquire the lane, but process verification may wait.
	// Result settlement is a separate idempotent Verify command below.
	out := status(a)
	out.Ready = true
	return out, nil
}
func (c *Coordinator) Verify(ctx context.Context, q CustodyRequest) (CustodyStatus, error) {
	a, err := c.requested(ctx, q)
	if err != nil {
		return status(a), err
	}
	if err = c.VerifyStoredCertificate(ctx, a); err != nil {
		return status(a), err
	}
	a, err = c.reconcileQuiesced(ctx, a)
	if err != nil {
		return status(a), err
	}
	if a.ClaimPhase == "quiesced" {
		return c.Status(ctx, q)
	}
	result := "quiesced"
	accepted, err := c.Authority.Transition(ctx, "native-result", a, &result)
	if err != nil {
		return status(a), err
	}
	lane := c.Gate.lane(a.SessionID)
	lane.Lock()
	defer lane.Unlock()
	fresh, ok, err := c.Store.CurrentAttempt(ctx, a.SessionID)
	if err != nil {
		return status(a), err
	}
	if !ok || fresh.AttemptID != a.AttemptID || fresh.Retired {
		return status(a), ErrConflict
	}
	fresh.ClaimGeneration = accepted.Claim.Native.Generation
	fresh.ClaimPhase = accepted.Claim.Native.Phase
	fresh, err = c.Store.UpdateAttempt(ctx, fresh, fresh.Revision)
	out := status(fresh)
	out.Ready = err == nil
	return out, err
}

func (c *Coordinator) suspendPreparing(ctx context.Context, a Attempt) (CustodyStatus, error) {
	lane := c.Gate.lane(a.SessionID)
	lane.Lock()
	defer lane.Unlock()
	fresh, ok, err := c.Store.CurrentAttempt(ctx, a.SessionID)
	if err != nil {
		return status(a), err
	}
	if !ok || fresh.AttemptID != a.AttemptID || fresh.Phase != "preparing" || fresh.Fence == "" || fresh.RuntimeHandleID != nil || fresh.ProviderID != "" || len(fresh.Operations) != 0 {
		return status(fresh), ErrConflict
	}
	a = fresh
	for _, p := range a.Preparations {
		gone, e := ProcessGone(p)
		if e != nil {
			return status(a), e
		}
		if !gone {
			return status(a), ErrUnknown
		}
	}
	project, ok, err := c.Store.GetProject(ctx, a.Project)
	if err != nil || !ok {
		return status(a), ErrUnknown
	}
	candidate, err := CaptureCandidate(ctx, a, project.Config.TrackerIntake.Repo)
	if err != nil {
		return status(a), err
	}
	route, err := json.Marshal(a.Route)
	if err != nil {
		return status(a), err
	}
	payload := PreservationPayload{Schema: "ao-custody-preservation/v1", ExecutionOrigin: "preparing_without_runtime", Project: a.Project, SessionID: a.SessionID, AttemptID: a.AttemptID, Generation: a.Generation, RequestID: a.RequestID, ForegroundID: a.ForegroundID, Candidate: candidate, Route: route, Members: []ProcessIdentity{}, Children: []ChildPreservation{}}
	cert, err := c.writeCertificate(a, payload)
	if err != nil {
		return status(a), err
	}
	a.Phase = "quiesced"
	a.Fence = "suspended"
	a.Certificate = &cert
	a.Blockers = []string{}
	a, err = c.Store.UpdateAttempt(ctx, a, a.Revision)
	out := status(a)
	out.Ready = err == nil
	return out, err
}
