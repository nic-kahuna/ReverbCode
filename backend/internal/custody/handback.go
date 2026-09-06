package custody

import (
	"context"
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type HandbackRequest struct {
	CustodyRequest
	Message string `json:"message"`
}

// Handback does not clear a fence or revive an old attempt. It atomically
// retires it and seeds a successor while the same provider remains stopped.
// Python must freshly reserve/launch this successor before any CONT/input.
func (c *Coordinator) Handback(ctx context.Context, q HandbackRequest) (CustodyStatus, error) {
	old, ok, err := c.Store.GetAttempt(ctx, q.SessionID, q.AttemptID)
	if err == nil && (!ok || q.Schema != "ao-custody-request/v1" || old.Project != q.Project || old.Generation != q.Generation || old.RequestID != q.RequestID || q.RequestID == "" || old.ForegroundID != q.ForegroundID) {
		err = ErrConflict
	}
	if err != nil {
		return status(old), err
	}
	if old.Retired {
		next, exists, e := c.Store.CurrentAttempt(ctx, old.SessionID)
		if e != nil {
			return status(next), e
		}
		if !exists || next.PredecessorAttemptID != old.AttemptID || next.AttemptID != old.SuccessorAttemptID || next.ContinuationHash != hashBytes([]byte(q.Message)) {
			return status(next), ErrConflict
		}
		return c.continueSeed(ctx, next, q.Message)
	}
	if old.ClaimPhase != "quiesced" {
		return status(old), fmt.Errorf("%w: verify registry quiescence before handback", ErrAdmission)
	}
	if err = c.VerifyStoredCertificate(ctx, old); err != nil {
		return status(old), err
	}
	id, err := identifier()
	if err != nil {
		return status(old), err
	}
	next := Attempt{Project: old.Project, SessionID: old.SessionID, AttemptID: id, Operation: "background_turn", IssueNumber: old.IssueNumber, Phase: "seed", ClaimID: old.ClaimID, ClaimGeneration: old.ClaimGeneration, ContinuationHash: hashBytes([]byte(q.Message)), Route: old.Route, Workspace: old.Workspace, WorkspaceBranch: old.WorkspaceBranch, OriginalBaseSHA: old.OriginalBaseSHA, ProviderID: old.ProviderID, Transcript: old.Transcript, TurnID: old.TurnID, StopHookTurn: old.StopHookTurn, Members: append([]ProcessIdentity{}, old.Members...), Operations: []string{}, Blockers: []string{}, RuntimeHandleID: old.RuntimeHandleID, OriginAttemptID: old.OriginAttemptID, OriginGeneration: old.OriginGeneration, PredecessorAttemptID: old.AttemptID, Children: append([]ChildRuntime{}, old.Children...)}
	if old.ProviderID == "" {
		next.Operation = "restore"
		next.OriginAttemptID = ""
		next.OriginGeneration = 0
	}
	if old.ProviderID != "" && next.OriginAttemptID == "" {
		next.OriginAttemptID = old.AttemptID
		next.OriginGeneration = old.Generation
	}
	lane := c.Gate.lane(old.SessionID)
	lane.Lock()
	fresh, ok, err := c.Store.CurrentAttempt(ctx, old.SessionID)
	if err == nil && ok && fresh.AttemptID == old.AttemptID && fresh.Revision == old.Revision {
		next, err = c.Store.CreateSuccessor(ctx, old, next)
	} else if err == nil {
		err = ErrConflict
	}
	lane.Unlock()
	if err != nil {
		return status(old), err
	}
	return c.continueSeed(ctx, next, q.Message)
}
func (c *Coordinator) continueSeed(ctx context.Context, a Attempt, message string) (CustodyStatus, error) {
	if a.PredecessorAttemptID == "" || a.Fence != "" || a.Retired || a.ContinuationHash != hashBytes([]byte(message)) {
		return status(a), ErrConflict
	}
	if a.Phase == "running" {
		if err := c.verifyRunningIdentity(ctx, a); err != nil {
			return status(a), err
		}
		err := c.settleRunning(ctx, a)
		fresh, _, readErr := c.Store.CurrentAttempt(ctx, a.SessionID)
		if err != nil {
			return status(fresh), err
		}
		return status(fresh), readErr
	}
	if a.LaunchEffectStarted {
		if !a.LaunchEffectDelivered {
			return status(a), fmt.Errorf("%w: continuation effect outcome is uncertain; no signal or input replay", ErrUnknown)
		}
		if err := c.ObserveStarted(ctx, a.SessionID, ports.RuntimeHandle{ID: *a.RuntimeHandleID}); err != nil {
			return status(a), err
		}
		fresh, _, err := c.Store.CurrentAttempt(ctx, a.SessionID)
		return status(fresh), err
	}
	if a.Phase == "seed" {
		accepted, err := c.Authority.Transition(ctx, "native-reserve", a, nil)
		if err != nil {
			return status(a), err
		}
		route := accepted.Claim.Native.Source.Evidence.Route
		if a.Route == nil || route != *a.Route {
			return status(a), fmt.Errorf("%w: retained provider route changed", ErrAdmission)
		}
		lane := c.Gate.lane(a.SessionID)
		lane.Lock()
		fresh, exists, err := c.Store.CurrentAttempt(ctx, a.SessionID)
		if err == nil && exists && fresh.AttemptID == a.AttemptID && fresh.Phase == "seed" {
			fresh.ClaimGeneration = accepted.Claim.Native.Generation
			fresh.ClaimPhase = accepted.Claim.Native.Phase
			fresh.IssueBody = *accepted.IssueBody
			fresh.Phase = "preparing"
			a, err = c.Store.UpdateAttempt(ctx, fresh, fresh.Revision)
		} else if err == nil {
			err = ErrConflict
		}
		lane.Unlock()
		if err != nil {
			return status(a), err
		}
	}
	if a.Phase != "preparing" && a.Phase != "launching" {
		return status(a), ErrConflict
	}
	if a.ProviderID == "" {
		if c.LaunchPrepared == nil {
			return status(a), ErrUnsupported
		}
		next, e := c.LaunchPrepared(c.preparationContext(c.Gate.admittedContext(ctx, a), a.SessionID), a, message)
		return status(next), e
	}
	if a.RuntimeHandleID == nil {
		return status(a), ErrUnknown
	}
	admitted := c.Gate.admittedContext(ctx, a)
	var err error
	if a.Phase == "preparing" {
		a, admitted, err = c.CommitLaunch(ctx, a.SessionID, ports.RuntimeHandle{ID: *a.RuntimeHandleID})
		if err != nil {
			return status(a), err
		}
	}
	err = c.Gate.Effect(admitted, a.SessionID, true, func() error {
		current, ok, e := c.Store.CurrentAttempt(ctx, a.SessionID)
		if e != nil {
			return e
		}
		if !ok || current.AttemptID != a.AttemptID || current.Route == nil || a.Route == nil || *current.Route != *a.Route || current.LaunchEffectStarted {
			return ErrConflict
		}
		tree, e := c.runtimeTree(ctx, a)
		if e != nil {
			return e
		}
		if e = sameMembers(a.Members, tree, true); e != nil {
			return e
		}
		current.LaunchEffectStarted = true
		current, e = c.Store.UpdateAttempt(ctx, current, current.Revision)
		if e != nil {
			return e
		}
		for _, member := range a.Members {
			if member.Role != "pane_parent" {
				if e = SignalProcess(member, true); e != nil {
					return e
				}
			}
		}
		// A new prompt is delivered only into the retained provider, after fresh
		// admission. No Restore, new provider, route fallback or workspace replay.
		prompt := "Continue this retained conversation after explicit native handback. Preserve and inspect the current candidate, including foreground edits. Current accepted ticket source:\n\n" + a.IssueBody
		if message != "" {
			prompt += "\n\nRequested continuation:\n" + message
		}
		if e = c.Runtime.raw.SendMessage(admitted, ports.RuntimeHandle{ID: *a.RuntimeHandleID}, prompt); e != nil {
			return e
		}
		current.LaunchEffectDelivered = true
		_, e = c.Store.UpdateAttempt(ctx, current, current.Revision)
		return e
	})
	if err != nil {
		return status(a), err
	}
	if err = c.ObserveStarted(ctx, a.SessionID, ports.RuntimeHandle{ID: *a.RuntimeHandleID}); err != nil {
		return status(a), err
	}
	a, _, err = c.Store.CurrentAttempt(ctx, a.SessionID)
	return status(a), err
}

// StartTurn is used for native Manager sends. Raw terminal continuation has a
// separate generation-bound path; scheduled nudges cannot use that exception.
func (c *Coordinator) StartTurn(ctx context.Context, rec domain.SessionRecord, message string) error {
	a, ok, err := c.Store.CurrentAttempt(ctx, string(rec.ID))
	if err != nil {
		return err
	}
	if !ok {
		return ErrUnknown
	}
	if a.Fence != "" || a.Phase != "running" {
		return ErrFenced
	}
	cp, err := ReadCheckpoint(a)
	if err != nil || !cp.Complete || len(cp.Outstanding) > 0 {
		return fmt.Errorf("%w: prior turn has not drained", ErrFenced)
	}
	request, err := identifier()
	if err != nil {
		return err
	}
	q := CustodyRequest{Schema: "ao-custody-request/v1", Project: a.Project, SessionID: a.SessionID, AttemptID: a.AttemptID, Generation: a.Generation, RequestID: request, ForegroundID: "native-turn:" + request}
	if _, err = c.Request(ctx, q); err != nil {
		return err
	}
	if _, err = c.Checkpoint(ctx, q); err != nil {
		return err
	}
	if _, err = c.Verify(ctx, q); err != nil {
		return err
	}
	_, err = c.Handback(ctx, HandbackRequest{CustodyRequest: q, Message: message})
	return err
}
