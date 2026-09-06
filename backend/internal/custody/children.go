package custody

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type ChildRuntime struct {
	Operation              string            `json:"operation"`
	HandleID               string            `json:"handle_id"`
	Members                []ProcessIdentity `json:"members"`
	ProviderID             string            `json:"provider_id"`
	Transcript             string            `json:"transcript"`
	TurnID                 string            `json:"turn_id"`
	StopHookTurn           string            `json:"stop_hook_turn"`
	CompletedContextSHA256 string            `json:"completed_context_sha256"`
	Callbacks              []ProcessIdentity `json:"callbacks"`
	InterruptSent          bool              `json:"interrupt_sent"`
	CheckpointSent         bool              `json:"checkpoint_sent"`
	NotifyHash             string            `json:"notify_sha256"`
	NotifyStarted          bool              `json:"notify_started"`
	NotifyDelivered        bool              `json:"notify_delivered"`
	Suspended              bool              `json:"suspended"`
	OriginAttemptID        string            `json:"origin_attempt_id"`
	OriginGeneration       int64             `json:"origin_generation"`
}

// BeginChild grants a narrow typed permit within existing parent authority.
// It records prelaunch debt before any adapter-side effects, including failure.
func (c *Coordinator) BeginChild(ctx context.Context, id, operation string) (context.Context, error) {
	lane := c.Gate.lane(id)
	lane.Lock()
	defer lane.Unlock()
	a, managed, err := c.Gate.Inspect(ctx, id)
	if err != nil {
		return ctx, err
	}
	if !managed {
		return ctx, nil
	}
	if a.Fence != "" || a.Phase != "running" || a.Retired || a.ClaimGeneration < 1 {
		return ctx, ErrFenced
	}
	for i := range a.Children {
		ch := a.Children[i]
		if ch.Operation == operation {
			continue
		}
		if !ch.Suspended {
			cp, e := ReadCheckpoint(childAttempt(a, ch))
			if e != nil || !cp.Complete || len(cp.Outstanding) > 0 {
				return ctx, ErrFenced
			}
		}
		ops := []string{}
		for _, op := range a.Operations {
			if op != ch.Operation {
				ops = append(ops, op)
			}
		}
		a.Operations = ops
		ch.Operation = operation
		ch.NotifyHash = ""
		ch.NotifyStarted = false
		ch.NotifyDelivered = false
		ch.InterruptSent = false
		ch.CheckpointSent = false
		a.Children[i] = ch
	}
	found := false
	for _, op := range a.Operations {
		if op == operation {
			found = true
		}
	}
	if !found {
		a.Operations = append(a.Operations, operation)
		a, err = c.Store.UpdateAttempt(ctx, a, a.Revision)
		if err != nil {
			return ctx, err
		}
	}
	return context.WithValue(c.preparationContext(c.Gate.admittedContext(ctx, a), a.SessionID), childKey{}, operation), nil
}
func (c *Coordinator) BindChild(ctx context.Context, id, operation, handle string) error {
	_, managed, err := c.Gate.Inspect(ctx, id)
	if err != nil {
		return err
	}
	if !managed {
		return nil
	}
	return c.Gate.Effect(ctx, id, true, func() error {
		a, ok, err := c.Store.CurrentAttempt(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return ErrUnknown
		}
		childAttempt := a
		childAttempt.RuntimeHandleID = &handle
		tree, err := c.runtimeTree(ctx, childAttempt)
		if err != nil {
			return err
		}
		found := false
		for i := range a.Children {
			if a.Children[i].HandleID == handle {
				if a.Children[i].Operation != operation {
					return ErrConflict
				}
				if len(a.Children[i].Members) > 0 {
					if e := sameProvider(a.Children[i].Members, tree); e != nil {
						return e
					}
				}
				a.Children[i].Members = tree
				found = true
			}
		}
		if !found {
			return ErrUnknown
		}
		_, err = c.Store.UpdateAttempt(ctx, a, a.Revision)
		return err
	})
}

// ChildPermit is exposed only to the in-process reviewer launcher; no request
// JSON can supply or select a force permit.
func (r *RuntimeFacade) ChildPermit(ctx context.Context, id, operation string) (context.Context, error) {
	if r.coordinator == nil {
		return ctx, ErrUnknown
	}
	return r.coordinator.BeginChild(ctx, id, operation)
}
func (r *RuntimeFacade) BindChild(ctx context.Context, id, operation, handle string) error {
	if r.coordinator == nil {
		return ErrUnknown
	}
	return r.coordinator.BindChild(ctx, id, operation, handle)
}

type childKey struct{}

func childAttempt(a Attempt, ch ChildRuntime) Attempt {
	a.RuntimeHandleID = &ch.HandleID
	a.Members = ch.Members
	a.ProviderID = ch.ProviderID
	a.Transcript = ch.Transcript
	a.TurnID = ch.TurnID
	a.StopHookTurn = ch.StopHookTurn
	a.Callbacks = ch.Callbacks
	return a
}

// sendChild serializes fresh reviewer input with the parent fence and records
// its intent before CONT or text. Same-pass retries never duplicate delivery.
func (c *Coordinator) sendChild(ctx context.Context, handle, message string) error {
	return c.Gate.Effect(ctx, handle, true, func() error {
		a, ok, err := c.Store.CurrentAttempt(ctx, WorkerID(handle))
		if err != nil {
			return err
		}
		if !ok {
			return ErrUnknown
		}
		operation, _ := ctx.Value(childKey{}).(string)
		index := -1
		for i, ch := range a.Children {
			if ch.HandleID == handle && ch.Operation == operation {
				index = i
				break
			}
		}
		if index < 0 {
			return ErrConflict
		}
		ch := a.Children[index]
		sum := hashBytes([]byte(message))
		if ch.NotifyStarted {
			if ch.NotifyHash != sum {
				return ErrConflict
			}
			if ch.NotifyDelivered {
				return nil
			}
			return ErrUnknown
		}
		tree, err := c.runtimeTree(ctx, childAttempt(a, ch))
		if err != nil {
			return err
		}
		if ch.Suspended {
			if err = sameMembers(ch.Members, tree, true); err != nil {
				return err
			}
		} else if err = sameProvider(ch.Members, tree); err != nil {
			return err
		}
		ch.NotifyStarted = true
		ch.NotifyHash = sum
		wasSuspended := ch.Suspended
		ch.Suspended = false
		a.Children[index] = ch
		a, err = c.Store.UpdateAttempt(ctx, a, a.Revision)
		if err != nil {
			return err
		}
		if wasSuspended {
			for _, member := range ch.Members {
				if member.Role != "pane_parent" {
					if err = SignalProcess(member, true); err != nil {
						return err
					}
				}
			}
		}
		if err = c.Runtime.raw.SendMessage(ctx, ports.RuntimeHandle{ID: handle}, message); err != nil {
			return err
		}
		ch.NotifyDelivered = true
		a.Children[index] = ch
		_, err = c.Store.UpdateAttempt(ctx, a, a.Revision)
		return err
	})
}
