package custody

import (
	"context"
	"fmt"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// drainChildren runs under the parent lane. A review result is never completion
// proof: the retained reviewer needs its own completed transcript, ceased
// callbacks and exact idle process tree before becoming a stopped child.
func (c *Coordinator) drainChildren(ctx context.Context, a Attempt) (Attempt, error) {
	changed := false
	done := map[string]bool{}
	for i := range a.Children {
		ch := a.Children[i]
		child := childAttempt(a, ch)
		if ch.Suspended {
			tree, err := c.runtimeTree(ctx, child)
			if err != nil {
				return a, err
			}
			if err = sameMembers(ch.Members, tree, true); err != nil {
				return a, err
			}
			done[ch.Operation] = true
			continue
		}
		if ch.ProviderID == "" || ch.Transcript == "" {
			continue
		}
		cp, err := ReadCheckpoint(child)
		if err != nil {
			continue
		}
		if !cp.Complete || len(cp.Outstanding) > 0 {
			if !cp.Interrupted && !cp.Complete && !ch.InterruptSent {
				ch.InterruptSent = true
				a.Children[i] = ch
				a, err = c.Store.UpdateAttempt(ctx, a, a.Revision)
				if err != nil {
					return a, err
				}
				if err = c.Runtime.raw.Interrupt(ctx, ports.RuntimeHandle{ID: ch.HandleID}); err != nil {
					return a, err
				}
			} else if (cp.Interrupted || cp.Complete) && !ch.CheckpointSent {
				ch.CheckpointSent = true
				a.Children[i] = ch
				a, err = c.Store.UpdateAttempt(ctx, a, a.Revision)
				if err != nil {
					return a, err
				}
				if err = c.Runtime.raw.SendMessage(ctx, ports.RuntimeHandle{ID: ch.HandleID}, checkpointPrompt+a.RequestID); err != nil {
					return a, err
				}
			}
			continue
		}
		callbacksDone := true
		for _, p := range ch.Callbacks {
			gone, e := ProcessGone(p)
			if e != nil {
				return a, e
			}
			if !gone {
				callbacksDone = false
			}
		}
		if !callbacksDone {
			continue
		}
		tree, err := c.runtimeTree(ctx, child)
		if err != nil {
			return a, err
		}
		if err = supportedIdleTree(tree); err != nil {
			return a, err
		}
		if err = sameProvider(ch.Members, tree); err != nil {
			return a, err
		}
		ch.Members = tree
		a.Children[i] = ch
		a, err = c.Store.UpdateAttempt(ctx, a, a.Revision)
		if err != nil {
			return a, err
		}
		for _, member := range tree {
			if member.Role != "pane_parent" {
				if err = SignalProcess(member, false); err != nil {
					return a, err
				}
			}
		}
		after, err := c.runtimeTree(ctx, childAttempt(a, ch))
		if err != nil {
			return a, err
		}
		if err = sameMembers(tree, after, true); err != nil {
			return a, err
		}
		verified, err := ReadCheckpoint(childAttempt(a, ch))
		if err != nil || !verified.Complete || len(verified.Outstanding) > 0 || verified.ContextSHA256 != cp.ContextSHA256 {
			return a, ErrUnknown
		}
		ch.Suspended = true
		ch.CompletedContextSHA256 = cp.ContextSHA256
		a.Children[i] = ch
		done[ch.Operation] = true
		changed = true
	}
	if changed {
		ops := []string{}
		for _, op := range a.Operations {
			if !done[op] {
				ops = append(ops, op)
			}
		}
		a.Operations = ops
		return c.Store.UpdateAttempt(ctx, a, a.Revision)
	}
	return a, nil
}

func (c *Coordinator) verifyChildren(ctx context.Context, a Attempt) error {
	for _, ch := range a.Children {
		if !ch.Suspended || ch.CompletedContextSHA256 == "" {
			return fmt.Errorf("%w: reviewer not suspended", ErrUnknown)
		}
		tree, err := c.runtimeTree(ctx, childAttempt(a, ch))
		if err != nil {
			return err
		}
		if err = sameMembers(ch.Members, tree, true); err != nil {
			return err
		}
	}
	return nil
}
