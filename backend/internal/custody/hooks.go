package custody

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type HookRequest struct {
	Project          string `json:"project"`
	SessionID        string `json:"session_id"`
	LaunchAttemptID  string `json:"launch_attempt_id"`
	LaunchGeneration int64  `json:"launch_generation"`
	PID              int    `json:"pid"`
	Event            string `json:"event"`
	State            string `json:"state"`
	ProviderID       string `json:"provider_id"`
	Transcript       string `json:"transcript"`
	Worktree         string `json:"worktree"`
	TurnID           string `json:"turn_id"`
}
type HookResult struct {
	OK bool `json:"ok"`
}
type activityKey struct{}

func ActivityAuthorized(ctx context.Context, id domain.SessionID) bool {
	v, _ := ctx.Value(activityKey{}).(string)
	return v == string(id)
}

// Hook accepts native callbacks only from a live descendant of the exact
// retained Codex provider. A reused PID, old launch, changed provider or old
// turn may not overwrite the current generation. The callback itself remains
// a tracked operation until the OS positively observes its cessation.
func (c *Coordinator) Hook(ctx context.Context, q HookRequest) (HookResult, error) {
	lane := c.Gate.lane(WorkerID(q.SessionID))
	lane.Lock()
	defer lane.Unlock()
	a, managed, err := c.Gate.Inspect(ctx, q.SessionID)
	if err != nil {
		return HookResult{}, err
	}
	if !managed || a.Project != q.Project || a.Retired || a.Phase == "quiesced" {
		return HookResult{}, ErrFenced
	}
	parent := a
	childIndex := -1
	if q.SessionID != a.SessionID {
		for i := range a.Children {
			if a.Children[i].HandleID == q.SessionID {
				childIndex = i
				break
			}
		}
		if childIndex < 0 || a.Children[childIndex].Suspended {
			return HookResult{}, ErrFenced
		}
		ch := a.Children[childIndex]
		a = childAttempt(a, ch)
		a.OriginAttemptID = ch.OriginAttemptID
		a.OriginGeneration = ch.OriginGeneration
		a.InterruptSent = ch.InterruptSent
		a.CheckpointSent = ch.CheckpointSent
	}
	state := domain.ActivityState(q.State)
	switch state {
	case domain.ActivityActive, domain.ActivityIdle, domain.ActivityWaitingInput, domain.ActivityBlocked:
	default:
		return HookResult{}, ErrUnknown
	}
	originID, originGeneration := a.OriginAttemptID, a.OriginGeneration
	if originID == "" {
		originID = a.AttemptID
		originGeneration = a.Generation
	}
	if originID != q.LaunchAttemptID || originGeneration != q.LaunchGeneration {
		return HookResult{}, ErrConflict
	}
	if q.ProviderID == "" || q.Transcript == "" || !filepath.IsAbs(q.Transcript) || q.Worktree != a.Workspace {
		return HookResult{}, ErrUnknown
	}
	if a.ProviderID != "" && (q.ProviderID != a.ProviderID || q.Transcript != a.Transcript) {
		return HookResult{}, ErrConflict
	}
	tree, err := c.runtimeTree(ctx, a)
	if err != nil {
		return HookResult{}, err
	}
	hook, err := ObserveProcess(q.PID)
	if err != nil {
		return HookResult{}, err
	}
	var provider *ProcessIdentity
	for i := range tree {
		if filepath.Base(tree[i].Executable) == "codex" {
			if provider != nil {
				return HookResult{}, ErrUnknown
			}
			provider = &tree[i]
		}
	}
	if provider == nil {
		return HookResult{}, ErrUnknown
	}
	ancestor := hook
	bound := false
	for i := 0; i < 8; i++ {
		if ancestor.ParentPID == provider.PID {
			bound = true
			break
		}
		ancestor, err = ObserveProcess(ancestor.ParentPID)
		if err != nil {
			return HookResult{}, err
		}
	}
	if !bound {
		return HookResult{}, ErrConflict
	}
	for _, old := range a.Members {
		if filepath.Base(old.Executable) == "codex" && !old.Same(*provider) {
			return HookResult{}, ErrConflict
		}
	}
	callbacks := []ProcessIdentity{}
	for _, old := range a.Callbacks {
		gone, e := ProcessGone(old)
		if e != nil {
			return HookResult{}, e
		}
		if !gone {
			callbacks = append(callbacks, old)
		}
	}
	hook.Role = "callback"
	callbacks = append(callbacks, hook)
	if len(callbacks) > 32 {
		return HookResult{}, fmt.Errorf("%w: callbacks not draining", ErrUnknown)
	}
	switch q.Event {
	case "session-start":
	case "user-prompt-submit":
		if q.TurnID == "" {
			return HookResult{}, ErrUnknown
		}
		if a.Fence != "" && !a.CheckpointSent {
			return HookResult{}, ErrFenced
		}
		a.TurnID = q.TurnID
		a.StopHookTurn = ""
		if a.Fence != "" {
			a.CheckpointTurn = q.TurnID
		}
	case "stop":
		if q.TurnID == "" || q.TurnID != a.TurnID {
			return HookResult{}, ErrConflict
		}
		a.StopHookTurn = q.TurnID
	case "permission-request":
		if q.TurnID != "" && q.TurnID != a.TurnID {
			return HookResult{}, ErrConflict
		}
	default:
		return HookResult{}, ErrUnsupported
	}
	a.ProviderID = q.ProviderID
	a.Transcript = q.Transcript
	a.Callbacks = callbacks
	if childIndex >= 0 {
		ch := parent.Children[childIndex]
		ch.ProviderID = a.ProviderID
		ch.Transcript = a.Transcript
		ch.TurnID = a.TurnID
		ch.StopHookTurn = a.StopHookTurn
		ch.Callbacks = a.Callbacks
		if len(ch.Members) == 0 {
			ch.Members = tree
		}
		parent.Children[childIndex] = ch
		_, err = c.Store.UpdateAttempt(ctx, parent, parent.Revision)
		return HookResult{OK: err == nil}, err
	}
	if _, err = c.Store.UpdateAttempt(ctx, a, a.Revision); err != nil {
		return HookResult{}, err
	}
	if c.Activity != nil {
		state := domain.ActivityState(q.State)
		switch state {
		case domain.ActivityActive, domain.ActivityIdle, domain.ActivityWaitingInput, domain.ActivityBlocked:
		default:
			return HookResult{}, ErrUnknown
		}
		err = c.Activity(context.WithValue(ctx, activityKey{}, q.SessionID), domain.SessionID(q.SessionID), ports.ActivitySignal{Valid: true, State: state, Event: q.Event})
	}
	return HookResult{OK: err == nil}, err
}
