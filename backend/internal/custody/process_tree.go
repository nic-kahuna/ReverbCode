package custody

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// runtimeTree first uses read-only BSD ancestry, then obtains identity-bound
// observations only for this retained pane. Unrelated users' task-port access
// is not required. Every recorded member is independently checked as well.
func (c *Coordinator) runtimeTree(ctx context.Context, a Attempt) ([]ProcessIdentity, error) {
	if a.RuntimeHandleID == nil {
		return nil, ErrUnknown
	}
	info, err := c.Runtime.RuntimeProcesses(ctx, ports.RuntimeHandle{ID: *a.RuntimeHandleID})
	if err != nil || info.Dead {
		return nil, fmt.Errorf("%w: retained pane unavailable", ErrUnknown)
	}
	parent, err := ObserveProcess(info.PanePID)
	if err != nil {
		return nil, err
	}
	if parent.ParentPID != info.ServerPID || parent.Executable != "/bin/sh" {
		return nil, fmt.Errorf("%w: trusted retained parent identity missing", ErrUnknown)
	}
	parent.Role = "pane_parent"
	topology, _, err := ProcessInventory()
	if err != nil {
		return nil, err
	}
	seen := map[int]bool{parent.PID: true}
	tree := []ProcessIdentity{parent}
	for changed := true; changed; {
		changed = false
		for _, p := range topology {
			if seen[p.ParentPID] && !seen[p.PID] {
				actual, e := ObserveProcess(p.PID)
				if e != nil {
					return nil, e
				}
				if actual.ParentPID != p.ParentPID {
					return nil, ErrConflict
				}
				actual.Role = "writer"
				tree = append(tree, actual)
				seen[p.PID] = true
				changed = true
			}
		}
	}
	sort.Slice(tree[1:], func(i, j int) bool { return tree[i+1].PID < tree[j+1].PID })
	return tree, nil
}

// Supported idle members are the native Codex provider and its code-mode host.
// A shell, tool, callback, reviewer or unknown executable must finish before
// custody can transfer. The provider's own idle state is proved separately.
func supportedIdleTree(tree []ProcessIdentity) error {
	if len(tree) < 2 || len(tree) > 3 {
		return fmt.Errorf("%w: child operation still present", ErrUnknown)
	}
	provider := 0
	for _, p := range tree {
		if p.Role == "pane_parent" {
			continue
		}
		switch filepath.Base(p.Executable) {
		case "codex":
			provider++
		case "codex-code-mode-host":
		default:
			return fmt.Errorf("%w: unsupported idle member %s", ErrUnknown, p.Executable)
		}
	}
	if provider != 1 {
		return fmt.Errorf("%w: exact Codex provider missing", ErrUnknown)
	}
	return nil
}
func sameMembers(before, after []ProcessIdentity, stopped bool) error {
	if len(before) != len(after) {
		return ErrConflict
	}
	for _, p := range before {
		found := false
		for _, q := range after {
			if p.Same(q) {
				if p.ParentPID != q.ParentPID || p.Executable != q.Executable {
					return ErrConflict
				}
				if stopped && p.Role != "pane_parent" && q.Status != 4 {
					return fmt.Errorf("%w: writer not stopped", ErrUnknown)
				}
				found = true
				break
			}
		}
		if !found {
			return ErrConflict
		}
	}
	return nil
}

// verifyRunningIdentity confirms the immutable provider and retained pane
// identities; active command descendants are permitted and remain drain debt.
func (c *Coordinator) verifyRunningIdentity(ctx context.Context, a Attempt) error {
	if a.Retired || a.Phase != "running" || a.RuntimeHandleID == nil {
		return ErrUnknown
	}
	tree, err := c.runtimeTree(ctx, a)
	if err != nil {
		return err
	}
	return sameProvider(a.Members, tree)
}
func sameProvider(old, tree []ProcessIdentity) error {
	required := 0
	for _, member := range old {
		if member.Role != "pane_parent" && filepath.Base(member.Executable) != "codex" {
			continue
		}
		required++
		found := false
		for _, actual := range tree {
			if member.Same(actual) && member.ParentPID == actual.ParentPID && member.Executable == actual.Executable {
				found = true
				break
			}
		}
		if !found {
			return ErrConflict
		}
	}
	if required != 2 {
		return ErrUnknown
	}
	return nil
}
