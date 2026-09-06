package custody

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

type preparationKey struct{}
type preparationScope struct {
	Coordinator *Coordinator
	SessionID   string
}

func (c *Coordinator) preparationContext(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, preparationKey{}, preparationScope{c, id})
}
func ManagedPreparation(ctx context.Context) bool {
	_, ok := ctx.Value(preparationKey{}).(preparationScope)
	return ok
}
func BindPrewriteCandidate(ctx context.Context, path, sha, branch string) error {
	scope, ok := ctx.Value(preparationKey{}).(preparationScope)
	if !ok {
		return ErrUnknown
	}
	return scope.Coordinator.Gate.Effect(ctx, scope.SessionID, true, func() error {
		a, exists, err := scope.Coordinator.Store.CurrentAttempt(ctx, scope.SessionID)
		if err != nil {
			return err
		}
		if !exists || a.Phase != "preparing" {
			return ErrConflict
		}
		if a.OriginalBaseSHA != "" && (a.OriginalBaseSHA != sha || a.Workspace != path) {
			return ErrConflict
		}
		a.Workspace = path
		a.OriginalBaseSHA = sha
		a.WorkspaceBranch = branch
		_, err = scope.Coordinator.Store.UpdateAttempt(ctx, a, a.Revision)
		return err
	})
}

// RunPreparedCommand is reachable only through an internal preparation context.
// The trusted waiting shell is observed and persisted before its command is
// released. Parent Wait alone cannot clear debt: every observed descendant and
// the command's private process group must also have positively ceased.
func RunPreparedCommand(ctx context.Context, dir, binary string, args ...string) ([]byte, bool, error) {
	scope, ok := ctx.Value(preparationKey{}).(preparationScope)
	if !ok {
		return nil, false, nil
	}
	c := scope.Coordinator
	id := scope.SessionID
	var out boundedBuffer
	var cmd *exec.Cmd
	var root ProcessIdentity
	var started bool
	reader, writer := io.Pipe()
	defer func() { _ = writer.Close(); _ = reader.Close() }()
	err := c.Gate.Effect(ctx, id, true, func() error {
		var e error
		cmd, e = preparationCommand(binary, args...)
		if e != nil {
			return e
		}
		cmd.Dir = dir
		cmd.Stdin = reader
		cmd.Stdout = &out
		cmd.Stderr = &out
		if e = cmd.Start(); e != nil {
			return e
		}
		started = true
		root, e = ObserveProcess(cmd.Process.Pid)
		if e != nil {
			return e
		}
		root.Role = "preparation"
		a, exists, e := c.Store.CurrentAttempt(ctx, id)
		if e != nil {
			return e
		}
		if !exists {
			return ErrUnknown
		}
		a.Preparations = append(a.Preparations, root)
		a, e = c.Store.UpdateAttempt(ctx, a, a.Revision)
		if e != nil {
			return e
		}
		_, e = io.WriteString(writer, "start\n")
		return e
	})
	if !started {
		return out.Bytes(), true, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	if err != nil {
		return nil, true, err
	}
	_ = writer.Close()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	observe := func() error {
		topology, unknown, e := ProcessInventory()
		if e != nil {
			return e
		}
		// Re-read identities only from this exact private preparation group.
		seen := []ProcessIdentity{}
		for _, p := range topology {
			if p.GroupID != root.GroupID {
				continue
			}
			actual, e := ObserveProcess(p.PID)
			if e != nil {
				return fmt.Errorf("%w: private preparation member observation failed: %v", ErrUnknown, e)
			}
			if actual.GroupID == root.GroupID {
				actual.Role = "preparation"
				seen = append(seen, actual)
			}
		}
		_ = unknown // disappearing unrelated PIDs do not represent command completion
		lane := c.Gate.lane(id)
		lane.Lock()
		defer lane.Unlock()
		a, exists, e := c.Store.CurrentAttempt(ctx, id)
		if e != nil {
			return e
		}
		if !exists {
			return ErrUnknown
		}
		changed := false
		for _, p := range seen {
			found := false
			for _, old := range a.Preparations {
				if old.Same(p) {
					found = true
					break
				}
			}
			if !found {
				a.Preparations = append(a.Preparations, p)
				changed = true
			}
		}
		if changed {
			_, e = c.Store.UpdateAttempt(ctx, a, a.Revision)
		}
		return e
	}
	for {
		select {
		case <-ctx.Done():
			return nil, true, fmt.Errorf("%w: preparation still requires reconciliation: %v", ErrUnknown, ctx.Err())
		case <-ticker.C:
			if e := observe(); e != nil {
				return nil, true, e
			}
		case e := <-done:
			if probeErr := observe(); probeErr != nil {
				return nil, true, probeErr
			}
			if e != nil {
				return out.Bytes(), true, fmt.Errorf("preparation %s: %w", strings.Join(append([]string{binary}, args...), " "), e)
			}
			a, exists, probeErr := c.Store.CurrentAttempt(ctx, id)
			if probeErr != nil {
				return nil, true, probeErr
			}
			if !exists {
				return nil, true, ErrUnknown
			}
			for _, p := range a.Preparations {
				gone, e := ProcessGone(p)
				if e != nil {
					return nil, true, e
				}
				if !gone {
					return nil, true, fmt.Errorf("%w: preparation descendant remains", ErrUnknown)
				}
			}
			return out.Bytes(), true, nil
		}
	}
}

func ValidatePreparation(ctx context.Context, id string) error {
	scope, ok := ctx.Value(preparationKey{}).(preparationScope)
	if !ok || scope.SessionID != id {
		return ErrFenced
	}
	return scope.Coordinator.Gate.Effect(ctx, id, true, func() error { return nil })
}
