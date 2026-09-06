package custody

import (
	"context"
	"fmt"
	"sync"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type Runtime interface {
	ports.Runtime
	ports.Attacher
	Interrupt(context.Context, ports.RuntimeHandle) error
	SendMessage(context.Context, ports.RuntimeHandle, string) error
}

// RuntimeFacade is the sole native writer adapter distributed by the daemon.
// Runtime reads remain available for diagnosis while effects are fenced.
type RuntimeFacade struct {
	raw         Runtime
	gate        *Gate
	coordinator *Coordinator
}

func NewRuntime(raw Runtime, gate *Gate) *RuntimeFacade { return &RuntimeFacade{raw: raw, gate: gate} }
func (r *RuntimeFacade) RuntimeProcesses(ctx context.Context, h ports.RuntimeHandle) (ports.RuntimeProcessInfo, error) {
	if probe, ok := r.raw.(interface {
		RuntimeProcesses(context.Context, ports.RuntimeHandle) (ports.RuntimeProcessInfo, error)
	}); ok {
		return probe.RuntimeProcesses(ctx, h)
	}
	return ports.RuntimeProcessInfo{}, ErrUnsupported
}
func (r *RuntimeFacade) HandleFor(c ports.RuntimeConfig) (ports.RuntimeHandle, error) {
	return r.raw.HandleFor(c)
}
func (r *RuntimeFacade) GetOutput(ctx context.Context, h ports.RuntimeHandle, n int) (string, error) {
	return r.raw.GetOutput(ctx, h, n)
}
func (r *RuntimeFacade) IsAlive(ctx context.Context, h ports.RuntimeHandle) (bool, error) {
	return r.raw.IsAlive(ctx, h)
}
func (r *RuntimeFacade) EnsureControlServer(ctx context.Context) error {
	if c, ok := r.raw.(interface{ EnsureControlServer(context.Context) error }); ok {
		return c.EnsureControlServer(ctx)
	}
	return nil
}
func (r *RuntimeFacade) Create(ctx context.Context, c ports.RuntimeConfig) (ports.RuntimeHandle, error) {
	var out ports.RuntimeHandle
	err := r.gate.Effect(ctx, string(c.SessionID), true, func() error {
		a, managed, err := r.gate.Inspect(ctx, string(c.SessionID))
		if err != nil {
			return err
		}
		if managed {
			if a.Phase != "launching" && !(string(c.SessionID) == "review-"+a.SessionID && a.Phase == "running") {
				return ErrFenced
			}
			c.Managed = true
			if string(c.SessionID) == "review-"+a.SessionID {
				operation, ok := ctx.Value(childKey{}).(string)
				if !ok || operation == "" {
					return ErrFenced
				}
				for _, child := range a.Children {
					if child.HandleID == string(c.SessionID) {
						return ErrConflict
					}
				}
				a.Children = append(a.Children, ChildRuntime{Operation: operation, HandleID: string(c.SessionID), OriginAttemptID: a.AttemptID, OriginGeneration: a.Generation})
				a, err = r.gate.store.UpdateAttempt(ctx, a, a.Revision)
				if err != nil {
					return err
				}
				if c.Env == nil {
					c.Env = map[string]string{}
				}
				c.Env["AO_SESSION_ID"] = a.SessionID
				c.Env["AO_PROJECT_ID"] = a.Project
				c.Env["AO_NATIVE_CHILD_HANDLE"] = string(c.SessionID)
				c.Env["AO_NATIVE_ATTEMPT_ID"] = a.AttemptID
				c.Env["AO_NATIVE_GENERATION"] = fmt.Sprint(a.Generation)
			}
			if string(c.SessionID) == a.SessionID {
				if a.LaunchEffectStarted {
					return fmt.Errorf("%w: runtime creation already attempted; reconcile retained runtime", ErrUnknown)
				}
				a.LaunchEffectStarted = true
				a, err = r.gate.store.UpdateAttempt(ctx, a, a.Revision)
				if err != nil {
					return err
				}
			}
		}
		out, err = r.raw.Create(ctx, c)
		if err == nil && managed && string(c.SessionID) == a.SessionID {
			fresh, ok, e := r.gate.store.CurrentAttempt(ctx, a.SessionID)
			if e != nil {
				return e
			}
			if !ok || fresh.AttemptID != a.AttemptID {
				return ErrConflict
			}
			fresh.LaunchEffectDelivered = true
			_, err = r.gate.store.UpdateAttempt(ctx, fresh, fresh.Revision)
		}
		return err
	})
	return out, err
}
func (r *RuntimeFacade) Destroy(ctx context.Context, h ports.RuntimeHandle) error {
	return r.gate.Effect(ctx, h.ID, false, func() error {
		if err := r.gate.Destruction(ctx, h.ID); err != nil {
			return err
		}
		return r.raw.Destroy(ctx, h)
	})
}
func (r *RuntimeFacade) SendMessage(ctx context.Context, h ports.RuntimeHandle, msg string) error {
	if _, hasPermit := ctx.Value(permitKey{}).(permit); !hasPermit {
		a, managed, err := r.gate.Inspect(ctx, h.ID)
		if err != nil {
			return err
		}
		if managed {
			if h.ID != a.SessionID || r.coordinator == nil {
				return ErrFenced
			}
			rec, ok, err := r.gate.store.GetSession(ctx, domain.SessionID(a.SessionID))
			if err != nil {
				return err
			}
			if !ok {
				return ErrUnknown
			}
			return r.coordinator.StartTurn(ctx, rec, msg)
		}
	}

	if _, ok := ctx.Value(childKey{}).(string); ok && WorkerID(h.ID) != h.ID && r.coordinator != nil {
		return r.coordinator.sendChild(ctx, h.ID, msg)
	}
	return r.gate.Effect(ctx, h.ID, true, func() error { return r.raw.SendMessage(ctx, h, msg) })
}
func (r *RuntimeFacade) Interrupt(ctx context.Context, h ports.RuntimeHandle) error {
	return r.gate.Effect(ctx, h.ID, true, func() error { return r.raw.Interrupt(ctx, h) })
}
func (r *RuntimeFacade) Attach(ctx context.Context, h ports.RuntimeHandle, rows, cols uint16) (ports.Stream, error) {
	var stream ports.Stream
	err := r.gate.Effect(ctx, h.ID, false, func() error {
		a, managed, err := r.gate.Inspect(ctx, h.ID)
		if err != nil {
			return err
		}
		stream, err = r.raw.Attach(ctx, h, rows, cols)
		if err != nil {
			return err
		}
		if managed {
			pinned, ok := ctx.Value(attachmentKey{}).(attachmentPin)
			if !ok || pinned.Attempt != a.AttemptID || pinned.Generation != a.Generation {
				_ = stream.Close()
				return ErrConflict
			}
			wrapped := &fencedStream{Stream: stream, gate: r.gate, id: h.ID, attempt: a.AttemptID, generation: a.Generation}
			r.gate.mu.Lock()
			if r.gate.streams[h.ID] == nil {
				r.gate.streams[h.ID] = map[*fencedStream]bool{}
			}
			r.gate.streams[h.ID][wrapped] = true
			r.gate.mu.Unlock()
			stream = wrapped
		}
		return nil
	})
	return stream, err
}

type fencedStream struct {
	ports.Stream
	gate        *Gate
	id, attempt string
	generation  int64
	mu          sync.Mutex
	closed      bool
}

func (s *fencedStream) check(ctx context.Context) error {
	a, managed, err := s.gate.Inspect(ctx, s.id)
	if err != nil {
		return err
	}
	if !managed || a.AttemptID != s.attempt || a.Generation != s.generation || a.Fence != "" {
		return fmt.Errorf("%w: stale terminal attachment", ErrFenced)
	}
	return nil
}
func (s *fencedStream) Write(p []byte) (int, error) {
	ctx := context.Background()
	n := 0
	err := s.gate.Effect(ctx, s.id, false, func() error {
		if err := s.check(ctx); err != nil {
			return err
		}
		a, _, err := s.gate.Inspect(ctx, s.id)
		if err != nil {
			return err
		}
		if a.Phase != "running" || a.ClaimGeneration < 1 {
			return ErrFenced
		}
		n, err = s.Stream.Write(p)
		return err
	})
	return n, err
}
func (s *fencedStream) Resize(rows, cols uint16) error {
	ctx := context.Background()
	return s.gate.Effect(ctx, s.id, false, func() error {
		if err := s.check(ctx); err != nil {
			return err
		}
		return s.Stream.Resize(rows, cols)
	})
}
func (s *fencedStream) Read(p []byte) (int, error) {
	if err := s.check(context.Background()); err != nil {
		_ = s.Close()
		return 0, err
	}
	return s.Stream.Read(p)
}
func (s *fencedStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.Stream.Close()
}

type attachmentKey struct{}
type attachmentPin struct {
	Attempt    string
	Generation int64
}

// PinAttachment binds input buffered before the first PTY attach as well as all
// automatic reattaches to the one generation selected when the UI opened it.
func (r *RuntimeFacade) PinAttachment(ctx context.Context, h ports.RuntimeHandle) (context.Context, error) {
	a, managed, err := r.gate.Inspect(ctx, h.ID)
	if err != nil {
		return ctx, err
	}
	if !managed {
		return ctx, nil
	}
	if a.Fence != "" || a.Phase != "running" || a.Retired {
		return ctx, ErrFenced
	}
	return context.WithValue(ctx, attachmentKey{}, attachmentPin{a.AttemptID, a.Generation}), nil
}
