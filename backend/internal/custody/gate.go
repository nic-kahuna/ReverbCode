package custody

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// Gate is the one process-local serialization boundary between a durable fence
// request and native effects. Long drains and authority calls run outside it.
type Gate struct {
	store   Store
	mu      sync.Mutex
	lanes   map[string]*sync.Mutex
	streams map[string]map[*fencedStream]bool
}

func NewGate(store Store) *Gate {
	return &Gate{store: store, lanes: make(map[string]*sync.Mutex), streams: make(map[string]map[*fencedStream]bool)}
}
func (g *Gate) lane(id string) *sync.Mutex {
	g.mu.Lock()
	defer g.mu.Unlock()
	p := g.lanes[id]
	if p == nil {
		p = &sync.Mutex{}
		g.lanes[id] = p
	}
	return p
}

// WorkerID maps only the existing reviewer naming convention. Exact durable
// runtime resolution is additionally checked by the facade before effects.
func WorkerID(id string) string { return strings.TrimPrefix(id, "review-") }

func (g *Gate) Inspect(ctx context.Context, id string) (Attempt, bool, error) {
	id = WorkerID(id)
	rec, ok, err := g.store.GetSession(ctx, domain.SessionID(id))
	if err != nil {
		return Attempt{}, false, err
	}
	if !ok {
		return Attempt{}, false, nil
	}
	managed, err := g.store.ManagedProject(ctx, string(rec.ProjectID))
	if err != nil {
		return Attempt{}, false, err
	}
	if !managed {
		return Attempt{}, false, nil
	}
	a, ok, err := g.store.CurrentAttempt(ctx, id)
	if err != nil {
		return a, true, err
	}
	if !ok {
		return a, true, fmt.Errorf("%w: legacy generation %s", ErrUnknown, id)
	}
	return a, true, nil
}

type permitKey struct{}
type permit struct {
	gate       *Gate
	attempt    string
	generation int64
}

// admittedContext is unexported: only the coordinator which binds a committed
// authority response may authorize a managed write. It cannot un-fence a hold.
func (g *Gate) admittedContext(ctx context.Context, a Attempt) context.Context {
	return context.WithValue(ctx, permitKey{}, permit{g, a.AttemptID, a.Generation})
}

func (g *Gate) Effect(ctx context.Context, id string, write bool, fn func() error) error {
	id = WorkerID(id)
	lane := g.lane(id)
	lane.Lock()
	defer lane.Unlock()
	a, managed, err := g.Inspect(ctx, id)
	if err != nil {
		return err
	}
	if !managed {
		return fn()
	}
	if a.Fence != "" || a.Retired {
		return ErrFenced
	}
	if write {
		p, ok := ctx.Value(permitKey{}).(permit)
		if ok && (p.gate != g || p.attempt != a.AttemptID || p.generation != a.Generation) {
			return ErrConflict
		}
		if !ok {
			return fmt.Errorf("%w: unmanaged write into managed generation", ErrFenced)
		}
	}
	return fn()
}

func (g *Gate) Destruction(ctx context.Context, id string) error {
	_, managed, err := g.Inspect(ctx, id)
	if err != nil {
		return err
	}
	if managed {
		return ErrFenced
	}
	return nil
}

// Request persists before returning. Repeated identical requests are idempotent;
// a competing request may never replace the current hold.
func (g *Gate) Request(ctx context.Context, id, attempt, request string, generation int64, foreground string) (Attempt, error) {
	lane := g.lane(id)
	lane.Lock()
	defer lane.Unlock()
	a, managed, err := g.Inspect(ctx, id)
	if err != nil {
		return a, err
	}
	if !managed {
		return a, ErrUnsupported
	}
	if request == "" || a.AttemptID != attempt || a.Generation != generation {
		return a, ErrConflict
	}
	if a.Fence != "" {
		if a.RequestID == request && a.ForegroundID == foreground {
			return a, nil
		}
		return a, ErrConflict
	}
	a.RequestID = request
	a.ForegroundID = foreground
	a.Fence = "requested"
	a.Blockers = []string{"checkpoint and writer drain pending"}
	updated, err := g.store.UpdateAttempt(ctx, a, a.Revision)
	if err != nil {
		return updated, err
	}
	g.mu.Lock()
	streams := []*fencedStream{}
	for stream := range g.streams[id] {
		streams = append(streams, stream)
	}
	delete(g.streams, id)
	g.mu.Unlock()
	for _, stream := range streams {
		_ = stream.Close()
	}
	return updated, nil
}
