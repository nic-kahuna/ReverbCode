// Package admission serializes project launch operations with persisted pause transitions.
// Admission pause never asserts that existing writers have stopped.
package admission

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// ErrPaused refuses new work without describing existing writer state.
var ErrPaused = errors.New("project admission paused; existing sessions may still be running")

// ErrUncertain refuses admission when durable policy cannot be read.
var ErrUncertain = errors.New("project admission state unavailable")

// Gate holds a short-lived project lane through launch completion or config commit.
// Waiting is cancellable; callers never publish a pause before an admitted launch finishes.
type Gate struct {
	mu    sync.Mutex
	lanes map[domain.ProjectID]chan struct{}
}

// New constructs an independent set of project lanes.
func New() *Gate { return &Gate{lanes: make(map[domain.ProjectID]chan struct{})} }

// Lock acquires the project lane, or returns on caller cancellation.
func (g *Gate) Lock(ctx context.Context, id domain.ProjectID) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if held, ok := ctx.Value(heldKey{}).(*heldLane); ok && held.gate == g && held.id == id && held.active.Load() {
		return func() {}, nil
	}
	g.mu.Lock()
	lane := g.lanes[id]
	if lane == nil {
		lane = make(chan struct{}, 1)
		lane <- struct{}{}
		g.lanes[id] = lane
	}
	g.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-lane:
	}
	if err := ctx.Err(); err != nil {
		lane <- struct{}{}
		return nil, err
	}
	return func() { lane <- struct{}{} }, nil
}

// For returns the one gate attached to a store. Non-durable test collaborators
// may use an independent gate; production storage always supplies the shared gate.
func For(store any) *Gate {
	if p, ok := store.(interface{ AdmissionGate() *Gate }); ok {
		return p.AdmissionGate()
	}
	return New()
}

// Check validates the durable admission fact after the lane is acquired.
func Check(project domain.ProjectRecord) error {
	if project.ConfigDecodeError != "" {
		return fmt.Errorf("%w: %s", ErrUncertain, project.ConfigDecodeError)
	}
	if project.Config.AdmissionPaused {
		return ErrPaused
	}
	return nil
}

type heldKey struct{}
type heldLane struct {
	gate   *Gate
	id     domain.ProjectID
	active atomic.Bool
}

// Enter lets a composite operation retain admission across nested manager calls.
func (g *Gate) Enter(ctx context.Context, id domain.ProjectID) (context.Context, func(), error) {
	unlock, err := g.Lock(ctx, id)
	if err != nil {
		return ctx, nil, err
	}
	held := &heldLane{gate: g, id: id}
	held.active.Store(true)
	return context.WithValue(ctx, heldKey{}, held), func() { held.active.Store(false); unlock() }, nil
}
