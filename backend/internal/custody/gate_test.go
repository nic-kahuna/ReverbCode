package custody

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

type memoryStore struct {
	mu      sync.Mutex
	a       Attempt
	rec     domain.SessionRecord
	managed bool
}

func copyAttempt(a Attempt) Attempt {
	b, _ := json.Marshal(a)
	var out Attempt
	_ = json.Unmarshal(b, &out)
	return out
}
func (s *memoryStore) ManagedProject(context.Context, string) (bool, error) { return s.managed, nil }
func (s *memoryStore) CreateAttempt(_ context.Context, a Attempt) (Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a.Generation = 1
	a.Revision = 1
	s.a = copyAttempt(a)
	return a, nil
}
func (s *memoryStore) CreateSuccessor(context.Context, Attempt, Attempt) (Attempt, error) {
	return Attempt{}, ErrUnsupported
}
func (s *memoryStore) UpdateAttempt(_ context.Context, a Attempt, revision int64) (Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.a.Revision != revision {
		return a, ErrConflict
	}
	if err := ValidateTransition(s.a, a); err != nil {
		return a, err
	}
	a.Revision++
	s.a = copyAttempt(a)
	return a, nil
}
func (s *memoryStore) GetAttempt(_ context.Context, session, attempt string) (Attempt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return copyAttempt(s.a), s.a.SessionID == session && s.a.AttemptID == attempt, nil
}
func (s *memoryStore) CurrentAttempt(_ context.Context, id string) (Attempt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return copyAttempt(s.a), s.a.SessionID == id, nil
}
func (s *memoryStore) ListAttempts(context.Context) ([]Attempt, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return []Attempt{copyAttempt(s.a)}, s.a.Generation, nil
}
func (s *memoryStore) GetSession(_ context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	return s.rec, id == s.rec.ID, nil
}
func (s *memoryStore) GetProject(context.Context, string) (domain.ProjectRecord, bool, error) {
	return domain.ProjectRecord{}, false, nil
}
func (s *memoryStore) ListProjects(context.Context) ([]domain.ProjectRecord, error) { return nil, nil }
func (s *memoryStore) ListAllSessions(context.Context) ([]domain.SessionRecord, error) {
	return []domain.SessionRecord{s.rec}, nil
}
func gateFixture() *memoryStore {
	return &memoryStore{managed: true, rec: domain.SessionRecord{ID: "p-1", ProjectID: "p"}, a: Attempt{Project: "p", SessionID: "p-1", AttemptID: "attempt", Operation: "spawn", IssueNumber: 1, Generation: 1, Revision: 1, Phase: "running", ClaimID: "claim", ClaimGeneration: 3, Operations: []string{}, Members: []ProcessIdentity{}, Blockers: []string{}}}
}
func TestFenceWaitsForFinalInFlightEffectAndRevokesFutureWrites(t *testing.T) {
	store := gateFixture()
	gate := NewGate(store)
	ctx := gate.admittedContext(context.Background(), store.a)
	entered, release := make(chan struct{}), make(chan struct{})
	effectDone := make(chan error, 1)
	go func() {
		effectDone <- gate.Effect(ctx, "p-1", true, func() error { close(entered); <-release; return nil })
	}()
	<-entered
	requestDone := make(chan error, 1)
	go func() {
		_, err := gate.Request(context.Background(), "p-1", "attempt", "request", 1, "foreground")
		requestDone <- err
	}()
	select {
	case err := <-requestDone:
		t.Fatalf("request overtook trailing effect: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-effectDone; err != nil {
		t.Fatal(err)
	}
	if err := <-requestDone; err != nil {
		t.Fatal(err)
	}
	called := false
	if err := gate.Effect(ctx, "p-1", true, func() error { called = true; return nil }); !errors.Is(err, ErrFenced) || called {
		t.Fatalf("stale permit allowed write: %v %v", err, called)
	}
	if _, err := gate.Request(context.Background(), "p-1", "attempt", "request", 1, "foreground"); err != nil {
		t.Fatal("identical hold not idempotent")
	}
	if _, err := gate.Request(context.Background(), "p-1", "attempt", "different", 1, "foreground"); !errors.Is(err, ErrConflict) {
		t.Fatal("competing request replaced hold")
	}
}
func TestGenericWritesCannotBorrowRunningTerminalAuthority(t *testing.T) {
	store := gateFixture()
	gate := NewGate(store)
	called := false
	if err := gate.Effect(context.Background(), "p-1", true, func() error { called = true; return nil }); !errors.Is(err, ErrFenced) || called {
		t.Fatal("generic no-permit write used raw-terminal exception")
	}
	store.managed = false
	if err := gate.Effect(context.Background(), "p-1", true, func() error { called = true; return nil }); err != nil || !called {
		t.Fatal("generic unmanaged behavior changed")
	}
}
func TestTransitionsDoNotReviveOrReplacePreservedIdentity(t *testing.T) {
	original := gateFixture().a
	original.Fence = "requested"
	original.RequestID = "request"
	original.Workspace = "/retained"
	original.OriginalBaseSHA = "base"
	next := copyAttempt(original)
	next.Fence = ""
	if err := ValidateTransition(original, next); err == nil {
		t.Fatal("fence cleared")
	}
	next = copyAttempt(original)
	next.Workspace = "/replacement"
	if err := ValidateTransition(original, next); err == nil {
		t.Fatal("candidate replaced")
	}
	original.Phase = "quiesced"
	original.Fence = "suspended"
	original.Certificate = &Certificate{ID: "proof", SHA256: "hash", WriterStopped: true, PreservationComplete: true}
	next = copyAttempt(original)
	next.Phase = "running"
	if err := ValidateTransition(original, next); err == nil {
		t.Fatal("quiesced attempt revived")
	}
}
