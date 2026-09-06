package sessionguard

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/admission"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

type fakeStore struct {
	rec     domain.SessionRecord
	ok      bool
	err     error
	hold    domain.WorkerSchedulingHold
	held    bool
	holdErr error
}

func (s *fakeStore) GetWorkerSchedulingHold(_ context.Context, _ domain.SessionID) (domain.WorkerSchedulingHold, bool, error) {
	return s.hold, s.held, s.holdErr
}

func (s *fakeStore) GetSession(_ context.Context, _ domain.SessionID) (domain.SessionRecord, bool, error) {
	return s.rec, s.ok, s.err
}

type fakeMessenger struct {
	sent []string
	err  error
}

type barrierStore struct {
	mu   sync.RWMutex
	rec  domain.SessionRecord
	hold domain.WorkerSchedulingHold
	held bool
	gate *admission.Gate
}

func newBarrierStore() *barrierStore {
	return &barrierStore{
		rec:  domain.SessionRecord{ID: "s1", ProjectID: "p1", Activity: domain.Activity{State: domain.ActivityActive}},
		gate: admission.New(),
	}
}

func (s *barrierStore) AdmissionGate() *admission.Gate { return s.gate }

func (s *barrierStore) GetSession(_ context.Context, _ domain.SessionID) (domain.SessionRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rec, true, nil
}

func (s *barrierStore) GetWorkerSchedulingHold(_ context.Context, _ domain.SessionID) (domain.WorkerSchedulingHold, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hold, s.held, nil
}

func (s *barrierStore) setHeld() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hold = domain.WorkerSchedulingHold{SessionID: s.rec.ID, HeldAt: time.Now()}
	s.held = true
}

type blockingMessenger struct {
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	sent    int
}

func (m *blockingMessenger) Send(ctx context.Context, _ domain.SessionID, _ string) error {
	close(m.entered)
	select {
	case <-m.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	m.mu.Lock()
	m.sent++
	m.mu.Unlock()
	return nil
}

func (m *blockingMessenger) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sent
}

func (m *fakeMessenger) Send(_ context.Context, _ domain.SessionID, msg string) error {
	m.sent = append(m.sent, msg)
	return m.err
}

func record(state domain.ActivityState, terminated bool) domain.SessionRecord {
	return domain.SessionRecord{ID: "s1", IsTerminated: terminated, Activity: domain.Activity{State: state}}
}

func TestGuard_OutcomeByState(t *testing.T) {
	cases := []struct {
		name        string
		rec         domain.SessionRecord
		ok          bool
		wantDeliver Outcome
		wantNudge   Outcome
	}{
		{"active", record(domain.ActivityActive, false), true, Sent, Sent},
		{"idle", record(domain.ActivityIdle, false), true, Sent, Sent},
		// waiting_input is the split that motivates two methods: a user message
		// (or its Enter re-submit) belongs at an idle prompt; an unsolicited
		// automated nudge does not.
		{"waiting_input", record(domain.ActivityWaitingInput, false), true, Sent, SuppressedAwaitingUser},
		{"blocked", record(domain.ActivityBlocked, false), true, SuppressedAwaitingUser, SuppressedAwaitingUser},
		// exited is refused even without IsTerminated: the pane holds an
		// interactive shell after agent exit, so a paste would execute there.
		{"exited", record(domain.ActivityExited, false), true, SuppressedTerminated, SuppressedTerminated},
		{"terminated", record(domain.ActivityIdle, true), true, SuppressedTerminated, SuppressedTerminated},
		{"missing", domain.SessionRecord{}, false, SuppressedNotFound, SuppressedNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for method, want := range map[string]Outcome{"Deliver": tc.wantDeliver, "Nudge": tc.wantNudge} {
				msgr := &fakeMessenger{}
				g := New(&fakeStore{rec: tc.rec, ok: tc.ok}, msgr, nil)
				var got Outcome
				var err error
				if method == "Deliver" {
					got, err = g.Deliver(context.Background(), "s1", "hello")
				} else {
					got, err = g.Nudge(context.Background(), "s1", "hello")
				}
				if err != nil {
					t.Fatalf("%s: unexpected error: %v", method, err)
				}
				if got != want {
					t.Errorf("%s: outcome = %v, want %v", method, got, want)
				}
				if wantSent := want == Sent; (len(msgr.sent) == 1) != wantSent {
					t.Errorf("%s: messenger sends = %d, want sent=%v", method, len(msgr.sent), wantSent)
				}
			}
		})
	}
}

func TestGuard_StoreErrorFailsClosed(t *testing.T) {
	msgr := &fakeMessenger{}
	g := New(&fakeStore{err: errors.New("db locked")}, msgr, nil)
	for name, call := range map[string]func() (Outcome, error){
		"Deliver": func() (Outcome, error) { return g.Deliver(context.Background(), "s1", "x") },
		"Nudge":   func() (Outcome, error) { return g.Nudge(context.Background(), "s1", "x") },
	} {
		got, err := call()
		if err == nil {
			t.Fatalf("%s: want error from store failure", name)
		}
		if got != SuppressedUnknown {
			t.Errorf("%s: outcome = %v, want SuppressedUnknown", name, got)
		}
	}
	if len(msgr.sent) != 0 {
		t.Errorf("messenger was called %d times on unknown state, want 0", len(msgr.sent))
	}
}

func TestGuard_HoldPolicy(t *testing.T) {
	held := &fakeStore{
		rec:  record(domain.ActivityActive, false),
		ok:   true,
		hold: domain.WorkerSchedulingHold{SessionID: "s1", HeldAt: time.Now()},
		held: true,
	}
	for _, method := range []string{"Deliver", "Nudge"} {
		msgr := &fakeMessenger{}
		g := New(held, msgr, nil)
		var got Outcome
		var err error
		if method == "Deliver" {
			got, err = g.Deliver(context.Background(), "s1", "ordinary")
		} else {
			got, err = g.Nudge(context.Background(), "s1", "ordinary")
		}
		if err != nil || got != SuppressedHeld {
			t.Fatalf("%s held outcome = %v, err=%v; want SuppressedHeld, nil", method, got, err)
		}
		if len(msgr.sent) != 0 {
			t.Fatalf("%s sent %d messages while held, want 0", method, len(msgr.sent))
		}
	}
	held.rec = record(domain.ActivityExited, false)
	got, err := New(held, &fakeMessenger{}, nil).Deliver(context.Background(), "s1", "ordinary")
	if err != nil || got != SuppressedHeld {
		t.Fatalf("held exited delivery outcome = %v, err=%v; want SuppressedHeld, nil", got, err)
	}
	held.rec = record(domain.ActivityActive, false)

	msgr := &fakeMessenger{}
	g := New(held, msgr, nil)
	got, err = g.Checkpoint(context.Background(), "s1")
	if err != nil || got != Sent {
		t.Fatalf("checkpoint held outcome = %v, err=%v; want Sent, nil", got, err)
	}
	if !reflect.DeepEqual(msgr.sent, []string{domain.WorkerCheckpointPrompt}) {
		t.Fatalf("checkpoint messages = %#v, want fixed prompt", msgr.sent)
	}

	msgr = &fakeMessenger{}
	g = New(&fakeStore{rec: record(domain.ActivityActive, false), ok: true}, msgr, nil)
	got, err = g.Checkpoint(context.Background(), "s1")
	if err != nil || got != SuppressedNotHeld {
		t.Fatalf("checkpoint unheld outcome = %v, err=%v; want SuppressedNotHeld, nil", got, err)
	}
	if len(msgr.sent) != 0 {
		t.Fatalf("unheld checkpoint sent %d messages, want 0", len(msgr.sent))
	}
}

func TestGuard_CheckpointRefusesUnsafeStates(t *testing.T) {
	cases := []struct {
		name string
		rec  domain.SessionRecord
		ok   bool
		want Outcome
	}{
		{"missing", domain.SessionRecord{}, false, SuppressedNotFound},
		{"terminated", record(domain.ActivityIdle, true), true, SuppressedTerminated},
		{"exited", record(domain.ActivityExited, false), true, SuppressedTerminated},
		{"blocked", record(domain.ActivityBlocked, false), true, SuppressedAwaitingUser},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msgr := &fakeMessenger{}
			g := New(&fakeStore{rec: tc.rec, ok: tc.ok, held: true}, msgr, nil)
			got, err := g.Checkpoint(context.Background(), "s1")
			if err != nil || got != tc.want {
				t.Fatalf("checkpoint outcome = %v, err=%v; want %v, nil", got, err, tc.want)
			}
			if len(msgr.sent) != 0 {
				t.Fatalf("checkpoint sent %d messages, want 0", len(msgr.sent))
			}
		})
	}
}

func TestGuard_HoldReadErrorFailsClosed(t *testing.T) {
	holdErr := errors.New("hold table unavailable")
	msgr := &fakeMessenger{}
	g := New(&fakeStore{rec: record(domain.ActivityActive, false), ok: true, holdErr: holdErr}, msgr, nil)
	for name, call := range map[string]func() (Outcome, error){
		"Deliver":    func() (Outcome, error) { return g.Deliver(context.Background(), "s1", "x") },
		"Nudge":      func() (Outcome, error) { return g.Nudge(context.Background(), "s1", "x") },
		"Checkpoint": func() (Outcome, error) { return g.Checkpoint(context.Background(), "s1") },
	} {
		got, err := call()
		if !errors.Is(err, holdErr) || got != SuppressedUnknown {
			t.Errorf("%s outcome = %v, err=%v; want SuppressedUnknown wrapping %v", name, got, err, holdErr)
		}
	}
	if len(msgr.sent) != 0 {
		t.Fatalf("hold read failure sent %d messages, want 0", len(msgr.sent))
	}
}

func TestGuard_MessengerErrorIsSentPlusError(t *testing.T) {
	sendErr := errors.New("pane gone")
	g := New(&fakeStore{rec: record(domain.ActivityActive, false), ok: true}, &fakeMessenger{err: sendErr}, nil)
	got, err := g.Deliver(context.Background(), "s1", "x")
	if !errors.Is(err, sendErr) {
		t.Fatalf("error = %v, want wrapped %v", err, sendErr)
	}
	if got != Sent {
		t.Errorf("outcome = %v, want Sent (the write was attempted)", got)
	}
}

func TestGuard_HoldCommitCannotOvertakeAdmittedPaneWrite(t *testing.T) {
	store := newBarrierStore()
	messenger := &blockingMessenger{entered: make(chan struct{}), release: make(chan struct{})}
	guard := New(store, messenger, nil)
	delivered := make(chan struct{})
	go func() {
		defer close(delivered)
		outcome, err := guard.Deliver(context.Background(), "s1", "before hold")
		if err != nil || outcome != Sent {
			t.Errorf("delivery outcome = %v, err=%v; want Sent, nil", outcome, err)
		}
	}()
	<-messenger.entered

	holdStarted := make(chan struct{})
	holdCommitted := make(chan struct{})
	go func() {
		defer close(holdCommitted)
		close(holdStarted)
		unlock, err := store.gate.Lock(context.Background(), "p1")
		if err != nil {
			t.Errorf("acquire hold lane: %v", err)
			return
		}
		defer unlock()
		store.setHeld()
	}()
	<-holdStarted
	select {
	case <-holdCommitted:
		t.Fatal("hold committed before the admitted pane write finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(messenger.release)
	<-delivered
	<-holdCommitted
	if got := messenger.count(); got != 1 {
		t.Fatalf("messenger sends = %d, want 1", got)
	}
	outcome, err := guard.Deliver(context.Background(), "s1", "after hold")
	if err != nil || outcome != SuppressedHeld {
		t.Fatalf("post-hold delivery outcome = %v, err=%v; want SuppressedHeld, nil", outcome, err)
	}
	if got := messenger.count(); got != 1 {
		t.Fatalf("post-hold messenger sends = %d, want 1", got)
	}
}

func TestGuard_WaitingPaneWriteObservesCommittedHold(t *testing.T) {
	store := newBarrierStore()
	messenger := &fakeMessenger{}
	guard := New(store, messenger, nil)
	unlock, err := store.gate.Lock(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var outcome Outcome
	var sendErr error
	go func() {
		defer close(done)
		outcome, sendErr = guard.Deliver(context.Background(), "s1", "racing hold")
	}()
	store.setHeld()
	unlock()
	<-done
	if sendErr != nil || outcome != SuppressedHeld {
		t.Fatalf("delivery outcome = %v, err=%v; want SuppressedHeld, nil", outcome, sendErr)
	}
	if len(messenger.sent) != 0 {
		t.Fatalf("messenger sends = %d, want 0", len(messenger.sent))
	}
}
