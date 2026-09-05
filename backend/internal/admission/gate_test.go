package admission

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGateWaitIsCancellableAndProjectsIndependent(t *testing.T) {
	g := New()
	ctx := context.Background()
	release, err := g.Lock(ctx, "one")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	other, err := g.Lock(ctx, "two")
	if err != nil {
		t.Fatal(err)
	}
	other()
	bounded, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if _, err := g.Lock(bounded, "one"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
func TestCompositeAdmissionCannotEscapeItsLifetime(t *testing.T) {
	g := New()
	ctx, release, err := g.Enter(context.Background(), "one")
	if err != nil {
		t.Fatal(err)
	}
	nested, err := g.Lock(ctx, "one")
	if err != nil {
		t.Fatal(err)
	}
	nested()
	release()
	held, err := g.Lock(context.Background(), "one")
	if err != nil {
		t.Fatal(err)
	}
	defer held()
	bounded, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if _, err := g.Lock(bounded, "one"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expired context retained admission", err)
	}
}
