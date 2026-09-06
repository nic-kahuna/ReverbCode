package store_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/bootguard"
	"github.com/aoagents/agent-orchestrator/backend/internal/custody"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// These fixtures test storage transactions, never actual writer-stop proof.
type custodyStoreFixture struct {
	dir     string
	guard   *bootguard.Guard
	store   *sqlite.Store
	session domain.SessionRecord
}

func newCustodyStoreFixture(t *testing.T, activate bool) *custodyStoreFixture {
	t.Helper()
	f := &custodyStoreFixture{dir: t.TempDir()}
	var err error
	f.guard, err = bootguard.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.guard.Close() })
	f.store, err = sqlite.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.store.Close() })
	ctx := context.Background()
	p := domain.ProjectRecord{ID: "custody", Path: filepath.Join(f.dir, "repo"), RegisteredAt: time.Now().UTC(), Config: domain.ProjectConfig{AdmissionPaused: true}}
	if err = f.store.UpsertProject(ctx, p); err != nil {
		t.Fatal(err)
	}
	r := sampleRecord(p.ID)
	r.Harness = domain.HarnessCodex
	r.Metadata = domain.SessionMetadata{}
	r.IssueID = "github:fixture/repo#1"
	f.session, err = f.store.CreateSession(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if activate {
		if err = f.store.ActivateCustody(ctx, f.guard, []string{p.ID}); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func (f *custodyStoreFixture) seed(t *testing.T, id string) custody.Attempt {
	t.Helper()
	a, err := f.store.CreateAttempt(context.Background(), custody.Attempt{Project: "custody", SessionID: string(f.session.ID), AttemptID: id, Operation: "spawn", IssueNumber: 1, Phase: "seed", ClaimID: "fixture-claim"})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func (f *custodyStoreFixture) quiesced(t *testing.T) custody.Attempt {
	t.Helper()
	a := f.seed(t, "original")
	a.Phase = "quiesced"
	a.Fence = "suspended"
	a.RequestID = "request"
	a.ForegroundID = "foreground"
	a.Workspace = filepath.Join(f.dir, "candidate")
	a.OriginalBaseSHA = strings.Repeat("a", 40)
	a.Certificate = &custody.Certificate{ID: strings.Repeat("b", 32), SHA256: strings.Repeat("c", 64), PreservationComplete: true, WriterStopped: true}
	got, err := f.store.UpdateAttempt(context.Background(), a, a.Revision)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func successorOf(a custody.Attempt) custody.Attempt {
	return custody.Attempt{Project: a.Project, SessionID: a.SessionID, AttemptID: "successor", Operation: "background_turn", IssueNumber: a.IssueNumber, Phase: "seed", ClaimID: a.ClaimID, Workspace: a.Workspace, OriginalBaseSHA: a.OriginalBaseSHA, ProviderID: a.ProviderID, PredecessorAttemptID: a.AttemptID}
}

func TestCustodyActivationRequiresMatchingHeldGuardAndAllPausedProjects(t *testing.T) {
	for _, name := range []string{"nil", "wrong-directory", "closed", "unpaused", "missing-member", "malformed-marker"} {
		t.Run(name, func(t *testing.T) {
			f := newCustodyStoreFixture(t, false)
			ctx := context.Background()
			guard := f.guard
			projects := []string{"custody"}
			switch name {
			case "nil":
				guard = nil
			case "wrong-directory":
				other := newCustodyStoreFixture(t, false)
				guard = other.guard
			case "closed":
				if err := guard.Close(); err != nil {
					t.Fatal(err)
				}
			case "unpaused":
				p, _, err := f.store.GetProject(ctx, "custody")
				if err != nil {
					t.Fatal(err)
				}
				p.Config.AdmissionPaused = false
				if err = f.store.UpsertProject(ctx, p); err != nil {
					t.Fatal(err)
				}
			case "missing-member":
				projects = append(projects, "unknown")
			case "malformed-marker":
				if err := os.WriteFile(filepath.Join(f.dir, bootguard.MarkerName), []byte("malformed\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.store.ActivateCustody(ctx, guard, projects); err == nil {
				t.Fatal("activation accepted invalid ownership/precondition")
			}
			if managed, err := f.store.ManagedProject(ctx, "custody"); err != nil || managed {
				t.Fatalf("activation leaked managed authority: %v %v", managed, err)
			}
			if attempts, gen, err := f.store.ListAttempts(ctx); err != nil || len(attempts) != 0 || gen != 0 {
				t.Fatalf("activation leaked revision: %v %d %v", attempts, gen, err)
			}
			if name != "malformed-marker" {
				proof, err := bootguard.Inspect(f.dir)
				if err != nil || proof.RequiredProtocol != 1 {
					t.Fatalf("failed validation raised floor: %+v %v", proof, err)
				}
			}
		})
	}
}

func TestCustodyActivationRatchetsBeforeAuthorityAndSurvivesReopen(t *testing.T) {
	f := newCustodyStoreFixture(t, false)
	ctx := context.Background()
	before, err := bootguard.Inspect(f.dir)
	if err != nil || before.RequiredProtocol != 1 {
		t.Fatalf("initial floor %+v %v", before, err)
	}
	if err = f.store.ActivateCustody(ctx, f.guard, []string{"custody"}); err != nil {
		t.Fatal(err)
	}
	proof, err := bootguard.Inspect(f.dir)
	if err != nil || proof.RequiredProtocol != 2 {
		t.Fatalf("activated floor %+v %v", proof, err)
	}
	if _, err = bootguard.Open(f.dir); err == nil {
		t.Fatal("second startup guard acquired held directory")
	}
	a := f.seed(t, "first")
	if err = f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.store, err = sqlite.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := f.store.CurrentAttempt(ctx, a.SessionID)
	if err != nil || !ok || !reflect.DeepEqual(a, got) {
		t.Fatalf("reopen lost exact attempt: %+v %v %v", got, ok, err)
	}
	if err = f.guard.Ratchet(1); err == nil {
		t.Fatal("activated floor lowered")
	}
}

func TestCustodyAttemptCASHasOneWinnerAndFailedWritesAreAtomic(t *testing.T) {
	f := newCustodyStoreFixture(t, true)
	ctx := context.Background()
	a := f.seed(t, "first")
	_, before, err := f.store.ListAttempts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, request := range []string{"foreground-one", "foreground-two"} {
		wg.Add(1)
		go func(request string) {
			defer wg.Done()
			<-start
			next := a
			next.Fence = "requested"
			next.RequestID = request
			_, e := f.store.UpdateAttempt(ctx, next, a.Revision)
			results <- e
		}(request)
	}
	close(start)
	wg.Wait()
	close(results)
	success := 0
	for e := range results {
		if e == nil {
			success++
		}
	}
	if success != 1 {
		t.Fatalf("CAS winners=%d", success)
	}
	current, ok, err := f.store.CurrentAttempt(ctx, a.SessionID)
	if err != nil || !ok {
		t.Fatal(err)
	}
	_, after, err := f.store.ListAttempts(ctx)
	if err != nil || after != before+1 || current.Revision != a.Revision+1 {
		t.Fatalf("failed CAS changed counters: %d %d %+v %v", before, after, current, err)
	}
	for _, change := range []func(*custody.Attempt){func(n *custody.Attempt) { n.Fence = "" }, func(n *custody.Attempt) { n.RequestID = "replacement" }, func(n *custody.Attempt) { n.SessionID = "other" }, func(n *custody.Attempt) { n.Generation++ }} {
		next := current
		change(&next)
		if _, err = f.store.UpdateAttempt(ctx, next, current.Revision); err == nil {
			t.Fatal("immutable/fenced identity changed")
		}
	}
	got, _, err := f.store.CurrentAttempt(ctx, a.SessionID)
	if err != nil || !reflect.DeepEqual(got, current) {
		t.Fatalf("failed writes changed document: %+v %v", got, err)
	}
}

func TestCustodySuccessorRetiresAtomicallyAndKeepsHistoricalCertificate(t *testing.T) {
	f := newCustodyStoreFixture(t, true)
	ctx := context.Background()
	old := f.quiesced(t)
	next, err := f.store.CreateSuccessor(ctx, old, successorOf(old))
	if err != nil {
		t.Fatal(err)
	}
	if next.Generation <= old.Generation || next.Revision != 1 {
		t.Fatalf("successor counters %+v", next)
	}
	got, ok, err := f.store.CurrentAttempt(ctx, old.SessionID)
	if err != nil || !ok || got.AttemptID != next.AttemptID {
		t.Fatalf("current %+v %v %v", got, ok, err)
	}
	historical, ok, err := f.store.GetAttempt(ctx, old.SessionID, old.AttemptID)
	if err != nil || !ok || !historical.Retired || historical.Fence != "retired" || historical.SuccessorAttemptID != next.AttemptID || !reflect.DeepEqual(historical.Certificate, old.Certificate) {
		t.Fatalf("lost history %+v %v %v", historical, ok, err)
	}
	if _, err = f.store.UpdateAttempt(ctx, historical, historical.Revision); err == nil {
		t.Fatal("retired attempt writable")
	}
	if _, err = f.store.CreateSuccessor(ctx, old, successorOf(old)); err == nil {
		t.Fatal("retirement replay created another successor")
	}
	all, _, err := f.store.ListAttempts(ctx)
	if err != nil || len(all) != 2 {
		t.Fatalf("retirement history %d %v", len(all), err)
	}
}

func TestCustodyFailedSuccessorDoesNotRetireOriginal(t *testing.T) {
	f := newCustodyStoreFixture(t, true)
	ctx := context.Background()
	old := f.quiesced(t)
	next := successorOf(old)
	next.AttemptID = old.AttemptID
	_, before, err := f.store.ListAttempts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.CreateSuccessor(ctx, old, next); err == nil {
		t.Fatal("duplicate successor inserted")
	}
	got, ok, err := f.store.CurrentAttempt(ctx, old.SessionID)
	if err != nil || !ok || !reflect.DeepEqual(got, old) {
		t.Fatalf("failed insert retired original: %+v %v %v", got, ok, err)
	}
	_, after, err := f.store.ListAttempts(ctx)
	if err != nil || before != after {
		t.Fatalf("failed transaction changed generation: %d %d %v", before, after, err)
	}
}

func TestCustodySuccessorRejectsChangedCallerHistory(t *testing.T) {
	for _, name := range []string{"workspace", "request", "certificate", "issue"} {
		t.Run(name, func(t *testing.T) {
			f := newCustodyStoreFixture(t, true)
			ctx := context.Background()
			old := f.quiesced(t)
			altered := old
			switch name {
			case "workspace":
				altered.Workspace = filepath.Join(f.dir, "other")
			case "request":
				altered.RequestID = "wrong"
			case "certificate":
				c := *old.Certificate
				c.SHA256 = strings.Repeat("d", 64)
				altered.Certificate = &c
			case "issue":
				altered.IssueNumber++
			}
			if _, err := f.store.CreateSuccessor(ctx, altered, successorOf(altered)); err == nil {
				t.Fatal("same revision allowed caller to replace preserved history")
			}
			got, ok, err := f.store.GetAttempt(ctx, old.SessionID, old.AttemptID)
			if err != nil || !ok || !reflect.DeepEqual(got, old) {
				t.Fatalf("historical evidence changed: %+v %v %v", got, ok, err)
			}
		})
	}
}

func TestCustodyDestructiveStorageFencesPreserveLiveIdentity(t *testing.T) {
	f := newCustodyStoreFixture(t, true)
	ctx := context.Background()
	if _, err := f.store.DeleteSession(ctx, f.session.ID); err == nil {
		t.Fatal("managed seed session deleted")
	}
	original, _, err := f.store.GetProject(ctx, "custody")
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*domain.ProjectRecord){func(p *domain.ProjectRecord) { p.Path = filepath.Join(f.dir, "other") }, func(p *domain.ProjectRecord) { p.Config.PostCreate = []string{"unexpected"} }} {
		p := original
		change(&p)
		if err = f.store.UpsertProject(ctx, p); err == nil {
			t.Fatal("managed project identity/config changed")
		}
	}
	if _, err = f.store.ArchiveProject(ctx, "custody", time.Now().UTC()); err == nil {
		t.Fatal("managed project archived")
	}
	current, _, err := f.store.GetProject(ctx, "custody")
	if err != nil || !reflect.DeepEqual(current, original) {
		t.Fatalf("blocked mutation changed project: %+v %v", current, err)
	}
	current.Config.AdmissionPaused = false
	if err = f.store.UpsertProject(ctx, current); err != nil {
		t.Fatalf("explicit admission toggle blocked: %v", err)
	}
	if changed, err := f.store.SetSessionPreviewURL(ctx, f.session.ID, "http://fixture.invalid", time.Now().UTC()); err != nil || !changed {
		t.Fatalf("observational metadata blocked: %v %v", changed, err)
	}
}

func TestCustodySuccessorRequiresFreshUnfencedSeed(t *testing.T) {
	for _, name := range []string{"retired", "certificate", "request", "issue"} {
		t.Run(name, func(t *testing.T) {
			f := newCustodyStoreFixture(t, true)
			ctx := context.Background()
			old := f.quiesced(t)
			next := successorOf(old)
			switch name {
			case "retired":
				next.Retired = true
			case "certificate":
				next.Certificate = old.Certificate
			case "request":
				next.RequestID = old.RequestID
			case "issue":
				next.IssueNumber++
			}
			if _, err := f.store.CreateSuccessor(ctx, old, next); err == nil {
				t.Fatal("successor accepted prior authority or a different issue")
			}
			got, ok, err := f.store.CurrentAttempt(ctx, old.SessionID)
			if err != nil || !ok || !reflect.DeepEqual(got, old) {
				t.Fatalf("invalid seed retired original: %+v %v %v", got, ok, err)
			}
		})
	}
}

func TestCustodyAttemptCannotBindAnotherProjectsSession(t *testing.T) {
	f := newCustodyStoreFixture(t, true)
	ctx := context.Background()
	p := domain.ProjectRecord{ID: "other", Path: filepath.Join(f.dir, "other"), RegisteredAt: time.Now().UTC(), Config: domain.ProjectConfig{AdmissionPaused: true}}
	if err := f.store.UpsertProject(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ActivateCustody(ctx, f.guard, []string{p.ID}); err != nil {
		t.Fatal(err)
	}
	_, before, err := f.store.ListAttempts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.store.CreateAttempt(ctx, custody.Attempt{Project: p.ID, SessionID: string(f.session.ID), AttemptID: "cross-project", Operation: "spawn", IssueNumber: 1, Phase: "seed", ClaimID: "cross-project"})
	if err == nil {
		t.Fatal("attempt bound one project's session to another project's authority")
	}
	all, after, err := f.store.ListAttempts(ctx)
	if err != nil || len(all) != 0 || after != before {
		t.Fatalf("rejected binding mutated storage: %v %d %d %v", all, before, after, err)
	}
}
