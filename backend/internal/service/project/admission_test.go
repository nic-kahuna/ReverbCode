package project_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/project"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func TestAdmissionPersistsAndResumeDoesNotChangeSessions(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	row := domain.ProjectRecord{ID: "p", Path: "/tmp/project", Config: domain.ProjectConfig{DefaultBranch: "main", Env: map[string]string{"KEEP": "value"}}}
	if err := store.UpsertProject(ctx, row); err != nil {
		t.Fatal(err)
	}
	saved, err := store.CreateSession(ctx, domain.SessionRecord{ProjectID: "p", Kind: domain.KindWorker, IsTerminated: true, Metadata: domain.SessionMetadata{Branch: "candidate", WorkspacePath: "/tmp/preserved", AgentSessionID: "native-context"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertSessionWorktree(ctx, domain.SessionWorktreeRecord{SessionID: saved.ID, RepoName: domain.RootWorkspaceRepoName, Branch: "candidate", WorktreePath: "/tmp/preserved", State: "removed", PreservedRef: "refs/ao/preserved/test"}); err != nil {
		t.Fatal(err)
	}
	svc := project.New(store)
	state, err := svc.SetAdmission(ctx, "p", true)
	if err != nil {
		t.Fatal(err)
	}
	if !state.AdmissionPaused || !state.ExistingSessionsMayBeRunning || state.Scope != "new_launches_only" {
		t.Fatalf("%+v", state)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	svc = project.New(store)
	if state, err := svc.GetAdmission(ctx, "p"); err != nil || !state.AdmissionPaused {
		t.Fatalf("restart: %+v %v", state, err)
	}
	rowsBefore, err := store.ListAllSessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SetAdmission(ctx, "p", false); err != nil {
		t.Fatal(err)
	}
	rowsAfter, err := store.ListAllSessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rowsBefore, rowsAfter) {
		t.Fatal("resume changed sessions")
	}
	got, _, err := store.GetProject(ctx, "p")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Config, row.Config) {
		t.Fatalf("config changed: %+v", got.Config)
	}
}
func TestConfigReplacementPreservesPauseUnlessExplicit(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.UpsertProject(ctx, domain.ProjectRecord{ID: "p", Config: domain.ProjectConfig{AdmissionPaused: true}}); err != nil {
		t.Fatal(err)
	}
	svc := project.New(store)
	var in project.SetConfigInput
	if err := json.Unmarshal([]byte(`{"config":{"defaultBranch":"develop"}}`), &in); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SetConfig(ctx, "p", in); err != nil {
		t.Fatal(err)
	}
	state, err := svc.GetAdmission(ctx, "p")
	if err != nil || !state.AdmissionPaused {
		t.Fatalf("implicit resume: %+v %v", state, err)
	}
	if err := json.Unmarshal([]byte(`{"config":{"admissionPaused":false,"defaultBranch":"develop"}}`), &in); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SetConfig(ctx, "p", in); err != nil {
		t.Fatal(err)
	}
	state, err = svc.GetAdmission(ctx, "p")
	if err != nil || state.AdmissionPaused {
		t.Fatalf("explicit resume: %+v %v", state, err)
	}
}
func TestPauseTransitionWaitsForLaunchAndCancellationDoesNotCommit(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.UpsertProject(ctx, domain.ProjectRecord{ID: "p"}); err != nil {
		t.Fatal(err)
	}
	svc := project.New(store)
	release, err := store.AdmissionGate().Lock(ctx, "p")
	if err != nil {
		t.Fatal(err)
	}
	bounded, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if _, err := svc.SetAdmission(bounded, "p", true); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pause wait: %v", err)
	}
	state, err := svc.GetAdmission(ctx, "p")
	if err != nil || state.AdmissionPaused {
		t.Fatalf("cancelled pause committed: %+v %v", state, err)
	}
	release()
	if _, err := svc.SetAdmission(ctx, "p", true); err != nil {
		t.Fatal(err)
	}
}

type pausedProjectReadKey struct{}
type admissionReadStore struct {
	*sqlite.Store
	read, release chan struct{}
}

func (s *admissionReadStore) GetProject(ctx context.Context, id string) (domain.ProjectRecord, bool, error) {
	row, ok, err := s.Store.GetProject(ctx, id)
	if ctx.Value(pausedProjectReadKey{}) != nil {
		close(s.read)
		<-s.release
	}
	return row, ok, err
}
func TestConcurrentPauseCannotResurrectRemovedProject(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.UpsertProject(ctx, domain.ProjectRecord{ID: "p"}); err != nil {
		t.Fatal(err)
	}
	wrapped := &admissionReadStore{Store: store, read: make(chan struct{}), release: make(chan struct{})}
	svc := project.New(wrapped)
	paused := make(chan error, 1)
	go func() {
		_, err := svc.SetAdmission(context.WithValue(ctx, pausedProjectReadKey{}, true), "p", true)
		paused <- err
	}()
	<-wrapped.read
	removed := make(chan error, 1)
	go func() { _, err := svc.Remove(ctx, "p"); removed <- err }()
	select {
	case <-removed:
		t.Fatal("remove bypassed in-flight config transition")
	case <-time.After(20 * time.Millisecond):
	}
	close(wrapped.release)
	if err := <-paused; err != nil {
		t.Fatal(err)
	}
	if err := <-removed; err != nil {
		t.Fatal(err)
	}
	row, ok, err := store.GetProject(ctx, "p")
	if err != nil || !ok || row.ArchivedAt.IsZero() {
		t.Fatalf("removed project resurrected: %+v %v", row, err)
	}
	if _, err := svc.SetAdmission(ctx, "p", false); err == nil {
		t.Fatal("resume resurrected archived project")
	}
}

func TestArchivedAdmissionRemainsReadableButCannotBeChanged(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	row := domain.ProjectRecord{ID: "archived", Path: "/tmp/archived", ArchivedAt: time.Now(), Config: domain.ProjectConfig{AdmissionPaused: true, DefaultBranch: "develop"}}
	if err := store.UpsertProject(ctx, row); err != nil {
		t.Fatal(err)
	}
	before, _, err := store.GetProject(ctx, row.ID)
	if err != nil {
		t.Fatal(err)
	}
	svc := project.New(store)
	state, err := svc.GetAdmission(ctx, "archived")
	if err != nil || state.ProjectID != "archived" || !state.AdmissionPaused || state.Scope != "new_launches_only" || !state.ExistingSessionsMayBeRunning {
		t.Fatalf("archived policy: %+v,%v", state, err)
	}
	if _, err := svc.SetAdmission(ctx, "archived", false); err == nil {
		t.Fatal("archived admission mutation allowed")
	}
	after, _, err := store.GetProject(ctx, row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("archived row changed: before=%+v after=%+v", before, after)
	}
	if _, err := svc.GetAdmission(ctx, "missing"); err == nil {
		t.Fatal("missing project accepted")
	}
}
