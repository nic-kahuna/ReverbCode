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
