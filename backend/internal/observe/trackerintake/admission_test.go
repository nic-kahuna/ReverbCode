package trackerintake

import (
	"context"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func TestPollSkipsPausedAndUnknownAdmission(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		project := domain.ProjectRecord{ID: "demo", RepoOriginURL: "https://github.com/acme/demo.git", Config: domain.ProjectConfig{AdmissionPaused: !unknown, TrackerIntake: domain.TrackerIntakeConfig{Enabled: true, Assignee: "alice"}}}
		if unknown {
			project.ConfigDecodeError = "invalid config"
		}
		store := &fakeStore{projects: []domain.ProjectRecord{project}}
		spawner := &fakeSpawner{}
		if err := New(singleResolver(&fakeTracker{}), store, spawner, Config{Logger: discardLogger()}).Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(spawner.calls) != 0 {
			t.Fatal("intake launched while admission unavailable")
		}
	}
}
