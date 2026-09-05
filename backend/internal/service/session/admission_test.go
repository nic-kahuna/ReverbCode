package session

import (
	"context"
	"errors"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apierr"
)

func TestPausedProjectNeverRetiresOrchestratorBeforeRejectedLaunch(t *testing.T) {
	st := newFakeStore()
	st.projects["mer"] = domain.ProjectRecord{ID: "mer", Config: domain.ProjectConfig{AdmissionPaused: true}}
	st.sessions["mer-1"] = domain.SessionRecord{ID: "mer-1", ProjectID: "mer", Kind: domain.KindOrchestrator}
	fc := &fakeCommander{}
	svc := NewWithDeps(Deps{Manager: fc, Store: st})
	_, err := svc.SpawnOrchestrator(context.Background(), "mer", true)
	var apiErr *apierr.Error
	if !errors.As(err, &apiErr) || apiErr.Code != "PROJECT_ADMISSION_PAUSED" {
		t.Fatalf("error %v", err)
	}
	if fc.spawned || len(fc.retired) != 0 || len(fc.sent) != 0 {
		t.Fatal("paused replacement altered active orchestrator")
	}
}
