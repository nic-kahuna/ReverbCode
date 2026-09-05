package review

import (
	"context"
	"errors"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/admission"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func TestPausedAdmissionDoesNotLaunchOrNotifyReviewer(t *testing.T) {
	for _, alive := range []bool{false, true} {
		store := &fakeStore{}
		launcher := &fakeLauncher{alive: alive}
		e := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{cfg: domain.ProjectConfig{AdmissionPaused: true}}, launcher)
		if _, err := e.Trigger(context.Background(), "mer-1"); !errors.Is(err, admission.ErrPaused) {
			t.Fatal(err)
		}
		if launcher.spawned || launcher.notified || launcher.stoppedHandle != "" || len(store.runs) != 0 {
			t.Fatal("review changed during pause")
		}
	}
}
