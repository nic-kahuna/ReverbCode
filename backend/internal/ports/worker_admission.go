package ports

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// WorkerAdmissionOperation identifies the native lifecycle boundary whose
// scoped ownership must be acquired or verified before worker effects begin.
type WorkerAdmissionOperation string

const (
	WorkerAdmissionSpawn   WorkerAdmissionOperation = "spawn"
	WorkerAdmissionRestore WorkerAdmissionOperation = "restore"
)

// WorkerAdmissionRequest binds one native worker identity to its managed
// project, repository, issue, and complete launch route.
type WorkerAdmissionRequest struct {
	ProjectID  domain.ProjectID
	Repository string
	SessionID  domain.SessionID
	IssueID    domain.IssueID
	Operation  WorkerAdmissionOperation
	Route      domain.AgentRoute
}

// WorkerAdmissionResult carries the authoritative issue body bound by the
// accepted ownership scope. Spawn persists and launches this exact body.
type WorkerAdmissionResult struct {
	IssueBody string
}

// WorkerAdmitter is the outbound boundary for scoped native worker ownership.
type WorkerAdmitter interface {
	AdmitWorker(context.Context, WorkerAdmissionRequest) (WorkerAdmissionResult, error)
}
