// Package custody fences managed native generations. Ticket, scope and capacity
// ownership remain in the external capacity authority; this package records only
// native effects and the evidence needed to prove their cessation.
package custody

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

var (
	ErrFenced      = errors.New("AO_CUSTODY_FENCED")
	ErrUnknown     = errors.New("AO_CUSTODY_UNKNOWN")
	ErrConflict    = errors.New("AO_CUSTODY_REVISION_CONFLICT")
	ErrUnsupported = errors.New("AO_CUSTODY_UNSUPPORTED")
	ErrAdmission   = errors.New("AO_MANAGED_ADMISSION_BLOCKED")
)

// Attempt is immutable identity plus CAS-versioned native execution evidence.
// Phase is execution authority evidence, never the display session status.
type Attempt struct {
	Project               string             `json:"project"`
	SessionID             string             `json:"session_id"`
	AttemptID             string             `json:"attempt_id"`
	Operation             string             `json:"operation"`
	IssueNumber           int                `json:"issue_number"`
	Generation            int64              `json:"generation"`
	Revision              int64              `json:"revision"`
	Phase                 string             `json:"phase"`
	Fence                 string             `json:"fence"`
	RequestID             string             `json:"request_id"`
	RuntimeHandleID       *string            `json:"runtime_handle_id"`
	ClaimPhase            string             `json:"native_claim_phase"`
	LaunchEffectStarted   bool               `json:"launch_effect_started"`
	LaunchEffectDelivered bool               `json:"launch_effect_delivered"`
	ContinuationHash      string             `json:"continuation_sha256"`
	ClaimGeneration       int64              `json:"native_claim_generation"`
	ClaimID               string             `json:"claim_id"`
	IssueBody             string             `json:"issue_body"`
	Route                 *domain.AgentRoute `json:"route"`
	WorkspaceBranch       string             `json:"workspace_branch"`
	CompletedOperations   []string           `json:"completed_operations"`
	Workspace             string             `json:"workspace"`
	OriginalBaseSHA       string             `json:"original_base_sha"`
	ProviderID            string             `json:"provider_id"`
	Transcript            string             `json:"transcript"`
	TurnID                string             `json:"turn_id"`
	Members               []ProcessIdentity  `json:"members"`
	Operations            []string           `json:"operations"`
	Blockers              []string           `json:"blockers"`
	Certificate           *Certificate       `json:"quiescence"`
	PredecessorAttemptID  string             `json:"predecessor_attempt_id"`
	SuccessorAttemptID    string             `json:"successor_attempt_id"`
	OriginAttemptID       string             `json:"origin_attempt_id"`
	OriginGeneration      int64              `json:"origin_generation"`
	Children              []ChildRuntime     `json:"children"`
	Preparations          []ProcessIdentity  `json:"preparations"`
	Callbacks             []ProcessIdentity  `json:"callbacks"`
	InterruptSent         bool               `json:"interrupt_sent"`
	CheckpointSent        bool               `json:"checkpoint_sent"`
	CheckpointTurn        string             `json:"checkpoint_turn"`
	StopHookTurn          string             `json:"stop_hook_turn"`
	ForegroundID          string             `json:"foreground_id"`
	Retired               bool               `json:"retired"`
}

type Certificate struct {
	ID                   string `json:"certificate_id"`
	SHA256               string `json:"certificate_sha256"`
	PreservationComplete bool   `json:"preservation_complete"`
	WriterStopped        bool   `json:"writer_stopped"`
}

// ProcessIdentity is a real kernel execution generation, not just a reusable PID.
type ProcessIdentity struct {
	PID               int       `json:"pid"`
	ParentPID         int       `json:"parent_pid"`
	GroupID           int       `json:"group_id"`
	UID               uint32    `json:"uid"`
	StartSeconds      uint64    `json:"start_seconds"`
	StartMicroseconds uint64    `json:"start_microseconds"`
	AuditToken        [8]uint32 `json:"audit_token"`
	Status            uint32    `json:"status"`
	Executable        string    `json:"executable"`
	Role              string    `json:"role"`
}

func (p ProcessIdentity) Same(q ProcessIdentity) bool {
	return p.PID > 1 && p.PID == q.PID && p.AuditToken == q.AuditToken && p.StartSeconds == q.StartSeconds && p.StartMicroseconds == q.StartMicroseconds
}

// Store implementations must atomically check identity, revision and transition.
// None of these reads may acquire the Manager's project admission lane.
type Store interface {
	ManagedProject(context.Context, string) (bool, error)
	CreateSuccessor(context.Context, Attempt, Attempt) (Attempt, error)
	CreateAttempt(context.Context, Attempt) (Attempt, error)
	UpdateAttempt(context.Context, Attempt, int64) (Attempt, error)
	GetAttempt(context.Context, string, string) (Attempt, bool, error)
	CurrentAttempt(context.Context, string) (Attempt, bool, error)
	ListAttempts(context.Context) ([]Attempt, int64, error)
	GetSession(context.Context, domain.SessionID) (domain.SessionRecord, bool, error)
	GetProject(context.Context, string) (domain.ProjectRecord, bool, error)
	ListProjects(context.Context) ([]domain.ProjectRecord, error)
	ListAllSessions(context.Context) ([]domain.SessionRecord, error)
}

func ValidateAttempt(a Attempt) error {
	if a.Project == "" || a.SessionID == "" || a.AttemptID == "" || a.IssueNumber < 1 || a.ClaimID == "" {
		return fmt.Errorf("%w: incomplete attempt identity", ErrUnknown)
	}
	switch a.Operation {
	case "spawn", "restore", "background_turn":
	default:
		return ErrUnknown
	}
	switch a.Phase {
	case "seed", "preparing", "launching", "running", "quiesced":
	default:
		return ErrUnknown
	}
	switch a.Fence {
	case "", "requested", "draining", "suspended", "handback", "retired":
	default:
		return ErrUnknown
	}
	if a.Fence != "" && a.RequestID == "" {
		return ErrUnknown
	}
	if a.Phase == "quiesced" && (a.Certificate == nil || !a.Certificate.WriterStopped || !a.Certificate.PreservationComplete) {
		return ErrUnknown
	}
	return nil
}

// ValidateTransition forbids revival and proof recycling even after a lost ack.
func ValidateTransition(old, next Attempt) error {
	if err := ValidateAttempt(next); err != nil {
		return err
	}
	if old.Project != next.Project || old.SessionID != next.SessionID || old.AttemptID != next.AttemptID || old.Operation != next.Operation || old.IssueNumber != next.IssueNumber || old.Generation != next.Generation || old.ClaimID != next.ClaimID {
		return ErrConflict
	}
	if old.WorkspaceBranch != "" && next.WorkspaceBranch != old.WorkspaceBranch {
		return ErrConflict
	}
	if old.Workspace != "" && next.Workspace != old.Workspace || old.OriginalBaseSHA != "" && next.OriginalBaseSHA != old.OriginalBaseSHA || old.ProviderID != "" && next.ProviderID != old.ProviderID || old.Transcript != "" && next.Transcript != old.Transcript {
		return ErrConflict
	}
	if old.RuntimeHandleID != nil && (next.RuntimeHandleID == nil || *next.RuntimeHandleID != *old.RuntimeHandleID) {
		return ErrConflict
	}
	if old.ContinuationHash != "" && next.ContinuationHash != old.ContinuationHash || old.LaunchEffectStarted && !next.LaunchEffectStarted || old.LaunchEffectDelivered && !next.LaunchEffectDelivered || next.LaunchEffectDelivered && !next.LaunchEffectStarted || old.ForegroundID != "" && old.ForegroundID != next.ForegroundID {
		return ErrConflict
	}
	if old.ClaimGeneration > next.ClaimGeneration {
		return ErrConflict
	}
	fences := map[string]int{"": 0, "requested": 1, "draining": 2, "suspended": 3, "handback": 4, "retired": 5}
	if fences[next.Fence] < fences[old.Fence] {
		return ErrConflict
	}
	if old.Retired || (old.RequestID != "" && old.RequestID != next.RequestID) {
		return ErrConflict
	}
	if old.Fence != "" && next.Fence == "" {
		return ErrFenced
	}
	if old.Phase == "quiesced" && next.Phase != "quiesced" {
		return ErrConflict
	}
	rank := map[string]int{"seed": 0, "preparing": 1, "launching": 2, "running": 3, "quiesced": 4}
	if rank[next.Phase] < rank[old.Phase] {
		return ErrConflict
	}
	if old.Certificate != nil {
		a, _ := json.Marshal(old.Certificate)
		b, _ := json.Marshal(next.Certificate)
		if string(a) != string(b) {
			return ErrConflict
		}
	}
	return nil
}
