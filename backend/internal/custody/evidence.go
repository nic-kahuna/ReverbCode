package custody

import (
	"context"
	"fmt"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/bootguard"
)

type EvidenceRequest struct {
	Schema    string  `json:"schema"`
	DataDir   string  `json:"data_dir"`
	Kind      string  `json:"kind"`
	Project   *string `json:"project"`
	SessionID *string `json:"session_id"`
	AttemptID *string `json:"attempt_id"`
}
type ProjectEvidence struct {
	Project           string `json:"project"`
	Repository        string `json:"repository"`
	CanonicalCheckout string `json:"canonical_checkout"`
}
type AttemptEvidence struct {
	Project         string       `json:"project"`
	SessionID       string       `json:"session_id"`
	AttemptID       string       `json:"attempt_id"`
	Operation       string       `json:"operation"`
	IssueNumber     int          `json:"issue_number"`
	Phase           string       `json:"phase"`
	RuntimeHandleID *string      `json:"runtime_handle_id"`
	Quiescence      *Certificate `json:"quiescence"`
}
type Evidence struct {
	Schema         string            `json:"schema"`
	DataDir        string            `json:"data_dir"`
	Protocol       int               `json:"protocol"`
	Generation     int64             `json:"generation"`
	Projects       []ProjectEvidence `json:"projects"`
	Attempts       []AttemptEvidence `json:"attempts"`
	LegacySessions []LegacyEvidence  `json:"legacy_sessions"`
}

type LegacyEvidence struct {
	Project         string  `json:"project"`
	SessionID       string  `json:"session_id"`
	IssueNumber     *int    `json:"issue_number"`
	RuntimeHandleID *string `json:"runtime_handle_id"`
	Reason          string  `json:"reason"`
}

// Evidence reads durable facts without taking any Manager/project/session lane.
// The capacity helper calls it while Manager may be waiting for that helper.
func (c *Coordinator) Evidence(ctx context.Context, q EvidenceRequest) (Evidence, error) {
	out := Evidence{Schema: "ao-admission-evidence/v1", DataDir: c.DataDir, Protocol: 2, Projects: []ProjectEvidence{}, Attempts: []AttemptEvidence{}, LegacySessions: []LegacyEvidence{}}
	if q.Schema != "ao-admission-evidence-request/v1" || q.DataDir != c.DataDir {
		return out, ErrUnknown
	}
	inspection, err := bootguard.Inspect(c.DataDir)
	if err != nil {
		return out, err
	}
	if inspection.RequiredProtocol != 2 {
		return out, ErrUnsupported
	}
	if q.Kind == "attempt" {
		if q.Project == nil || q.SessionID == nil || q.AttemptID == nil || *q.Project == "" || *q.SessionID == "" || *q.AttemptID == "" {
			return out, ErrUnknown
		}
	} else if q.Kind != "inventory" || q.Project != nil || q.SessionID != nil || q.AttemptID != nil {
		return out, ErrUnknown
	}
	projects, err := c.Store.ListProjects(ctx)
	if err != nil {
		return out, err
	}
	for _, p := range projects {
		if q.Kind == "attempt" && p.ID != *q.Project {
			continue
		}
		repo := p.Config.TrackerIntake.Repo
		if repo == "" {
			repo = strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(p.RepoOriginURL, "https://github.com/"), "git@github.com:"), ".git")
		}
		if repo == "" || !strings.Contains(repo, "/") || p.Path == "" {
			return out, fmt.Errorf("%w: incomplete project repository identity", ErrUnknown)
		}
		out.Projects = append(out.Projects, ProjectEvidence{Project: p.ID, Repository: repo, CanonicalCheckout: p.Path})
	}
	attempts, generation, err := c.Store.ListAttempts(ctx)
	if err != nil {
		return out, err
	}
	if generation < 1 {
		return out, ErrUnknown
	}
	out.Generation = generation
	known := map[string]bool{}
	for _, a := range attempts {
		known[a.SessionID] = true
	}
	if q.Kind == "inventory" {
		sessions, e := c.Store.ListAllSessions(ctx)
		if e != nil {
			return out, e
		}
		for _, rec := range sessions {
			if known[string(rec.ID)] || (rec.IsTerminated && rec.Metadata.WorkspacePath == "" && rec.Metadata.RuntimeHandleID == "") {
				continue
			}
			var issue *int
			project, exists, e := c.Store.GetProject(ctx, string(rec.ProjectID))
			if e != nil || !exists {
				return out, ErrUnknown
			}
			if n, e := IssueNumber(string(rec.IssueID), project.Config.TrackerIntake.Repo); e == nil {
				issue = &n
			}
			var handle *string
			if rec.Metadata.RuntimeHandleID != "" {
				v := rec.Metadata.RuntimeHandleID
				handle = &v
			}
			out.LegacySessions = append(out.LegacySessions, LegacyEvidence{Project: string(rec.ProjectID), SessionID: string(rec.ID), IssueNumber: issue, RuntimeHandleID: handle, Reason: "untracked_generation"})
		}
	}
	for _, a := range attempts {
		if a.Retired {
			continue
		}
		if q.Kind == "attempt" && (a.Project != *q.Project || a.SessionID != *q.SessionID || a.AttemptID != *q.AttemptID) {
			continue
		}
		if a.Phase == "running" {
			if err = c.verifyRunningIdentity(ctx, a); err != nil {
				return out, err
			}
		}
		if a.Phase == "quiesced" {
			// A certificate is never caller input. The production verifier checks
			// its immutable bytes and the stopped identities before exposing it.
			if err = c.VerifyStoredCertificate(ctx, a); err != nil {
				return out, err
			}
		}
		out.Attempts = append(out.Attempts, AttemptEvidence{Project: a.Project, SessionID: a.SessionID, AttemptID: a.AttemptID, Operation: a.Operation, IssueNumber: a.IssueNumber, Phase: a.Phase, RuntimeHandleID: a.RuntimeHandleID, Quiescence: a.Certificate})
	}
	if q.Kind == "attempt" && (len(out.Projects) != 1 || len(out.Attempts) != 1) {
		return out, ErrUnknown
	}
	return out, nil
}
