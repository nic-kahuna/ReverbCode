package session

import (
	"context"
	"fmt"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// sessionRecovery derives the managed recovery inventory from durable worktree
// journal facts. The marker is authoritative even if mutable project config is
// later removed, damaged, or rewritten by an older binary. Preserving every row
// makes exact identity and partial materialization visible while restore
// preflight independently decides whether any mutation is safe.
func (s *Service) sessionRecovery(ctx context.Context, rec domain.SessionRecord) (*domain.SessionRecovery, error) {
	rows, err := s.store.ListSessionWorktrees(ctx, rec.ID)
	if err != nil {
		return nil, fmt.Errorf("recovery worktrees %s: %w", rec.ID, err)
	}
	if !hasManagedRecoveryEvidence(rows) {
		return nil, nil
	}

	worktrees := make([]domain.RecoveryWorktree, 0, len(rows))
	runtimeState := domain.SessionRecoveryRuntimeAbsent
	if !rec.IsTerminated {
		runtimeState = domain.SessionRecoveryRuntimeUnknown
	}
	for _, row := range rows {
		if row.State == domain.SessionWorktreeStatePreservedPartial {
			runtimeState = domain.SessionRecoveryRuntimeUnknown
		}
		worktrees = append(worktrees, domain.RecoveryWorktree{
			RepoName:     row.RepoName,
			Branch:       row.Branch,
			BaseSHA:      row.BaseSHA,
			WorktreePath: row.WorktreePath,
			PreservedRef: row.PreservedRef,
			State:        row.State,
		})
	}
	return &domain.SessionRecovery{
		State:                domain.SessionRecoveryAwaitingRecovery,
		Policy:               domain.StartupRestorePreserveOnly,
		RuntimeState:         runtimeState,
		ProviderSessionSaved: strings.TrimSpace(rec.Metadata.AgentSessionID) != "",
		Worktrees:            worktrees,
	}, nil
}

func hasManagedRecoveryEvidence(rows []domain.SessionWorktreeRecord) bool {
	for _, row := range rows {
		switch row.State {
		case domain.SessionWorktreeStatePreserved,
			domain.SessionWorktreeStatePreservedRemoved,
			domain.SessionWorktreeStatePreservedPartial:
			return true
		}
	}
	return false
}
