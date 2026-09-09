package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// ---- sessions ----

// CreateSession assigns the per-project identity ("{project}-{num}") and inserts
// the record, returning it with ID populated. The next-num read and the insert
// run on the writer connection under writeMu, so two concurrent creates in the
// same project can't collide on num.
func (s *Store) CreateSession(ctx context.Context, rec domain.SessionRecord) (domain.SessionRecord, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	num, err := s.qw.NextSessionNum(ctx, rec.ProjectID)
	if err != nil {
		return domain.SessionRecord{}, fmt.Errorf("next session num for %s: %w", rec.ProjectID, err)
	}
	rec.ID = domain.SessionID(fmt.Sprintf("%s-%d", rec.ProjectID, num))
	if err := s.qw.InsertSession(ctx, recordToInsert(rec, num)); err != nil {
		return domain.SessionRecord{}, fmt.Errorf("insert session %s: %w", rec.ID, err)
	}
	return rec, nil
}

// UpdateSession writes the full mutable state of an existing session. The
// id/project/num/created_at are immutable and not touched here.
func (s *Store) UpdateSession(ctx context.Context, rec domain.SessionRecord) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.qw.UpdateSession(ctx, recordToUpdate(rec))
}

// RecordAdmissionFailureBeforeWorkspace atomically records the native Spawn
// admission observation only while the exact row remains an unused worker seed.
// The write lock also serializes seed deletion; zero matched rows is not proof.
func (s *Store) RecordAdmissionFailureBeforeWorkspace(ctx context.Context, id domain.SessionID, at time.Time) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	rows, err := s.qw.RecordAdmissionFailureBeforeWorkspace(ctx, gen.RecordAdmissionFailureBeforeWorkspaceParams{
		ID: id, ActivityLastAt: at, UpdatedAt: at,
	})
	if err != nil {
		return false, fmt.Errorf("record admission failure for %s: %w", id, err)
	}
	return rows == 1, nil
}

// ClearSessionLaunchFailureStage invalidates only launch evidence, with a
// checked matched row count. It never rewrites concurrently changed metadata.
func (s *Store) ClearSessionLaunchFailureStage(ctx context.Context, id domain.SessionID, at time.Time) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	rows, err := s.qw.ClearSessionLaunchFailureStage(ctx, gen.ClearSessionLaunchFailureStageParams{ID: id, ObservedAt: at})
	if err != nil {
		return false, fmt.Errorf("clear launch failure for %s: %w", id, err)
	}
	return rows == 1, nil
}

// RenameSession updates only the user-facing display name for an existing
// session. It returns ok=false when the session id does not exist.
func (s *Store) RenameSession(ctx context.Context, id domain.SessionID, displayName string, updatedAt time.Time) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	rows, err := s.qw.RenameSession(ctx, gen.RenameSessionParams{
		ID:          id,
		DisplayName: displayName,
		UpdatedAt:   updatedAt,
	})
	if err != nil {
		return false, fmt.Errorf("rename session %s: %w", id, err)
	}
	return rows > 0, nil
}

// SetSessionPreviewURL updates only the browser preview URL for an existing
// session. It returns ok=false when the session id does not exist. The
// sessions_cdc_update trigger fans out a session_updated CDC event when the
// preview URL actually changes.
func (s *Store) SetSessionPreviewURL(ctx context.Context, id domain.SessionID, previewURL string, updatedAt time.Time) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	rows, err := s.qw.SetSessionPreviewURL(ctx, gen.SetSessionPreviewURLParams{
		ID:         id,
		PreviewURL: previewURL,
		UpdatedAt:  updatedAt,
	})
	if err != nil {
		return false, fmt.Errorf("set preview url for session %s: %w", id, err)
	}
	return rows > 0, nil
}

// SetWorkerSchedulingHold persists one immutable scheduling hold per session.
// Repeated calls return the first hold unchanged.
func (s *Store) SetWorkerSchedulingHold(ctx context.Context, id domain.SessionID, heldAt time.Time) (domain.WorkerSchedulingHold, error) {
	if err := s.RatchetCompatibility(2); err != nil {
		return domain.WorkerSchedulingHold{}, fmt.Errorf("set worker scheduling hold for session %s: compatibility: %w", id, err)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	row, err := s.qw.SetWorkerSchedulingHold(ctx, gen.SetWorkerSchedulingHoldParams{
		SessionID: string(id),
		HeldAt:    heldAt,
	})
	if err != nil {
		return domain.WorkerSchedulingHold{}, fmt.Errorf("set worker scheduling hold for session %s: %w", id, err)
	}
	return workerHoldFromRow(row)
}

// GetWorkerSchedulingHold returns the durable hold for a session, or ok=false
// when the session has no hold.
func (s *Store) GetWorkerSchedulingHold(ctx context.Context, id domain.SessionID) (domain.WorkerSchedulingHold, bool, error) {
	row, err := s.qr.GetWorkerSchedulingHold(ctx, string(id))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.WorkerSchedulingHold{}, false, nil
	}
	if err != nil {
		return domain.WorkerSchedulingHold{}, false, fmt.Errorf("get worker scheduling hold for session %s: %w", id, err)
	}
	hold, err := workerHoldFromRow(row)
	return hold, err == nil, err
}

func workerHoldFromRow(row gen.WorkerSchedulingHold) (domain.WorkerSchedulingHold, error) {
	hold := domain.WorkerSchedulingHold{SessionID: domain.SessionID(row.SessionID), HeldAt: row.HeldAt}
	if !row.RetiredAt.Valid && !row.RetirementProjectID.Valid && !row.RetirementTicket.Valid && !row.RetirementSessionUpdatedAt.Valid {
		return hold, nil
	}
	if !row.RetiredAt.Valid || !row.RetirementProjectID.Valid || !row.RetirementTicket.Valid || !row.RetirementSessionUpdatedAt.Valid || row.RetirementProjectID.String == "" || row.RetirementTicket.String == "" || row.RetiredAt.Time.IsZero() || row.RetirementSessionUpdatedAt.Time.IsZero() {
		return hold, errors.New("worker retirement fact is incomplete")
	}
	hold.Retirement = &domain.WorkerRetirement{ProjectID: domain.ProjectID(row.RetirementProjectID.String), Ticket: domain.IssueID(row.RetirementTicket.String), SessionUpdatedAt: row.RetirementSessionUpdatedAt.Time, RetiredAt: row.RetiredAt.Time}
	return hold, nil
}

// SetWorkerRetirement adds the immutable fact only while the exact session and
// its PR obligations still match. The session row and all workspace rows remain
// untouched. Existing protocol-2 holds already fence every older restore path.
func (s *Store) SetWorkerRetirement(ctx context.Context, expected domain.SessionRecord, fact domain.WorkerRetirement) (domain.WorkerSchedulingHold, error) {
	if err := s.RatchetCompatibility(2); err != nil {
		return domain.WorkerSchedulingHold{}, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var hold domain.WorkerSchedulingHold
	err := s.inTx(ctx, "record worker retirement", func(q *gen.Queries) error {
		row, err := q.GetSession(ctx, expected.ID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(rowToRecord(row), expected) || !expected.IsTerminated || expected.Kind != domain.KindWorker || expected.Activity.State != domain.ActivityExited || fact.ProjectID != expected.ProjectID || !fact.SessionUpdatedAt.Equal(expected.UpdatedAt) || fact.Ticket == "" || fact.RetiredAt.Before(expected.UpdatedAt) {
			return errors.New("worker retirement session changed")
		}
		prs, err := q.ListPRFactsBySession(ctx, expected.ID)
		if err != nil {
			return err
		}
		for _, pr := range prs {
			if pr.PRState != domain.PRStateClosed || pr.ReviewComments {
				return errors.New("worker retirement has PR duty")
			}
		}
		held, err := q.GetWorkerSchedulingHold(ctx, string(expected.ID))
		if err != nil {
			return err
		}
		hold, err = workerHoldFromRow(held)
		if err != nil {
			return err
		}
		if fact.RetiredAt.Before(hold.HeldAt) {
			return errors.New("worker retirement predates hold")
		}
		if hold.Retirement != nil {
			if !reflect.DeepEqual(*hold.Retirement, fact) {
				return errors.New("worker retirement already has another immutable fact")
			}
			return nil
		}
		held, err = q.SetWorkerRetirement(ctx, gen.SetWorkerRetirementParams{SessionID: string(expected.ID), RetirementProjectID: sql.NullString{String: string(fact.ProjectID), Valid: true}, RetirementTicket: sql.NullString{String: string(fact.Ticket), Valid: true}, RetirementSessionUpdatedAt: sql.NullTime{Time: fact.SessionUpdatedAt, Valid: true}, RetiredAt: sql.NullTime{Time: fact.RetiredAt, Valid: true}})
		if err != nil {
			return err
		}
		hold, err = workerHoldFromRow(held)
		return err
	})
	return hold, err
}

// DeleteSession removes a session row, but only if it is still in seed state
// (no workspace, no runtime handle, no agent session id, no prompt, and not
// already terminated). Rows that have observable spawn output are immutable
// to preserve the no-resurrection guarantee — for those, callers fall back to
// MarkTerminated (lifecycle.Manager) instead.
//
// The deletion runs in a transaction. It first probes seed state with
// SessionIsSeed; only if that returns true does it clear the session's
// change_log rows (required because change_log FKs sessions(id) without
// ON DELETE CASCADE) and then delete the session row. For live or absent
// sessions the transaction commits with no rows touched — critically, the
// session_created / session_updated CDC events for live sessions are NOT
// destroyed when callers (e.g. RollbackSpawn's delete-then-kill fallback)
// invoke DeleteSession on a fully-spawned row.
//
// Returns deleted=true when a seed row was removed; deleted=false when the
// session id did not match a seed row (either it never existed, or it had
// already progressed past seed state). The latter case is benign — the caller
// should fall back to MarkTerminated.
func (s *Store) DeleteSession(ctx context.Context, id domain.SessionID) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin delete seed session: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	q := s.qw.WithTx(tx)

	isSeed, err := q.SessionIsSeed(ctx, id)
	if err != nil {
		return false, fmt.Errorf("delete seed session: probe seed state for %s: %w", id, err)
	}
	if !isSeed {
		// Commit the empty tx so we don't leak a transaction. Critically, do
		// NOT touch change_log here — for a live session that contains real
		// session_created / session_updated CDC events.
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("delete seed session: commit no-op: %w", err)
		}
		return false, nil
	}

	// Drop change_log rows for this session id first so the FK doesn't reject
	// the session DELETE. We do not touch project-level events (session_id IS
	// NULL) — those belong to the project, not this session. Both this DELETE
	// and the session DELETE below run via raw ExecContext to sidestep sqlc
	// 1.31's SQLite-parser bug, which strips trailing `?` placeholders and
	// string literals from DELETE statements (see queries/changelog.sql and
	// queries/sessions.sql for the documented workaround context).
	if _, err := tx.ExecContext(ctx, `DELETE FROM change_log WHERE session_id = ?`, id); err != nil {
		return false, fmt.Errorf("delete seed session: clear change log for %s: %w", id, err)
	}
	res, err := tx.ExecContext(ctx, `
DELETE FROM sessions
WHERE id = ?
  AND is_terminated = 0
  AND workspace_path = ''
  AND runtime_handle_id = ''
  AND agent_session_id = ''
  AND prompt = ''`, id)
	if err != nil {
		return false, fmt.Errorf("delete seed session %s: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("delete seed session %s: rows affected: %w", id, err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("delete seed session: commit: %w", err)
	}
	return n > 0, nil
}

// GetSession returns the full record for a session, or ok=false if absent.
func (s *Store) GetSession(ctx context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	row, err := s.qr.GetSession(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.SessionRecord{}, false, nil
	}
	if err != nil {
		return domain.SessionRecord{}, false, fmt.Errorf("get session %s: %w", id, err)
	}
	return rowToRecord(row), true, nil
}

// ListSessions returns every session in a project, ordered by num.
func (s *Store) ListSessions(ctx context.Context, project domain.ProjectID) ([]domain.SessionRecord, error) {
	rows, err := s.qr.ListSessionsByProject(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("list sessions for %s: %w", project, err)
	}
	return mapSessionRows(rows), nil
}

// ListAllSessions returns every session across all projects.
func (s *Store) ListAllSessions(ctx context.Context) ([]domain.SessionRecord, error) {
	rows, err := s.qr.ListAllSessions(ctx)
	if err != nil {
		return nil, fmt.Errorf("list all sessions: %w", err)
	}
	return mapSessionRows(rows), nil
}

func mapSessionRows(rows []gen.Session) []domain.SessionRecord {
	out := make([]domain.SessionRecord, 0, len(rows))
	for _, r := range rows {
		out = append(out, rowToRecord(r))
	}
	return out
}

func rowToRecord(row gen.Session) domain.SessionRecord {
	var requestedRoute *domain.AgentRoute
	if row.RequestedHarness != "" {
		requestedRoute = &domain.AgentRoute{
			Harness:         domain.AgentHarness(row.RequestedHarness),
			Model:           row.RequestedModel,
			ReasoningEffort: domain.ReasoningEffort(row.RequestedReasoningEffort),
		}
	}
	var launchRoute *domain.AgentLaunchRoute
	if row.LaunchRouteRecorded {
		launchRoute = &domain.AgentLaunchRoute{
			Harness:         row.Harness,
			Model:           row.LaunchModel,
			ReasoningEffort: domain.ReasoningEffort(row.LaunchReasoningEffort),
		}
	}
	return domain.SessionRecord{
		ID:          row.ID,
		ProjectID:   row.ProjectID,
		IssueID:     row.IssueID,
		Kind:        row.Kind,
		Harness:     row.Harness,
		DisplayName: row.DisplayName,
		Activity: domain.Activity{
			State:          row.ActivityState,
			LastActivityAt: row.ActivityLastAt,
		},
		FirstSignalAt:      nullTimeToTime(row.FirstSignalAt),
		IsTerminated:       row.IsTerminated,
		LaunchFailureStage: row.LaunchFailureStage,
		Metadata: domain.SessionMetadata{
			Branch:          row.Branch,
			WorkspacePath:   row.WorkspacePath,
			RuntimeHandleID: row.RuntimeHandleID,
			AgentSessionID:  row.AgentSessionID,
			Prompt:          row.Prompt,
			RequestedRoute:  requestedRoute,
			LaunchRoute:     launchRoute,
			PreviewURL:      row.PreviewURL,
			PreviewRevision: row.PreviewRevision,
		},
		CreatedAt: row.CreatedAt,
		UpdatedAt: row.UpdatedAt,
	}
}

func recordToInsert(rec domain.SessionRecord, num int64) gen.InsertSessionParams {
	activity := normalActivity(rec.Activity, rec.CreatedAt)
	requestedHarness, requestedModel, requestedEffort := requestedRouteColumns(rec.Metadata.RequestedRoute)
	launchModel, launchEffort, launchRecorded := launchRouteColumns(rec.Metadata.LaunchRoute)
	return gen.InsertSessionParams{
		ID:                       rec.ID,
		ProjectID:                rec.ProjectID,
		Num:                      num,
		IssueID:                  rec.IssueID,
		Kind:                     rec.Kind,
		Harness:                  rec.Harness,
		DisplayName:              rec.DisplayName,
		ActivityState:            activity.State,
		ActivityLastAt:           activity.LastActivityAt,
		FirstSignalAt:            timeToNullTime(rec.FirstSignalAt),
		IsTerminated:             rec.IsTerminated,
		Branch:                   rec.Metadata.Branch,
		WorkspacePath:            rec.Metadata.WorkspacePath,
		RuntimeHandleID:          rec.Metadata.RuntimeHandleID,
		AgentSessionID:           rec.Metadata.AgentSessionID,
		Prompt:                   rec.Metadata.Prompt,
		PreviewURL:               rec.Metadata.PreviewURL,
		PreviewRevision:          rec.Metadata.PreviewRevision,
		RequestedHarness:         requestedHarness,
		RequestedModel:           requestedModel,
		RequestedReasoningEffort: requestedEffort,
		LaunchModel:              launchModel,
		LaunchReasoningEffort:    launchEffort,
		LaunchRouteRecorded:      launchRecorded,
		LaunchFailureStage:       rec.LaunchFailureStage,
		CreatedAt:                rec.CreatedAt,
		UpdatedAt:                rec.UpdatedAt,
	}
}

func recordToUpdate(rec domain.SessionRecord) gen.UpdateSessionParams {
	activity := normalActivity(rec.Activity, rec.UpdatedAt)
	requestedHarness, requestedModel, requestedEffort := requestedRouteColumns(rec.Metadata.RequestedRoute)
	launchModel, launchEffort, launchRecorded := launchRouteColumns(rec.Metadata.LaunchRoute)
	return gen.UpdateSessionParams{
		ID:                       rec.ID,
		IssueID:                  rec.IssueID,
		Kind:                     rec.Kind,
		Harness:                  rec.Harness,
		DisplayName:              rec.DisplayName,
		ActivityState:            activity.State,
		ActivityLastAt:           activity.LastActivityAt,
		FirstSignalAt:            timeToNullTime(rec.FirstSignalAt),
		IsTerminated:             rec.IsTerminated,
		Branch:                   rec.Metadata.Branch,
		WorkspacePath:            rec.Metadata.WorkspacePath,
		RuntimeHandleID:          rec.Metadata.RuntimeHandleID,
		AgentSessionID:           rec.Metadata.AgentSessionID,
		Prompt:                   rec.Metadata.Prompt,
		PreviewURL:               rec.Metadata.PreviewURL,
		PreviewRevision:          rec.Metadata.PreviewRevision,
		RequestedHarness:         requestedHarness,
		RequestedModel:           requestedModel,
		RequestedReasoningEffort: requestedEffort,
		LaunchModel:              launchModel,
		LaunchReasoningEffort:    launchEffort,
		LaunchRouteRecorded:      launchRecorded,
		LaunchFailureStage:       rec.LaunchFailureStage,
		UpdatedAt:                rec.UpdatedAt,
	}
}

func requestedRouteColumns(route *domain.AgentRoute) (harness, model, effort string) {
	if route == nil {
		return "", "", ""
	}
	return string(route.Harness), route.Model, string(route.ReasoningEffort)
}

func launchRouteColumns(route *domain.AgentLaunchRoute) (model, effort string, recorded bool) {
	if route == nil {
		return "", "", false
	}
	return route.Model, string(route.ReasoningEffort), true
}

// nullTimeToTime / timeToNullTime bridge the nullable first_signal_at column
// to the domain's zero-time convention (zero = no signal received yet).
func nullTimeToTime(t sql.NullTime) time.Time {
	if !t.Valid {
		return time.Time{}
	}
	return t.Time
}

func timeToNullTime(t time.Time) sql.NullTime {
	if t.IsZero() {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: t, Valid: true}
}

func normalActivity(a domain.Activity, fallback time.Time) domain.Activity {
	if a.State == "" {
		a.State = domain.ActivityIdle
	}
	if a.LastActivityAt.IsZero() {
		a.LastActivityAt = fallback
	}
	if a.LastActivityAt.IsZero() {
		a.LastActivityAt = time.Now().UTC()
	}
	return a
}
