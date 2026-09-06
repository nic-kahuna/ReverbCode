package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/bootguard"
	"github.com/aoagents/agent-orchestrator/backend/internal/custody"
)

func (s *Store) ManagedProject(ctx context.Context, id string) (bool, error) {
	var n int
	err := s.readDB.QueryRowContext(ctx, "SELECT count(*) FROM custody_projects WHERE project_id=?", id).Scan(&n)
	return n == 1, err
}

// ActivateCustody requires the real held startup guard, not a caller boolean.
// It is used only before mutation lanes start; a failed ratchet inserts nothing.
func (s *Store) ActivateCustody(ctx context.Context, guard *bootguard.Guard, projects []string) error {
	if guard == nil || len(projects) == 0 {
		return custody.ErrUnknown
	}
	var databasePath string
	if err := s.readDB.QueryRowContext(ctx, "SELECT file FROM pragma_database_list WHERE name='main'").Scan(&databasePath); err != nil {
		return err
	}
	actual, err := filepath.EvalSymlinks(databasePath)
	if err != nil {
		return err
	}
	expected, err := filepath.EvalSymlinks(filepath.Join(guard.DataDir(), "ao.db"))
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("%w: startup lock and database identities differ", custody.ErrConflict)
	}
	for _, id := range projects {
		p, ok, err := s.GetProject(ctx, id)
		if err != nil {
			return err
		}
		if !ok || !p.Config.AdmissionPaused || p.ConfigDecodeError != "" {
			return fmt.Errorf("%w: activation requires known paused project %s", custody.ErrUnknown, id)
		}
	}
	if err := guard.Ratchet(2); err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO custody_revision(id,revision) VALUES(1,1)"); err != nil {
		return err
	}
	for _, id := range projects {
		if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO custody_projects(project_id,activated_at) VALUES(?,?)", id, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE custody_revision SET revision=revision+1 WHERE id=1"); err != nil {
		return err
	}
	return tx.Commit()
}

func decodeAttempt(row interface{ Scan(...any) error }) (custody.Attempt, bool, error) {
	var doc string
	if err := row.Scan(&doc); errors.Is(err, sql.ErrNoRows) {
		return custody.Attempt{}, false, nil
	} else if err != nil {
		return custody.Attempt{}, false, err
	}
	var a custody.Attempt
	if err := json.Unmarshal([]byte(doc), &a); err != nil {
		return a, false, err
	}
	if err := custody.ValidateAttempt(a); err != nil {
		return a, false, err
	}
	return a, true, nil
}
func (s *Store) GetAttempt(ctx context.Context, session, attempt string) (custody.Attempt, bool, error) {
	return decodeAttempt(s.readDB.QueryRowContext(ctx, "SELECT document FROM custody_attempts WHERE session_id=? AND attempt_id=?", session, attempt))
}
func (s *Store) CurrentAttempt(ctx context.Context, session string) (custody.Attempt, bool, error) {
	return decodeAttempt(s.readDB.QueryRowContext(ctx, "SELECT document FROM custody_attempts WHERE session_id=? AND retired=0", session))
}

func (s *Store) CreateAttempt(ctx context.Context, a custody.Attempt) (custody.Attempt, error) {
	if err := custody.ValidateAttempt(a); err != nil {
		return a, err
	}
	if a.Phase != "seed" || a.Fence != "" || a.Retired || a.Certificate != nil || a.RequestID != "" || a.ForegroundID != "" || a.PredecessorAttemptID != "" || a.SuccessorAttemptID != "" {
		return a, custody.ErrConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return a, err
	}
	defer func() { _ = tx.Rollback() }()
	var sessionProject string
	if err = tx.QueryRowContext(ctx, "SELECT project_id FROM sessions WHERE id=?", a.SessionID).Scan(&sessionProject); err != nil {
		return a, err
	}
	if sessionProject != a.Project {
		return a, custody.ErrConflict
	}
	if err = tx.QueryRowContext(ctx, "UPDATE custody_revision SET revision=revision+1 WHERE id=1 RETURNING revision").Scan(&a.Generation); err != nil {
		return a, err
	}
	a.Revision = 1
	doc, err := json.Marshal(a)
	if err != nil {
		return a, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO custody_attempts(attempt_id,session_id,project_id,generation,revision,document) VALUES(?,?,?,?,?,?)", a.AttemptID, a.SessionID, a.Project, a.Generation, a.Revision, string(doc))
	if err != nil {
		return a, err
	}
	return a, tx.Commit()
}
func (s *Store) UpdateAttempt(ctx context.Context, a custody.Attempt, expected int64) (custody.Attempt, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return a, err
	}
	defer func() { _ = tx.Rollback() }()
	old, ok, err := decodeAttempt(tx.QueryRowContext(ctx, "SELECT document FROM custody_attempts WHERE attempt_id=?", a.AttemptID))
	if err != nil {
		return a, err
	}
	if !ok || old.Revision != expected {
		return a, custody.ErrConflict
	}
	if err = custody.ValidateTransition(old, a); err != nil {
		return a, err
	}
	a.Revision = expected + 1
	doc, err := json.Marshal(a)
	if err != nil {
		return a, err
	}
	_, err = tx.ExecContext(ctx, "UPDATE custody_attempts SET revision=?,retired=?,document=? WHERE attempt_id=? AND revision=?", a.Revision, a.Retired, string(doc), a.AttemptID, expected)
	if err != nil {
		return a, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE custody_revision SET revision=revision+1 WHERE id=1"); err != nil {
		return a, err
	}
	return a, tx.Commit()
}
func (s *Store) ListAttempts(ctx context.Context) ([]custody.Attempt, int64, error) {
	tx, err := s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = tx.Rollback() }()
	var generation int64
	if err = tx.QueryRowContext(ctx, "SELECT revision FROM custody_revision WHERE id=1").Scan(&generation); errors.Is(err, sql.ErrNoRows) {
		return []custody.Attempt{}, 0, nil
	} else if err != nil {
		return nil, 0, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT document FROM custody_attempts ORDER BY generation")
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	out := []custody.Attempt{}
	for rows.Next() {
		a, _, e := decodeAttempt(rows)
		if e != nil {
			return nil, 0, e
		}
		out = append(out, a)
	}
	if err = rows.Err(); err != nil {
		return nil, 0, err
	}
	return out, generation, tx.Commit()
}

// CreateSuccessor is the one retirement/seed transaction; there is no crash
// window with an unfenced old provider or two current execution generations.
func (s *Store) CreateSuccessor(ctx context.Context, old, next custody.Attempt) (custody.Attempt, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return next, err
	}
	defer func() { _ = tx.Rollback() }()
	actual, ok, err := decodeAttempt(tx.QueryRowContext(ctx, "SELECT document FROM custody_attempts WHERE attempt_id=? AND retired=0", old.AttemptID))
	if err != nil {
		return next, err
	}
	if !ok || !reflect.DeepEqual(actual, old) || actual.Phase != "quiesced" || actual.Certificate == nil || actual.RequestID == "" {
		return next, custody.ErrConflict
	}
	if next.Project != actual.Project || next.SessionID != actual.SessionID || next.PredecessorAttemptID != actual.AttemptID || next.AttemptID == actual.AttemptID || next.ClaimID != actual.ClaimID || next.IssueNumber != actual.IssueNumber || next.ClaimGeneration != actual.ClaimGeneration || next.Phase != "seed" || next.Fence != "" || next.Retired || next.Certificate != nil || next.RequestID != "" || next.ForegroundID != "" || next.SuccessorAttemptID != "" || next.ProviderID != actual.ProviderID || next.Workspace != actual.Workspace || next.WorkspaceBranch != actual.WorkspaceBranch || next.OriginalBaseSHA != actual.OriginalBaseSHA || next.Transcript != actual.Transcript || !reflect.DeepEqual(next.RuntimeHandleID, actual.RuntimeHandleID) || !reflect.DeepEqual(next.Route, actual.Route) || next.InterruptSent || next.CheckpointSent || len(next.Operations) != 0 || !reflect.DeepEqual(next.Children, actual.Children) || len(next.Callbacks) != 0 || len(next.Preparations) != 0 {
		return next, custody.ErrConflict
	}
	if err = custody.ValidateAttempt(next); err != nil {
		return next, err
	}
	old = actual
	old.Retired = true
	old.Fence = "retired"
	old.SuccessorAttemptID = next.AttemptID
	old.Revision++
	doc, err := json.Marshal(old)
	if err != nil {
		return next, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE custody_attempts SET revision=?,retired=1,document=? WHERE attempt_id=?", old.Revision, string(doc), old.AttemptID); err != nil {
		return next, err
	}
	if err = tx.QueryRowContext(ctx, "UPDATE custody_revision SET revision=revision+1 WHERE id=1 RETURNING revision").Scan(&next.Generation); err != nil {
		return next, err
	}
	next.Revision = 1
	doc, err = json.Marshal(next)
	if err != nil {
		return next, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO custody_attempts(attempt_id,session_id,project_id,generation,revision,document) VALUES(?,?,?,?,?,?)", next.AttemptID, next.SessionID, next.Project, next.Generation, next.Revision, string(doc)); err != nil {
		return next, err
	}
	return next, tx.Commit()
}
