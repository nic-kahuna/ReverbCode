package sqlite

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestMigration0025AddsWorkerSchedulingHoldsForExistingSessions(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "ao.db")+pragmas)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	upTo(t, db, 24)
	if _, err := db.Exec(
		`INSERT INTO projects (id, path, repo_origin_url, display_name, registered_at)
		 VALUES ('demo', '/tmp/demo', 'https://github.com/example/demo.git', 'Demo', '2026-09-05T00:00:00Z')`,
	); err != nil {
		t.Fatalf("seed project: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO sessions (
			id, project_id, num, issue_id, kind, harness, activity_state,
			activity_last_at, branch, workspace_path, runtime_handle_id,
			agent_session_id, prompt, created_at, updated_at
		) VALUES (
			'demo-1', 'demo', 1, '17', 'worker', 'codex', 'idle',
			'2026-09-05T00:00:00Z', 'codex/17', '/tmp/demo-1', 'tmux:demo-1',
			'agent-session-1', 'preserve me', '2026-09-05T00:00:00Z', '2026-09-05T00:00:00Z'
		)`,
	); err != nil {
		t.Fatalf("seed session: %v", err)
	}

	upTo(t, db, 25)
	if _, err := db.Exec(
		`INSERT INTO worker_scheduling_holds (session_id, held_at)
		 VALUES ('demo-1', '2026-09-05T01:00:00Z')`,
	); err != nil {
		t.Fatalf("insert hold for existing session: %v", err)
	}

	var heldAt time.Time
	if err := db.QueryRow(
		`SELECT held_at FROM worker_scheduling_holds WHERE session_id = 'demo-1'`,
	).Scan(&heldAt); err != nil {
		t.Fatalf("read migrated hold: %v", err)
	}
	wantHeldAt := time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC)
	if !heldAt.Equal(wantHeldAt) {
		t.Fatalf("held_at = %v, want %v", heldAt, wantHeldAt)
	}

	if _, err := db.Exec(
		`INSERT INTO worker_scheduling_holds (session_id, held_at)
		 VALUES ('missing-1', '2026-09-05T01:00:00Z')`,
	); err == nil {
		t.Fatal("hold for missing session succeeded, want foreign-key failure")
	}
}
