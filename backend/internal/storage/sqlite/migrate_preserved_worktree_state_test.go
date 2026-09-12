package sqlite

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/pressly/goose/v3"
)

type migrationWorktreeRow struct {
	SessionID    string
	RepoName     string
	Branch       string
	BaseSHA      string
	WorktreePath string
	PreservedRef string
	State        string
}

func TestMigration0025PreservesWorktreeJournalAcrossUpgradeAndRollback(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "ao.db")+pragmas)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	upTo(t, db, 24)
	if _, err := db.Exec(
		`INSERT INTO projects (id, path, repo_origin_url, display_name, registered_at, config)
		 VALUES ('demo', '/tmp/demo', 'https://github.com/example/demo.git', 'Demo', '2026-08-12T00:00:00Z',
		 '{"defaultBranch":"develop","env":{"KEEP":"yes"},"startupRestorePolicy":"preserve_only"}')`,
	); err != nil {
		t.Fatalf("seed project: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO sessions (
			id, project_id, num, issue_id, kind, harness, activity_state,
			activity_last_at, branch, workspace_path, runtime_handle_id,
			agent_session_id, prompt, requested_harness, requested_model,
			requested_reasoning_effort, launch_model, launch_reasoning_effort,
			launch_route_recorded, created_at, updated_at
		) VALUES (
			'demo-1', 'demo', 1, '', 'worker', 'codex', 'exited',
			'2026-08-12T00:00:00Z', 'ao/demo-1', '/managed/demo-1', 'demo-1',
			'provider-session-1', 'do not replay', 'codex', 'gpt-requested', 'high',
			'gpt-launched', 'medium', TRUE, '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z'
		)`,
	); err != nil {
		t.Fatalf("seed session: %v", err)
	}

	wantBefore := []migrationWorktreeRow{
		{"demo-1", "__root__", "ao/demo-1", "base-root", "/managed/demo-1", "", "active"},
		{"demo-1", "api", "ao/demo-1", "base-api", "/managed/demo-1/api", "refs/ao/preserved/demo-1-api", "removed"},
		{"demo-1", "docs", "ao/demo-1", "base-docs", "/managed/demo-1/docs", "refs/ao/preserved/demo-1-docs", "retry_remove"},
		{"demo-1", "mobile", "ao/demo-1", "base-mobile", "/managed/demo-1/mobile", "", "unavailable"},
		{"demo-1", "tools", "ao/demo-1", "base-tools", "/managed/demo-1/tools", "refs/ao/preserved/demo-1-tools", "stray_moved"},
		{"demo-1", "web", "ao/demo-1", "base-web", "/managed/demo-1/web", "refs/ao/preserved/demo-1-web", "removed"},
	}
	for _, row := range wantBefore {
		if _, err := db.Exec(
			`INSERT INTO session_worktrees
			 (session_id, repo_name, branch, base_sha, worktree_path, preserved_ref, state)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			row.SessionID, row.RepoName, row.Branch, row.BaseSHA, row.WorktreePath, row.PreservedRef, row.State,
		); err != nil {
			t.Fatalf("seed worktree %s: %v", row.RepoName, err)
		}
	}

	upTo(t, db, 25)
	if got := readMigrationWorktrees(t, db); !reflect.DeepEqual(got, wantBefore) {
		t.Fatalf("rows after upgrade = %#v, want %#v", got, wantBefore)
	}
	assertMigrationSessionFacts(t, db, `{"defaultBranch":"develop","env":{"KEEP":"yes"},"startupRestorePolicy":"preserve_only"}`)

	if _, err := db.Exec(
		`UPDATE session_worktrees
		 SET state = CASE repo_name
		     WHEN '__root__' THEN 'preserved_partial'
		     WHEN 'api' THEN 'preserved_removed'
		     WHEN 'docs' THEN 'preserved_partial'
		     WHEN 'mobile' THEN 'preserved_partial'
		     WHEN 'tools' THEN 'preserved_partial'
		     WHEN 'web' THEN 'preserved'
		 END
		 WHERE session_id = 'demo-1'`,
	); err != nil {
		t.Fatalf("write exact managed-recovery states: %v", err)
	}
	wantPreserved := append([]migrationWorktreeRow(nil), wantBefore...)
	wantPreserved[0].State = "preserved_partial"
	wantPreserved[1].State = "preserved_removed"
	wantPreserved[2].State = "preserved_partial"
	wantPreserved[3].State = "preserved_partial"
	wantPreserved[4].State = "preserved_partial"
	wantPreserved[5].State = "preserved"

	downTo(t, db, 24)
	if got := readMigrationWorktrees(t, db); !reflect.DeepEqual(got, wantPreserved) {
		t.Fatalf("rows after rollback = %#v, want preserved evidence %#v", got, wantPreserved)
	}
	// Simulate a v0.10.3-era whole-config writer that cannot preserve the new
	// field. Managed journal provenance must survive independently of policy.
	if _, err := db.Exec(`UPDATE projects SET config = '{"defaultBranch":"develop","env":{"KEEP":"yes"}}' WHERE id = 'demo'`); err != nil {
		t.Fatalf("simulate old config rewrite: %v", err)
	}
	assertMigrationSessionFacts(t, db, `{"defaultBranch":"develop","env":{"KEEP":"yes"}}`)

	// Down intentionally leaves the widened table in place. Reinstalling a new
	// binary reapplies 0025; the rebuild must remain data-preserving and accept
	// the already-journaled state.
	upTo(t, db, 25)
	if got := readMigrationWorktrees(t, db); !reflect.DeepEqual(got, wantPreserved) {
		t.Fatalf("rows after re-upgrade = %#v, want %#v", got, wantPreserved)
	}
	assertMigrationSessionFacts(t, db, `{"defaultBranch":"develop","env":{"KEEP":"yes"}}`)
	if _, err := db.Exec(
		`UPDATE session_worktrees SET state = 'not-a-state' WHERE session_id = 'demo-1' AND repo_name = 'api'`,
	); err == nil {
		t.Fatal("widened CHECK accepted an unknown worktree state")
	}
}

func assertMigrationSessionFacts(t *testing.T, db *sql.DB, wantConfig string) {
	t.Helper()
	var config, provider, requestedHarness, requestedModel, requestedEffort, launchModel, launchEffort string
	var launchRecorded bool
	err := db.QueryRow(
		`SELECT p.config, s.agent_session_id, s.requested_harness, s.requested_model,
		        s.requested_reasoning_effort, s.launch_model, s.launch_reasoning_effort,
		        s.launch_route_recorded
		 FROM sessions s JOIN projects p ON p.id = s.project_id
		 WHERE s.id = 'demo-1'`,
	).Scan(&config, &provider, &requestedHarness, &requestedModel, &requestedEffort, &launchModel, &launchEffort, &launchRecorded)
	if err != nil {
		t.Fatalf("read durable session facts: %v", err)
	}
	if config != wantConfig || provider != "provider-session-1" || requestedHarness != "codex" || requestedModel != "gpt-requested" || requestedEffort != "high" || launchModel != "gpt-launched" || launchEffort != "medium" || !launchRecorded {
		t.Fatalf("durable facts changed: config=%s provider=%q requested=%q/%q/%q launch=%q/%q recorded=%v", config, provider, requestedHarness, requestedModel, requestedEffort, launchModel, launchEffort, launchRecorded)
	}
}

func downTo(t *testing.T, db *sql.DB, version int64) {
	t.Helper()
	gooseMu.Lock()
	defer gooseMu.Unlock()
	goose.SetBaseFS(migrationsFS)
	goose.SetLogger(goose.NopLogger())
	if err := goose.SetDialect("sqlite3"); err != nil {
		t.Fatalf("set dialect: %v", err)
	}
	if err := goose.DownTo(db, "migrations", version); err != nil {
		t.Fatalf("migrate down to %d: %v", version, err)
	}
}

func readMigrationWorktrees(t *testing.T, db *sql.DB) []migrationWorktreeRow {
	t.Helper()
	rows, err := db.Query(
		`SELECT session_id, repo_name, branch, base_sha, worktree_path, preserved_ref, state
		 FROM session_worktrees
		 ORDER BY CASE WHEN repo_name = '__root__' THEN 0 ELSE 1 END, repo_name`,
	)
	if err != nil {
		t.Fatalf("query worktrees: %v", err)
	}
	defer rows.Close()

	var out []migrationWorktreeRow
	for rows.Next() {
		var row migrationWorktreeRow
		if err := rows.Scan(
			&row.SessionID,
			&row.RepoName,
			&row.Branch,
			&row.BaseSHA,
			&row.WorktreePath,
			&row.PreservedRef,
			&row.State,
		); err != nil {
			t.Fatalf("scan worktree: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate worktrees: %v", err)
	}
	return out
}
