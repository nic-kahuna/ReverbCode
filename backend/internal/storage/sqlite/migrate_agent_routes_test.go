package sqlite

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func TestMigrationFromVersion19PreservesSessionAndAddsAgentRoutes(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "ao.db")+pragmas)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	upTo(t, db, 19)

	if _, err := db.Exec(
		`INSERT INTO projects (id, path, repo_origin_url, display_name, registered_at)
		 VALUES ('demo', '/tmp/demo', 'https://github.com/example/demo.git', 'Demo', '2026-07-01T00:00:00Z')`,
	); err != nil {
		t.Fatalf("seed project: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO sessions (
			id, project_id, num, issue_id, kind, harness, activity_state,
			activity_last_at, branch, workspace_path, runtime_handle_id,
			agent_session_id, prompt, created_at, updated_at
		) VALUES (
			'demo-1', 'demo', 1, 'github:example/demo#1', 'worker', 'codex', 'idle',
			'2026-07-01T00:00:00Z', 'agent/demo-1', '/tmp/demo-1', 'tmux:demo-1',
			'agent-session-1', 'preserve me', '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z'
		)`,
	); err != nil {
		t.Fatalf("seed session: %v", err)
	}

	upTo(t, db, 24)

	var (
		issueID                  string
		branch                   string
		workspacePath            string
		prompt                   string
		requestedHarness         string
		requestedModel           string
		requestedReasoningEffort string
		launchModel              string
		launchReasoningEffort    string
		launchRouteRecorded      bool
	)
	if err := db.QueryRow(
		`SELECT issue_id, branch, workspace_path, prompt,
		        requested_harness, requested_model, requested_reasoning_effort,
		        launch_model, launch_reasoning_effort, launch_route_recorded
		   FROM sessions
		  WHERE id = 'demo-1'`,
	).Scan(
		&issueID,
		&branch,
		&workspacePath,
		&prompt,
		&requestedHarness,
		&requestedModel,
		&requestedReasoningEffort,
		&launchModel,
		&launchReasoningEffort,
		&launchRouteRecorded,
	); err != nil {
		t.Fatalf("read migrated session: %v", err)
	}

	if issueID != "github:example/demo#1" ||
		branch != "agent/demo-1" ||
		workspacePath != "/tmp/demo-1" ||
		prompt != "preserve me" {
		t.Fatalf(
			"existing session data changed: issue=%q branch=%q workspace=%q prompt=%q",
			issueID,
			branch,
			workspacePath,
			prompt,
		)
	}
	if requestedHarness != "" ||
		requestedModel != "" ||
		requestedReasoningEffort != "" ||
		launchModel != "" ||
		launchReasoningEffort != "" ||
		launchRouteRecorded {
		t.Fatalf(
			"route defaults are not empty: requested=(%q,%q,%q) launch=(%q,%q,%t)",
			requestedHarness,
			requestedModel,
			requestedReasoningEffort,
			launchModel,
			launchReasoningEffort,
			launchRouteRecorded,
		)
	}
}
