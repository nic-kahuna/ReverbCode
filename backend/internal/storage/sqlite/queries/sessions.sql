-- name: NextSessionNum :one
SELECT COALESCE(MAX(num), 0) + 1 AS next FROM sessions WHERE project_id = ?;

-- name: InsertSession :exec
INSERT INTO sessions (
    id, project_id, num, issue_id, kind, harness, display_name,
    activity_state, activity_last_at, first_signal_at, is_terminated,
    branch, workspace_path, runtime_handle_id, agent_session_id, prompt,
    preview_url, preview_revision, requested_harness, requested_model,
    requested_reasoning_effort, launch_model, launch_reasoning_effort,
    launch_route_recorded, launch_failure_stage, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: UpdateSession :exec
UPDATE sessions SET
    issue_id = ?, kind = ?, harness = ?, display_name = ?,
    activity_state = ?, activity_last_at = ?, first_signal_at = ?, is_terminated = ?,
    branch = ?, workspace_path = ?, runtime_handle_id = ?, agent_session_id = ?, prompt = ?,
    preview_url = ?, preview_revision = ?, requested_harness = ?, requested_model = ?,
    requested_reasoning_effort = ?, launch_model = ?, launch_reasoning_effort = ?,
    launch_route_recorded = ?, launch_failure_stage = ?, updated_at = ?
WHERE id = ?;

-- name: RecordAdmissionFailureBeforeWorkspace :execrows
-- The native Spawn admission branch supplies the observation; these seed
-- predicates prevent a concurrent delete/reuse from recording stale evidence.
UPDATE sessions SET
    activity_state = 'exited', activity_last_at = ?, is_terminated = 1,
    launch_failure_stage = 'admission_before_workspace', updated_at = ?
WHERE id = ?
    AND kind = 'worker'
    AND activity_state = 'idle'
    AND first_signal_at IS NULL
    AND branch = '' AND workspace_path = '' AND runtime_handle_id = ''
    AND agent_session_id = '' AND prompt = '' AND launch_failure_stage = ''
    AND is_terminated = 0;

-- name: ClearSessionLaunchFailureStage :execrows
-- Confirm existence while invalidating only the stage. Already-empty records
-- preserve their timestamp and generate no CDC event.
UPDATE sessions SET
    updated_at = CASE WHEN launch_failure_stage <> '' THEN sqlc.arg(observed_at) ELSE updated_at END,
    launch_failure_stage = ''
WHERE id = sqlc.arg(id);

-- name: GetSession :one
SELECT id, project_id, num, issue_id, kind, harness,
    activity_state, activity_last_at, is_terminated, branch, workspace_path,
    runtime_handle_id, agent_session_id, prompt, created_at, updated_at, display_name, first_signal_at, preview_url, preview_revision,
    requested_harness, requested_model, requested_reasoning_effort, launch_model, launch_reasoning_effort, launch_route_recorded, launch_failure_stage
FROM sessions WHERE id = ?;

-- name: ListSessionsByProject :many
SELECT id, project_id, num, issue_id, kind, harness,
    activity_state, activity_last_at, is_terminated, branch, workspace_path,
    runtime_handle_id, agent_session_id, prompt, created_at, updated_at, display_name, first_signal_at, preview_url, preview_revision,
    requested_harness, requested_model, requested_reasoning_effort, launch_model, launch_reasoning_effort, launch_route_recorded, launch_failure_stage
FROM sessions WHERE project_id = ? ORDER BY num;

-- name: ListAllSessions :many
SELECT id, project_id, num, issue_id, kind, harness,
    activity_state, activity_last_at, is_terminated, branch, workspace_path,
    runtime_handle_id, agent_session_id, prompt, created_at, updated_at, display_name, first_signal_at, preview_url, preview_revision,
    requested_harness, requested_model, requested_reasoning_effort, launch_model, launch_reasoning_effort, launch_route_recorded, launch_failure_stage
FROM sessions ORDER BY project_id, num;


-- name: RenameSession :execrows
UPDATE sessions SET display_name = ?, updated_at = ? WHERE id = ?;

-- name: SetSessionPreviewURL :execrows
-- preview_revision is bumped on every call (even when preview_url is unchanged)
-- so a repeated `ao preview <same-url>` still trips the sessions_cdc_update
-- trigger and the desktop browser panel re-navigates / refreshes.
UPDATE sessions SET preview_url = ?, preview_revision = preview_revision + 1, updated_at = ? WHERE id = ?;

-- name: SetWorkerSchedulingHold :one
-- Preserve the first hold timestamp so repeated requests are idempotent.
INSERT INTO worker_scheduling_holds (session_id, held_at)
VALUES (?, ?)
ON CONFLICT (session_id) DO UPDATE SET held_at = worker_scheduling_holds.held_at
RETURNING session_id, held_at;

-- name: GetWorkerSchedulingHold :one
SELECT session_id, held_at
FROM worker_scheduling_holds
WHERE session_id = ?;

-- name: SessionIsSeed :one
-- SessionIsSeed reports whether the session id matches a row still in seed
-- state (see DeleteSeedSession for the conditions). Callers probe with this
-- before touching change_log so that DeleteSession is a true no-op for live
-- sessions instead of silently destroying their CDC events. Returns 0 when
-- the row does not exist OR has progressed past seed state.
SELECT EXISTS(
    SELECT 1 FROM sessions
    WHERE id = ?
      AND is_terminated = 0
      AND workspace_path = ''
      AND runtime_handle_id = ''
      AND agent_session_id = ''
      AND prompt = ''
) AS is_seed;

-- NOTE: the `DELETE FROM sessions WHERE id = ? AND <seed-state predicates>`
-- statement is intentionally NOT a sqlc query — same sqlc 1.31 SQLite-parser
-- bug as documented in queries/changelog.sql: trailing string literals (and
-- placeholders) on the RHS of `=` in a DELETE get silently stripped, so the
-- generated SQL ends up mid-clause and the row count is meaningless. The
-- store runs that DELETE directly via tx.ExecContext inside
-- Store.DeleteSession, inside the same transaction as the SessionIsSeed
-- probe and the raw change_log cleanup.
