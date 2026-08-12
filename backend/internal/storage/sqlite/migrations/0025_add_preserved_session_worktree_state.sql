-- Managed startup reconciliation needs a durable journal state that the
-- current binary never treats as an automatic-restore marker. Rebuild the table
-- only to widen its CHECK; every identity, branch, path, ref, and prior state is
-- copied verbatim. Older executables are not managed-marker-aware throughout
-- reconciliation and must not be launched while any managed row exists.

-- +goose Up
-- +goose StatementBegin
CREATE TABLE session_worktrees_next (
    session_id     TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    repo_name      TEXT NOT NULL,
    branch         TEXT NOT NULL,
    base_sha       TEXT NOT NULL,
    worktree_path  TEXT NOT NULL,
    preserved_ref  TEXT NOT NULL DEFAULT '',
    state          TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('active', 'removed', 'retry_remove', 'unavailable', 'stray_moved', 'preserved', 'preserved_removed', 'preserved_partial')),
    PRIMARY KEY (session_id, repo_name)
);

INSERT INTO session_worktrees_next (
    session_id, repo_name, branch, base_sha, worktree_path, preserved_ref, state
)
SELECT session_id, repo_name, branch, base_sha, worktree_path, preserved_ref, state
FROM session_worktrees;

DROP TABLE session_worktrees;
ALTER TABLE session_worktrees_next RENAME TO session_worktrees;
CREATE INDEX idx_session_worktrees_session ON session_worktrees(session_id);
-- +goose StatementEnd

-- +goose Down
-- Deliberately retain the widened CHECK and every managed-recovery row.
-- Narrowing the table would require deleting or coercing recovery evidence.
-- This preserves bytes for reinstalling the current build; it does not make an
-- older executable safe to run against managed recovery rows.
-- +goose StatementBegin
SELECT 1;
-- +goose StatementEnd
