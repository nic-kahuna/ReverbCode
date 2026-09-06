-- +goose Up
-- These empty tables do not activate protocol 2. Activation inserts their first
-- authority rows only after the held bootguard has durably ratcheted the floor.
CREATE TABLE custody_projects (
    project_id TEXT PRIMARY KEY REFERENCES projects(id),
    activated_at TEXT NOT NULL
);
CREATE TABLE custody_revision (id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL);
CREATE TABLE custody_attempts (
    attempt_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    project_id TEXT NOT NULL REFERENCES custody_projects(project_id),
    generation INTEGER NOT NULL UNIQUE,
    revision INTEGER NOT NULL,
    retired INTEGER NOT NULL DEFAULT 0 CHECK(retired IN (0,1)),
    document TEXT NOT NULL CHECK(json_valid(document))
);
CREATE UNIQUE INDEX custody_current_session ON custody_attempts(session_id) WHERE retired=0;

-- +goose StatementBegin
CREATE TRIGGER custody_prevent_session_delete BEFORE DELETE ON sessions
WHEN EXISTS (SELECT 1 FROM custody_projects WHERE project_id=OLD.project_id)
BEGIN SELECT RAISE(ABORT, 'AO_CUSTODY_FENCED'); END;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE TRIGGER custody_prevent_project_delete BEFORE DELETE ON projects
WHEN EXISTS (SELECT 1 FROM custody_projects WHERE project_id=OLD.id)
BEGIN SELECT RAISE(ABORT, 'AO_CUSTODY_FENCED'); END;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE TRIGGER custody_prevent_project_identity_change BEFORE UPDATE ON projects
WHEN EXISTS (SELECT 1 FROM custody_projects WHERE project_id=OLD.id)
AND (NEW.id<>OLD.id OR NEW.path<>OLD.path OR NEW.repo_origin_url<>OLD.repo_origin_url OR NEW.archived_at IS NOT OLD.archived_at)
BEGIN SELECT RAISE(ABORT, 'AO_CUSTODY_FENCED'); END;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE TRIGGER custody_prevent_fenced_session_change BEFORE UPDATE ON sessions
WHEN EXISTS (SELECT 1 FROM custody_projects WHERE project_id=OLD.project_id)
AND NOT EXISTS (SELECT 1 FROM custody_attempts WHERE session_id=OLD.id AND retired=0 AND json_extract(document,'$.fence')='' AND json_extract(document,'$.phase') IN ('preparing','launching') AND (COALESCE(json_extract(document,'$.predecessor_attempt_id'),'')='' OR COALESCE(json_extract(document,'$.provider_id'),'')=''))
AND (NEW.id<>OLD.id OR NEW.project_id<>OLD.project_id OR NEW.issue_id<>OLD.issue_id OR NEW.harness<>OLD.harness
 OR NEW.branch<>OLD.branch OR NEW.workspace_path<>OLD.workspace_path OR NEW.runtime_handle_id<>OLD.runtime_handle_id
 OR NEW.agent_session_id<>OLD.agent_session_id OR NEW.prompt<>OLD.prompt OR NEW.is_terminated<>OLD.is_terminated)
BEGIN SELECT RAISE(ABORT, 'AO_CUSTODY_FENCED'); END;
-- +goose StatementEnd


-- +goose StatementBegin
CREATE TRIGGER custody_prevent_incompatible_project_config BEFORE UPDATE ON projects
WHEN EXISTS (SELECT 1 FROM custody_projects WHERE project_id=OLD.id)
AND CASE WHEN json_valid(COALESCE(NEW.config,'{}')) AND json_valid(COALESCE(OLD.config,'{}'))
 THEN EXISTS (
 SELECT fullkey,type,atom FROM json_tree(COALESCE(NEW.config,'{}')) WHERE type NOT IN ('object','array','null') AND fullkey<>'$.admissionPaused'
 EXCEPT SELECT fullkey,type,atom FROM json_tree(COALESCE(OLD.config,'{}')) WHERE type NOT IN ('object','array','null') AND fullkey<>'$.admissionPaused'
 ) OR EXISTS (
 SELECT fullkey,type,atom FROM json_tree(COALESCE(OLD.config,'{}')) WHERE type NOT IN ('object','array','null') AND fullkey<>'$.admissionPaused'
 EXCEPT SELECT fullkey,type,atom FROM json_tree(COALESCE(NEW.config,'{}')) WHERE type NOT IN ('object','array','null') AND fullkey<>'$.admissionPaused'
 )
 ELSE 1 END
BEGIN SELECT RAISE(ABORT, 'AO_CUSTODY_FENCED'); END;
-- +goose StatementEnd

-- +goose Down
-- Custody authority is permanent historical evidence. Downgrade requires a
-- separately designed migration; never drop fences to make an older binary run.
SELECT 1;
