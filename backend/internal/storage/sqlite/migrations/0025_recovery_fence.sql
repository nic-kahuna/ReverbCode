-- +goose Up
-- The recovery fence is deliberately independent of the product schema. The
-- bootstrap opener installs this exact table before ordinary Goose migrations,
-- then Goose records version 25 after an inactive decision permits migrations.
-- A temporary guard prevents a down/re-up from replacing a missing/corrupt row
-- with a fresh inactive row.
-- +goose StatementBegin
CREATE TEMP TABLE IF NOT EXISTS recovery_fence_0025_guard (
    table_existed INTEGER NOT NULL
);
DELETE FROM recovery_fence_0025_guard;
INSERT INTO recovery_fence_0025_guard (table_existed)
SELECT EXISTS (
    SELECT 1 FROM sqlite_master
    WHERE type = 'table' AND name = 'recovery_fence'
);

CREATE TABLE IF NOT EXISTS recovery_fence (
    singleton        INTEGER PRIMARY KEY CHECK (singleton = 1),
    protocol_version INTEGER NOT NULL,
    state            TEXT NOT NULL,
    payload          BLOB NOT NULL,
    generation       INTEGER NOT NULL,
    activation_id    TEXT
);

INSERT INTO recovery_fence (singleton, protocol_version, state, payload, generation, activation_id)
SELECT 1, 1, 'inactive', CAST('{}' AS BLOB), 0, NULL
WHERE (SELECT table_existed FROM recovery_fence_0025_guard) = 0;

DROP TABLE recovery_fence_0025_guard;
-- +goose StatementEnd

-- +goose Down
-- Rollback must retain the fence and its opaque payload bytes. Goose may mark
-- version 25 down, but the durable rollback floor remains present.
SELECT 1;
