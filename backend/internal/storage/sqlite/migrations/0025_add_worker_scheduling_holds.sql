-- +goose Up
-- +goose StatementBegin
CREATE TABLE worker_scheduling_holds (
    session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
    held_at TIMESTAMP NOT NULL
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS worker_scheduling_holds;
-- +goose StatementEnd
