-- +goose Up
ALTER TABLE worker_scheduling_holds ADD COLUMN retirement_project_id TEXT;
ALTER TABLE worker_scheduling_holds ADD COLUMN retirement_ticket TEXT;
ALTER TABLE worker_scheduling_holds ADD COLUMN retirement_session_updated_at TIMESTAMP;
ALTER TABLE worker_scheduling_holds ADD COLUMN retired_at TIMESTAMP;

-- +goose Down
ALTER TABLE worker_scheduling_holds DROP COLUMN retired_at;
ALTER TABLE worker_scheduling_holds DROP COLUMN retirement_session_updated_at;
ALTER TABLE worker_scheduling_holds DROP COLUMN retirement_ticket;
ALTER TABLE worker_scheduling_holds DROP COLUMN retirement_project_id;
