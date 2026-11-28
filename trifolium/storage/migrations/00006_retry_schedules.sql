-- +goose Up
ALTER TABLE discovery_schedule ADD COLUMN retry_seconds INTEGER NOT NULL DEFAULT 0;
ALTER TABLE retries ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE retries DROP COLUMN attempts;
ALTER TABLE discovery_schedule DROP COLUMN retry_seconds;
