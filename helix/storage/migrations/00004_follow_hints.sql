-- +goose Up
-- Existing jobs retain their full reconciliation scope.
ALTER TABLE pending ADD COLUMN hint_target TEXT NOT NULL DEFAULT '';
ALTER TABLE pending ADD COLUMN hint_rkey TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE pending DROP COLUMN hint_rkey;
ALTER TABLE pending DROP COLUMN hint_target;
