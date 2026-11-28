-- +goose Up
-- ready=0 gives live events priority. Keep their actual ready time separately.
ALTER TABLE pending ADD COLUMN ready_since INTEGER NOT NULL DEFAULT 0;
UPDATE pending SET ready_since=CASE WHEN ready=0 THEN unixepoch() ELSE ready END;
CREATE INDEX pending_ready_since ON pending(ready_since);

-- +goose Down
DROP INDEX pending_ready_since;
ALTER TABLE pending DROP COLUMN ready_since;
