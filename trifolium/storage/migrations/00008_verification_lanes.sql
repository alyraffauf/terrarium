-- +goose Up
ALTER TABLE scans ADD COLUMN continuation INTEGER NOT NULL DEFAULT 0;
ALTER TABLE scans ADD COLUMN queued INTEGER NOT NULL DEFAULT 0;
ALTER TABLE hints ADD COLUMN queued INTEGER NOT NULL DEFAULT 0;
UPDATE scans SET continuation=EXISTS(SELECT 1 FROM pages WHERE pages.did=scans.did AND pages.version=scans.version),queued=unixepoch();
UPDATE hints SET queued=unixepoch();
CREATE INDEX scans_lane_ready ON scans(continuation,due);
-- +goose StatementBegin
CREATE TRIGGER scans_enqueue AFTER INSERT ON scans BEGIN
 UPDATE scans SET queued=unixepoch() WHERE did=NEW.did;
END;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE TRIGGER hints_enqueue AFTER INSERT ON hints BEGIN
 UPDATE hints SET queued=unixepoch() WHERE id=NEW.id;
END;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE TRIGGER scans_restart AFTER UPDATE OF version ON scans WHEN NEW.version<>OLD.version BEGIN
 UPDATE scans SET continuation=0,queued=unixepoch() WHERE did=NEW.did;
END;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER scans_restart;
DROP TRIGGER hints_enqueue;
DROP TRIGGER scans_enqueue;
DROP INDEX scans_lane_ready;
ALTER TABLE hints DROP COLUMN queued;
ALTER TABLE scans DROP COLUMN queued;
ALTER TABLE scans DROP COLUMN continuation;
