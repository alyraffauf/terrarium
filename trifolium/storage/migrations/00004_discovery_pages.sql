-- +goose Up
ALTER TABLE discovery_schedule ADD COLUMN cursor TEXT NOT NULL DEFAULT '';
ALTER TABLE discovery_schedule ADD COLUMN turn INTEGER NOT NULL DEFAULT 0;
ALTER TABLE discovery_schedule ADD COLUMN retrying INTEGER NOT NULL DEFAULT 0;
CREATE TABLE discovery_cursors(
    target TEXT NOT NULL,
    backlinks INTEGER NOT NULL,
    cursor TEXT NOT NULL,
    PRIMARY KEY(target,backlinks,cursor)
);

-- +goose Down
DROP TABLE discovery_cursors;
ALTER TABLE discovery_schedule DROP COLUMN retrying;
ALTER TABLE discovery_schedule DROP COLUMN turn;
ALTER TABLE discovery_schedule DROP COLUMN cursor;
