-- +goose Up
CREATE TABLE sync_state (
 id INTEGER PRIMARY KEY CHECK (id=1),
 cursor_us INTEGER NOT NULL DEFAULT 0,
 stream_seq INTEGER NOT NULL DEFAULT 0,
 stream_endpoint TEXT NOT NULL DEFAULT '',
 follow_version INTEGER NOT NULL DEFAULT 0,
 follow_applied INTEGER NOT NULL DEFAULT -1,
 rescan_requested INTEGER NOT NULL DEFAULT 0,
 invalid_frame TEXT NOT NULL DEFAULT ''
);
INSERT INTO sync_state(id,cursor_us,stream_seq,stream_endpoint,follow_version,follow_applied,rescan_requested,invalid_frame)
VALUES(1,
 COALESCE((SELECT CAST(value AS INTEGER) FROM metadata WHERE key='cursor'),0),
 COALESCE((SELECT CAST(value AS INTEGER) FROM metadata WHERE key='stream_seq'),0),
 COALESCE((SELECT value FROM metadata WHERE key='stream_endpoint'),''),
 COALESCE((SELECT CAST(value AS INTEGER) FROM metadata WHERE key='follow_version'),0),
 COALESCE((SELECT CAST(value AS INTEGER) FROM metadata WHERE key='follow_applied'),-1),
 COALESCE((SELECT CAST(value AS INTEGER) FROM metadata WHERE key='rescan_requested'),0),
 COALESCE((SELECT value FROM metadata WHERE key='invalid_frame'),''));
DELETE FROM metadata WHERE key IN ('cursor','stream_seq','stream_endpoint','follow_version','follow_applied','rescan_requested','invalid_frame');
CREATE TABLE new_follows (did TEXT PRIMARY KEY);
