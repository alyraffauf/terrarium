-- name: GetState :one
SELECT * FROM sync_state WHERE id=1;
-- name: InitializeCursor :exec
UPDATE sync_state SET cursor_us=? WHERE id=1 AND cursor_us=0;
-- name: SaveCheckpoint :exec
UPDATE sync_state SET cursor_us=MAX(cursor_us,CAST(sqlc.arg(cursor_us) AS INTEGER)),
 stream_seq=CASE WHEN CAST(sqlc.arg(seq) AS INTEGER)>0 THEN CAST(sqlc.arg(seq) AS INTEGER) ELSE stream_seq END,
 stream_endpoint=CASE WHEN CAST(sqlc.arg(seq) AS INTEGER)>0 THEN sqlc.arg(endpoint) ELSE stream_endpoint END WHERE id=1;
-- name: DirtyFollows :exec
UPDATE sync_state SET follow_version=follow_version+1 WHERE id=1;
-- name: ResetStream :exec
UPDATE sync_state SET stream_seq=0,stream_endpoint='',cursor_us=?,follow_version=follow_version+1 WHERE id=1;
-- name: RecoverStream :exec
UPDATE sync_state SET stream_seq=?,stream_endpoint=?,invalid_frame=?,rescan_requested=1,follow_version=follow_version+1 WHERE id=1;
-- name: TakeRescan :one
UPDATE sync_state SET rescan_requested=0 WHERE id=1 AND rescan_requested=1 RETURNING id;
-- name: ApplyFollowVersion :exec
UPDATE sync_state SET follow_applied=? WHERE id=1;
-- name: GetBinding :one
SELECT value FROM metadata WHERE key=?;
-- name: SetBinding :exec
INSERT OR IGNORE INTO metadata(key,value) VALUES(?,?);
-- name: GetList :one
SELECT list_uri FROM targets WHERE pds=?;
-- name: BindList :exec
INSERT INTO targets(pds,list_uri) VALUES(?,?);
-- name: EnqueueEvent :exec
INSERT INTO pending(did,ready) VALUES(?,unixepoch()) ON CONFLICT(did) DO UPDATE SET generation=generation+1;
-- name: QueueScan :exec
INSERT INTO pending(did,ready) VALUES(?,unixepoch()) ON CONFLICT(did) DO NOTHING;
-- name: QueueMembers :exec
INSERT INTO pending(did,ready) SELECT DISTINCT did,unixepoch() FROM list_members WHERE pds=? ON CONFLICT(did) DO NOTHING;
-- name: NextJob :one
SELECT did,generation,attempts FROM pending WHERE ready<=? ORDER BY ready,did LIMIT 1;
-- name: FinishJob :exec
DELETE FROM pending WHERE did=? AND generation=?;
-- name: RetryJob :exec
UPDATE pending SET attempts=attempts+1,ready=? WHERE did=? AND generation=?;
-- name: MemberKeys :many
SELECT rkey FROM list_members WHERE pds=? AND did=? ORDER BY rkey;
-- name: RememberMember :exec
INSERT INTO list_members(pds,did,rkey) VALUES(?,?,?) ON CONFLICT(pds,rkey) DO UPDATE SET did=excluded.did;
-- name: ForgetMember :exec
DELETE FROM list_members WHERE pds=? AND rkey=?;
-- name: IsFollowed :one
SELECT EXISTS(SELECT 1 FROM follows WHERE did=?);
-- name: ClearNewFollows :exec
DELETE FROM new_follows;
-- name: AddNewFollow :exec
INSERT INTO new_follows(did) VALUES(?);
-- name: QueueFollowChanges :exec
INSERT INTO pending(did)
SELECT did FROM (SELECT did FROM follows EXCEPT SELECT did FROM new_follows)
UNION SELECT did FROM (SELECT did FROM new_follows EXCEPT SELECT did FROM follows) WHERE true
ON CONFLICT(did) DO UPDATE SET generation=generation+1,ready=0,attempts=0;
-- name: ClearFollows :exec
DELETE FROM follows;
-- name: ReplaceFollows :exec
INSERT INTO follows SELECT did FROM new_follows;

-- name: PendingCount :one
SELECT COUNT(*) FROM pending;
