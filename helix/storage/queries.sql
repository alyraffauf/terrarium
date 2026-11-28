-- name: GetState :one
SELECT * FROM sync_state WHERE id=1;
-- name: InitializeCursor :exec
UPDATE sync_state SET cursor_us=? WHERE id=1 AND cursor_us=0;
-- name: SaveCheckpoint :exec
UPDATE sync_state SET cursor_us=MAX(cursor_us,CAST(sqlc.arg(cursor_us) AS INTEGER)),
 stream_seq=CASE WHEN CAST(sqlc.arg(seq) AS INTEGER)>0 AND (sqlc.arg(endpoint)<>stream_endpoint OR CAST(sqlc.arg(seq) AS INTEGER)>stream_seq) THEN CAST(sqlc.arg(seq) AS INTEGER) ELSE stream_seq END,
 stream_endpoint=CASE WHEN CAST(sqlc.arg(seq) AS INTEGER)>0 AND (sqlc.arg(endpoint)<>stream_endpoint OR CAST(sqlc.arg(seq) AS INTEGER)>stream_seq) THEN sqlc.arg(endpoint) ELSE stream_endpoint END WHERE id=1;
-- name: DirtyFollows :exec
UPDATE sync_state SET follow_version=follow_version+1 WHERE id=1;
-- name: ResetStream :exec
UPDATE sync_state SET stream_seq=0,stream_endpoint='',cursor_us=?,follow_version=follow_version+1,rescan_requested=1 WHERE id=1;
-- name: RecoverStream :exec
UPDATE sync_state SET stream_seq=CASE WHEN CAST(sqlc.arg(stream_seq) AS INTEGER)>0 AND (sqlc.arg(stream_endpoint)<>stream_endpoint OR CAST(sqlc.arg(stream_seq) AS INTEGER)>stream_seq) THEN CAST(sqlc.arg(stream_seq) AS INTEGER) ELSE stream_seq END,stream_endpoint=CASE WHEN CAST(sqlc.arg(stream_seq) AS INTEGER)>0 AND (sqlc.arg(stream_endpoint)<>stream_endpoint OR CAST(sqlc.arg(stream_seq) AS INTEGER)>stream_seq) THEN sqlc.arg(stream_endpoint) ELSE stream_endpoint END,invalid_frame=sqlc.arg(invalid_frame),rescan_requested=1,follow_version=follow_version+1 WHERE id=1;
-- name: TakeRescan :one
UPDATE sync_state SET rescan_requested=0 WHERE id=1 AND rescan_requested=1 RETURNING id;
-- name: ApplyFollowVersion :exec
UPDATE sync_state SET follow_applied=? WHERE id=1;
-- name: GetBinding :one
SELECT value FROM metadata WHERE key=?;
-- name: SetBinding :exec
INSERT OR IGNORE INTO metadata(key,value) VALUES(?,?);
-- name: GetList :one
SELECT list_uri FROM targets WHERE actor=?;
-- name: BindList :exec
INSERT INTO targets(actor,list_uri) VALUES(?,?) ON CONFLICT(actor) DO NOTHING;
-- name: GetListTarget :one
SELECT actor FROM targets WHERE list_uri=?;
-- name: EnqueueEvent :exec
INSERT INTO pending(did,ready,ready_since) VALUES(?,0,unixepoch()) ON CONFLICT(did) DO UPDATE SET hint_target='',hint_rkey='',generation=generation+1,ready=0,attempts=0,ready_since=min(pending.ready_since,excluded.ready_since);
-- name: QueueScan :exec
INSERT INTO pending(did,ready,ready_since) VALUES(?,unixepoch(),unixepoch()) ON CONFLICT(did) DO UPDATE SET hint_target='',hint_rkey='',generation=generation+1 WHERE pending.hint_target<>'';
-- name: QueueMembers :exec
INSERT INTO pending(did,ready,ready_since) SELECT DISTINCT did,unixepoch(),unixepoch() FROM list_members WHERE actor=? ON CONFLICT(did) DO UPDATE SET hint_target='',hint_rkey='',generation=generation+1 WHERE pending.hint_target<>'';
-- name: NextJob :one
SELECT did,generation,attempts FROM pending WHERE ready<=? ORDER BY ready,did LIMIT 1;
-- name: FinishJob :execrows
DELETE FROM pending WHERE did=? AND generation=?;
-- name: RetryJob :exec
UPDATE pending SET attempts=attempts+1,ready=sqlc.arg(ready),ready_since=sqlc.arg(ready) WHERE did=sqlc.arg(did) AND generation=sqlc.arg(generation);
-- name: MemberKeys :many
SELECT rkey FROM list_members WHERE actor=? AND did=? ORDER BY rkey;
-- name: RememberMember :exec
INSERT INTO list_members(actor,did,rkey) VALUES(?,?,?) ON CONFLICT(actor,rkey) DO UPDATE SET did=excluded.did;
-- name: ForgetMember :exec
DELETE FROM list_members WHERE actor=? AND rkey=?;
-- name: IsFollowed :one
SELECT EXISTS(SELECT 1 FROM follows WHERE did=?);
-- name: ClearNewFollows :exec
DELETE FROM new_follows;
-- name: AddNewFollow :exec
INSERT INTO new_follows(did) VALUES(?);
-- name: QueueFollowChanges :exec
INSERT INTO pending(did,ready_since)
SELECT did,unixepoch() FROM (SELECT did FROM follows EXCEPT SELECT did FROM new_follows)
UNION SELECT did,unixepoch() FROM (SELECT did FROM new_follows EXCEPT SELECT did FROM follows) WHERE true
ON CONFLICT(did) DO UPDATE SET hint_target='',hint_rkey='',generation=generation+1,ready=0,attempts=0,ready_since=min(pending.ready_since,excluded.ready_since);
-- name: ClearFollows :exec
DELETE FROM follows;
-- name: ReplaceFollows :exec
INSERT INTO follows SELECT did FROM new_follows;

-- name: RememberCandidate :exec
INSERT OR IGNORE INTO candidates(did) VALUES(?);
-- name: SaveCandidatePDS :exec
INSERT INTO candidates(did,pds) VALUES(?,?) ON CONFLICT(did) DO UPDATE SET pds=excluded.pds;
-- name: CandidatePDS :one
SELECT pds FROM candidates WHERE did=?;
-- name: KnownCandidate :one
SELECT EXISTS(SELECT did FROM candidates WHERE candidates.did=sqlc.arg(did) UNION ALL SELECT did FROM list_members WHERE list_members.did=sqlc.arg(did));

-- name: SaveListName :exec
INSERT INTO metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value;

-- name: NextJobs :many
-- Seven slots retain live-event priority. The last serves the oldest ready
-- account, so sustained event traffic cannot starve backfills or due retries.
WITH held AS (SELECT CAST(value AS TEXT) AS did FROM json_each(CAST(sqlc.arg(busy) AS TEXT)) WHERE value IS NOT NULL), priority AS (
  SELECT p.did,p.generation,p.attempts FROM pending AS p WHERE p.ready<=sqlc.arg(now) AND p.did NOT IN (SELECT did FROM held) ORDER BY p.ready,p.did LIMIT sqlc.arg(priority_slots)
), oldest AS (
  SELECT p.did,p.generation,p.attempts FROM pending AS p
  WHERE p.ready_since<=sqlc.arg(now) AND p.ready<=sqlc.arg(now) AND p.did NOT IN (SELECT did FROM priority) AND p.did NOT IN (SELECT did FROM held)
  ORDER BY p.ready_since LIMIT sqlc.arg(oldest_slot)
)
SELECT did,generation,attempts FROM priority
UNION ALL SELECT did,generation,attempts FROM oldest;
-- name: PendingCount :one
SELECT COUNT(*) FROM pending;

-- name: OldestReadyAgeSeconds :one
SELECT CAST(COALESCE(unixepoch()-MIN(ready_since),0) AS INTEGER)
FROM pending WHERE ready_since<=unixepoch();

-- name: QueueDiscoveredFollow :exec
-- A second target requires a full inventory. Preserve full work and retry deadlines.
INSERT INTO pending(did,ready,ready_since,hint_target,hint_rkey)
VALUES(?,unixepoch(),unixepoch(),?,?)
ON CONFLICT(did) DO UPDATE SET
  hint_target=CASE WHEN pending.hint_target=excluded.hint_target THEN excluded.hint_target ELSE '' END,
  hint_rkey=CASE WHEN pending.hint_target=excluded.hint_target THEN excluded.hint_rkey ELSE '' END,
  generation=generation+1
WHERE pending.hint_target<>'' AND (pending.hint_target<>excluded.hint_target OR (pending.hint_rkey='' AND excluded.hint_rkey<>''));

-- name: FollowHint :one
SELECT hint_target, hint_rkey FROM pending WHERE did=?;

-- name: CurrentJob :one
SELECT EXISTS(SELECT 1 FROM pending WHERE did=? AND generation=?);

-- name: BlocksWatchedAccount :one
SELECT EXISTS(SELECT 1 FROM follower_blocks WHERE target=sqlc.arg(target) AND actor=sqlc.arg(actor));
