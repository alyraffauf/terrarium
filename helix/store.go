package helix

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/alyraffauf/terrarium/helix/storage"
	"github.com/alyraffauf/terrarium/palustre"
)

type store struct {
	conn    *sql.DB
	queries *storage.Queries
}
type checkpoint struct {
	TimeUS, Seq int64
	Endpoint    string
}
type job struct {
	DID        string
	Generation int64
	Attempts   int
}

func openStore(path string) (*store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;`); err != nil {
		db.Close()
		return nil, err
	}
	if err = migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &store{conn: db, queries: storage.New(db)}, nil
}

func (db *store) transaction(ctx context.Context, apply func(*storage.Queries) error) error {
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = apply(db.queries.WithTx(tx)); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *store) bindList(ctx context.Context, target Target, owner string) (string, error) {
	uris, err := db.bindLists(ctx, []Target{target}, owner)
	if err != nil {
		return "", err
	}
	return uris[0], nil
}
func (db *store) bindLists(ctx context.Context, targets []Target, owner string) ([]string, error) {
	var uris []string
	err := db.transaction(ctx, func(queries *storage.Queries) error {
		for _, target := range targets {
			uri, err := queries.GetList(ctx, target.Actor)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if uri != "" {
				if target.ListURI != "" && target.ListURI != uri {
					return fmt.Errorf("%s is already bound to %s", target.Actor, uri)
				}
			} else {
				uri = target.ListURI
				if uri == "" {
					uri = "at://" + owner + "/" + listCollection + "/" + recordKey("followers:"+target.Actor)
				}
				if err := queries.BindList(ctx, storage.BindListParams{Actor: target.Actor, ListUri: uri}); err != nil {
					return fmt.Errorf("bind %s to %s: %w", target.Actor, uri, err)
				}
			}
			uris = append(uris, uri)
		}
		return nil
	})
	return uris, err
}
func (db *store) bind(ctx context.Context, key, value string) error {
	return db.transaction(ctx, func(queries *storage.Queries) error {
		if err := queries.SetBinding(ctx, storage.SetBindingParams{Key: key, Value: value}); err != nil {
			return err
		}
		saved, err := queries.GetBinding(ctx, key)
		if err != nil {
			return err
		}
		if saved != value {
			return fmt.Errorf("database belongs to a different %s (%s); use a separate database", key, saved)
		}
		return nil
	})
}
func (db *store) owner(ctx context.Context) (string, error) {
	return db.queries.GetBinding(ctx, "owner")
}
func (db *store) initializeCursor(ctx context.Context) error {
	return db.queries.InitializeCursor(ctx, time.Now().Add(-time.Minute).UnixMicro())
}
func (db *store) takeRescan(ctx context.Context) (bool, error) {
	_, err := db.queries.TakeRescan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// Event work and its checkpoint commit together, including follow invalidation.
func (db *store) ingest(ctx context.Context, did string, position checkpoint, followChanged bool) error {
	return db.transaction(ctx, func(queries *storage.Queries) error {
		if followChanged {
			if err := queries.DirtyFollows(ctx); err != nil {
				return err
			}
		}
		if did != "" {
			if err := queries.RememberCandidate(ctx, did); err != nil {
				return err
			}
			if err := queries.EnqueueEvent(ctx, did); err != nil {
				return err
			}
		}
		return queries.SaveCheckpoint(ctx, storage.SaveCheckpointParams{CursorUs: position.TimeUS, Seq: position.Seq, Endpoint: position.Endpoint})
	})
}
func (db *store) resumeCursor(ctx context.Context, endpoint string) (int64, error) {
	state, err := db.queries.GetState(ctx)
	if err != nil {
		return 0, err
	}
	if palustre.StreamEndpointsEqual(state.StreamEndpoint, endpoint) && state.StreamSeq > 0 {
		return state.StreamSeq, nil
	}
	return max(0, state.CursorUs-5_000_000), nil
}
func (db *store) resetStream(ctx context.Context) error {
	return db.queries.ResetStream(ctx, time.Now().Add(-time.Minute).UnixMicro())
}
func (db *store) recoverStream(ctx context.Context, endpoint string, seq int64, frame string) error {
	return db.queries.RecoverStream(ctx, storage.RecoverStreamParams{StreamSeq: seq, StreamEndpoint: endpoint, InvalidFrame: frame})
}
func (db *store) next(ctx context.Context) (job, error) {
	row, err := db.queries.NextJob(ctx, time.Now().Unix())
	return job{DID: row.Did, Generation: row.Generation, Attempts: int(row.Attempts)}, err
}
func (db *store) finish(ctx context.Context, work job) error {
	// A newer event must survive a request that was already in flight.
	return db.queries.FinishJob(ctx, storage.FinishJobParams{Did: work.DID, Generation: work.Generation})
}
func (db *store) retry(ctx context.Context, work job) error {
	delay := min(time.Hour, time.Second*time.Duration(1<<min(work.Attempts+1, 12)))
	return db.queries.RetryJob(ctx, storage.RetryJobParams{Did: work.DID, Generation: work.Generation, Ready: time.Now().Add(delay).Unix()})
}
func (db *store) queueScan(ctx context.Context, did string) error {
	return db.transaction(ctx, func(queries *storage.Queries) error {
		if err := queries.RememberCandidate(ctx, did); err != nil {
			return err
		}
		return queries.QueueScan(ctx, did)
	})
}
func (db *store) queueMembers(ctx context.Context, actor string) error {
	return db.queries.QueueMembers(ctx, actor)
}
func (db *store) MemberKeys(ctx context.Context, actor, did string) ([]string, error) {
	return db.queries.MemberKeys(ctx, storage.MemberKeysParams{Actor: actor, Did: did})
}
func (db *store) RememberMember(ctx context.Context, actor, did, key string) error {
	return db.queries.RememberMember(ctx, storage.RememberMemberParams{Actor: actor, Did: did, Rkey: key})
}
func (db *store) ForgetMember(ctx context.Context, actor, key string) error {
	return db.queries.ForgetMember(ctx, storage.ForgetMemberParams{Actor: actor, Rkey: key})
}
func (db *store) isFollowed(ctx context.Context, did string) (bool, error) {
	return db.queries.IsFollowed(ctx, did)
}
func (db *store) followVersion(ctx context.Context) (version, applied int64, err error) {
	state, err := db.queries.GetState(ctx)
	return state.FollowVersion, state.FollowApplied, err
}
func (db *store) applyFollowSnapshot(ctx context.Context, version int64, follows map[string]bool) error {
	return db.transaction(ctx, func(queries *storage.Queries) error {
		if err := queries.ClearNewFollows(ctx); err != nil {
			return err
		}
		for did := range follows {
			if err := queries.AddNewFollow(ctx, did); err != nil {
				return err
			}
		}
		if err := queries.QueueFollowChanges(ctx); err != nil {
			return err
		}
		if err := queries.ClearFollows(ctx); err != nil {
			return err
		}
		if err := queries.ReplaceFollows(ctx); err != nil {
			return err
		}
		if err := queries.ClearNewFollows(ctx); err != nil {
			return err
		}
		return queries.ApplyFollowVersion(ctx, version)
	})
}

func (db *store) Close() error { return db.conn.Close() }

func (db *store) knownCandidate(ctx context.Context, did string) (bool, error) {
	return db.queries.KnownCandidate(ctx, did)
}
func (db *store) candidatePDS(ctx context.Context, did string) (string, error) {
	return db.queries.CandidatePDS(ctx, did)
}
func (db *store) saveCandidatePDS(ctx context.Context, did, pds string) error {
	return db.queries.SaveCandidatePDS(ctx, storage.SaveCandidatePDSParams{Did: did, Pds: pds})
}
