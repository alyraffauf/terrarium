package capensis

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/alyraffauf/terrarium/internal/syncmodel"
)

func FuzzDurableState(f *testing.F) {
	syncmodel.Fuzz(f, func(t *testing.T, path string) syncmodel.API {
		db, err := openStore(path)
		if err != nil {
			t.Fatal(err)
		}
		ctx := t.Context()
		return syncmodel.API{Store: db, Close: db.Close,
			Event: func(d, e string, n, ts int64, dirty bool) error {
				return db.ingest(ctx, d, checkpoint{Endpoint: e, Seq: n, TimeUS: ts}, dirty)
			},
			Recover: func(e string, n int64) error { return db.recoverStream(ctx, e, n, "invalid") },
			Cursor:  func(e string) (int64, error) { return db.resumeCursor(ctx, e) },
			Scan:    func(d string) error { return db.queueScan(ctx, d) },
			Members: func(a string) error { return db.queueMembers(ctx, a) },
			Next:    func() (syncmodel.Job, error) { j, e := db.next(ctx); return syncmodel.Job(j), e },
			Finish:  func(j syncmodel.Job) error { return db.finish(ctx, job(j)) },
			Retry: func(j syncmodel.Job) error {
				// Freeze a real retry deadline until the trace advances logical time.
				start := time.Now().Unix()
				if err := db.retry(ctx, job(j)); err != nil {
					return err
				}
				_, err := db.conn.Exec("UPDATE pending SET ready=253402300799 WHERE did=? AND generation=? AND ready>?", j.DID, j.Generation, start)
				return err
			},
			Elapse: func() error {
				_, err := db.conn.Exec("UPDATE pending SET ready=unixepoch() WHERE ready=253402300799")
				return err
			},
			Snapshot: func(v int64, m map[string]bool) error { return db.applyFollowSnapshot(ctx, v, m) },
			Followed: func(d string) (bool, error) { return db.isFollowed(ctx, d) },
			Versions: func() (int64, int64, error) { return db.followVersion(ctx) },
		}
	})
}

func TestEventCheckpointRollbackAndGapReset(t *testing.T) {
	db, err := openStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := t.Context()
	const ep = "wss://jet.example"
	if err := db.ingest(ctx, "", checkpoint{Seq: 9, Endpoint: ep}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.Exec("CREATE TRIGGER reject_checkpoint BEFORE UPDATE OF stream_seq ON sync_state BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
		t.Fatal(err)
	}
	if err := db.ingest(ctx, "did:plc:new", checkpoint{Seq: 10, Endpoint: ep}, true); err == nil {
		t.Fatal("acknowledged uncommitted checkpoint")
	}
	if seq, err := db.resumeCursor(ctx, ep); err != nil || seq != 9 {
		t.Fatalf("cursor %d %v", seq, err)
	}
	if _, err := db.next(ctx); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("queued work from aborted transaction: %v", err)
	}
	if v, _, err := db.followVersion(ctx); err != nil || v != 0 {
		t.Fatalf("owner version %d %v", v, err)
	}
	if _, err := db.conn.Exec("DROP TRIGGER reject_checkpoint"); err != nil {
		t.Fatal(err)
	}
	if err := db.resetStream(ctx); err != nil {
		t.Fatal(err)
	}
	if need, err := db.takeRescan(ctx); err != nil || !need {
		t.Fatalf("rescan %t %v", need, err)
	}
	if err := db.ingest(ctx, "", checkpoint{Seq: 1, Endpoint: ep}, false); err != nil {
		t.Fatal(err)
	}
	if seq, err := db.resumeCursor(ctx, ep); err != nil || seq != 1 {
		t.Fatalf("new cursor %d %v", seq, err)
	}
}
