package capensis

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/xrpc"

	"github.com/alyraffauf/terrarium/internal/followers"
	m "github.com/alyraffauf/terrarium/internal/syncmodel"
)

func reconcileTargets(ctx context.Context, targets []*listTarget, work job, db *store) error {
	owner := targets[0].Client.Auth.Did
	return checkAccount(ctx, targets, owner, work, db).apply(ctx, db)
}

func FuzzServiceMembership(f *testing.F) {
	m.Service(f, true, func(t *testing.T, w *m.World) func(string, bool, int, bool) error {
		path := t.TempDir() + "/store"
		db, err := openStore(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		client := &http.Client{Transport: w}
		owner := &xrpc.Client{Host: "https://owner.example", Client: client, Auth: &xrpc.AuthInfo{Did: m.Owner}}
		api := newSession(Config{WriteInterval: time.Nanosecond, Slingshot: "https://resolver.example"}, client, owner)
		return func(did string, excluded bool, paused int, restart bool) error {
			if restart {
				if err := db.Close(); err != nil {
					return err
				}
				db, err = openStore(path)
				if err != nil {
					return err
				}
			}
			ctx := t.Context()
			following := map[string]bool{}
			if excluded {
				following[m.Subject] = true
			}
			if err := db.ApplyFollowSnapshot(ctx, 0, following); err != nil {
				return err
			}
			var targets []*listTarget
			for i := range 2 {
				target := listTarget{session: api, pds: m.Hosts[i], listURI: m.List(i), target: &xrpc.Client{Host: m.Hosts[i], Client: client}}
				target.paused.Store(i == paused)
				targets = append(targets, &target)
			}
			if err := db.Ingest(ctx, did, followers.Checkpoint{}, false); err != nil {
				return err
			}
			work := job{DID: did}
			if err := db.conn.QueryRow("SELECT generation FROM pending WHERE did=?", did).Scan(&work.Generation); err != nil {
				return err
			}
			return reconcileTargets(ctx, targets, work, db)
		}
	})
}

func TestOwnerChangeDuringMembershipWrite(t *testing.T) {
	db, err := openStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := t.Context()
	if err := db.ApplyFollowSnapshot(ctx, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.queueScan(ctx, m.Subject); err != nil {
		t.Fatal(err)
	}
	work, err := db.next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w := &m.World{Active: true, Follows: 1}
	w.BeforeRecordRead = func() {
		if err := db.Ingest(ctx, "", followers.Checkpoint{}, true); err != nil {
			t.Error(err)
		}
	}
	client := &http.Client{Transport: w}
	api := newSession(Config{WriteInterval: time.Nanosecond, Slingshot: "https://resolver.example"}, client, &xrpc.Client{Host: "https://owner.example", Client: client, Auth: &xrpc.AuthInfo{Did: m.Owner}})
	target := &listTarget{pds: m.Hosts[0], session: api, listURI: m.List(0), target: &xrpc.Client{Host: m.Hosts[0], Client: client}}
	if err := reconcileTargets(ctx, []*listTarget{target}, work, db); !errors.Is(err, followers.ErrSnapshotSuperseded) {
		t.Fatalf("stale reconciliation: %v", err)
	}
	if w.BeforeRecordRead != nil || w.RecordCount() != 0 {
		t.Fatal("expected owner invalidation before write")
	}
	next, err := db.next(ctx)
	if err != nil || next != work {
		t.Fatalf("deferred job %+v want %+v %v", next, work, err)
	}
}

func TestInterruptedRemovalAfterMigration(t *testing.T) {
	path := t.TempDir() + "/db"
	db, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	w := &m.World{Unresolved: true, RejectDelete: "old"}
	client := &http.Client{Transport: w}
	api := newSession(Config{WriteInterval: time.Nanosecond, Slingshot: "https://resolver.example"}, client, &xrpc.Client{Host: "https://owner.example", Client: client, Auth: &xrpc.AuthInfo{Did: m.Owner}})
	if err := db.ApplyFollowSnapshot(ctx, 0, nil); err != nil {
		t.Fatal(err)
	}
	keys := []string{"old", "new"}
	var targets []*listTarget
	for i, key := range keys {
		id := m.Hosts[i]
		target := &listTarget{session: api, pds: id, listURI: m.List(i), target: &xrpc.Client{Host: id, Client: client}}
		targets = append(targets, target)
		w.SeedItem(key, m.List(i), m.Subject)
		if err := db.RememberMember(ctx, id, m.Subject, key); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.queueScan(ctx, m.Subject); err != nil {
		t.Fatal(err)
	}
	for attempt := range 2 {
		work, err := db.next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		err = reconcileTargets(ctx, targets, work, db)
		if (err != nil) != (attempt == 0) {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if attempt == 0 {
			if w.Record("old") == nil || w.Record("new") != nil {
				t.Fatal("healthy target failed to progress independently")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = openStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			w.RejectDelete = ""
			if err := db.Ingest(ctx, m.Subject, followers.Checkpoint{}, false); err != nil {
				t.Fatal(err)
			}
		}
	}
	if w.RecordCount() != 0 {
		t.Fatal("retry retained deleted subject")
	}
}

func TestRejectedTargetCanResumeAfterRecordRepair(t *testing.T) {
	db, err := openStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w := &m.World{}
	client := &http.Client{Transport: w}
	api := newSession(Config{WriteInterval: time.Nanosecond}, client, &xrpc.Client{Host: "https://owner.example", Client: client, Auth: &xrpc.AuthInfo{Did: m.Owner}})
	bad := &listTarget{session: api, pds: m.Hosts[0], listURI: m.List(0)}
	good := &listTarget{session: api, pds: m.Hosts[1], listURI: m.List(1)}
	w.SeedItem("list0", m.List(0), m.Subject)
	if err := bad.checkList(t.Context(), db); err == nil || !bad.paused.Load() {
		t.Fatalf("conflicting list accepted: %v", err)
	}
	if err := good.checkList(t.Context(), db); err != nil || good.paused.Load() {
		t.Fatalf("healthy list paused: %v", err)
	}
	var count int
	if err := db.conn.QueryRow("SELECT count(*) FROM targets").Scan(&count); err != nil || count != 1 {
		t.Fatalf("bound %d lists: %v", count, err)
	}
	bad.listURI = m.List(2)
	if err := bad.checkList(t.Context(), db); err != nil || bad.paused.Load() {
		t.Fatalf("could not resume on repaired binding: %v", err)
	}
}

func TestRetryPreservesMovedListItem(t *testing.T) {
	db, err := openStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := t.Context()
	w := &m.World{MoveOnDelete: "key"}
	w.SeedItem("key", m.List(0), m.Subject)
	client := &http.Client{Transport: w}
	api := newSession(Config{Slingshot: "https://resolver.example", WriteInterval: time.Nanosecond}, client, &xrpc.Client{Client: client, Host: "https://owner.example", Auth: &xrpc.AuthInfo{Did: m.Owner}})
	targets := []*listTarget{{pds: m.Hosts[0], listURI: m.List(0), session: api, target: &xrpc.Client{Host: m.Hosts[0], Client: client}}}
	if err := db.RememberMember(ctx, m.Hosts[0], m.Subject, "key"); err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyFollowSnapshot(ctx, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.queueScan(ctx, m.Subject); err != nil {
		t.Fatal(err)
	}
	work, err := db.next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconcileTargets(ctx, targets, work, db); err == nil {
		t.Fatal("accepted conflicted delete")
	}
	retry, err := db.next(ctx)
	if err != nil || retry != work {
		t.Fatalf("lost retry %+v %v", retry, err)
	}
	w.MoveOnDelete = ""
	if err := reconcileTargets(ctx, targets, retry, db); err != nil {
		t.Fatal(err)
	}
	if w.Record("key")["list"] != m.List(2) {
		t.Fatal("removed externally moved item")
	}
	keys, err := db.MemberKeys(ctx, m.Hosts[0], m.Subject)
	if err != nil || len(keys) != 0 {
		t.Fatalf("stale membership keys %v %v", keys, err)
	}
}

// A public read can finish after a newer event for the same account or a
// change to the owner's exclusions. Only the latest read may change the list.
func FuzzBufferedMembership(f *testing.F) {
	f.Add([]byte{0, 2, 1, 3, 2, 3})
	f.Add([]byte{0, 2, 4, 3, 5, 2, 3})
	f.Fuzz(func(t *testing.T, history []byte) {
		ctx := t.Context()
		db, err := openStore(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		world := &m.World{}
		client := &http.Client{Transport: world}
		api := newSession(Config{WriteInterval: time.Nanosecond, Slingshot: "https://resolver.example"}, client, &xrpc.Client{Host: "https://owner.example", Client: client, Auth: &xrpc.AuthInfo{Did: m.Owner}})
		targets := []*listTarget{{pds: m.Hosts[0], listURI: m.List(0), session: api, target: &xrpc.Client{Host: m.Hosts[0], Client: client}}}
		if err := db.ApplyFollowSnapshot(ctx, 0, nil); err != nil {
			t.Fatal(err)
		}
		var checked *checkedAccount
		var queued, followed, member, capturedMember bool
		var version, capturedVersion int
		for step, b := range history[:min(len(history), 40)] {
			switch b % 6 {
			case 0, 1:
				world.Active = b%6 == 0
				version++
				queued = true
				if err := db.Ingest(ctx, m.Subject, followers.Checkpoint{}, false); err != nil {
					t.Fatal(err)
				}
			case 2:
				if !queued {
					continue
				}
				work, err := db.next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				read := checkAccount(ctx, targets, m.Owner, work, db)
				checked = &read
				capturedVersion, capturedMember = version, world.Active && !followed
			case 3:
				if checked == nil {
					continue
				}
				err := checked.apply(ctx, db)
				checked = nil
				if version != capturedVersion {
					if !errors.Is(err, followers.ErrSnapshotSuperseded) {
						t.Fatalf("step %d accepted superseded read: %v", step, err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					member, queued = capturedMember, false
				}
			case 4, 5:
				next := b%6 == 4
				if next != followed {
					version++
					queued = true
				}
				followed = next
				follows := map[string]bool{}
				if followed {
					follows[m.Subject] = true
				}
				if err := db.ApplyFollowSnapshot(ctx, 0, follows); err != nil {
					t.Fatal(err)
				}
			}
			if (world.RecordCount() == 1) != member || world.RecordCount() > 1 {
				t.Fatalf("step %d has %d items, want member=%t", step, world.RecordCount(), member)
			}
		}
	})
}
