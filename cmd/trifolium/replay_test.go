package main

import (
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"testing"
	"time"
)

const owner = "did:example:owner"
const actor = "did:example:member"

var targets = []string{"did:example:a", "did:example:b", "did:example:c"}

func requireOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func openStore(t *testing.T, path string) *service {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	requireOK(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	a := &service{store: store{db}, owner: owner, stream: "stream", targets: map[string]bool{}}
	requireOK(t, a.initialize(t.Context()))
	return a
}
func configure(t *testing.T, a *service) []*cluster {
	t.Helper()
	lists, err := a.setupClusters(t.Context(), map[string]clusterConfig{"garden": {Accounts: targets[:2], Threshold: 1}, "other": {Accounts: targets[1:], Threshold: 2}}, 2)
	requireOK(t, err)
	return lists
}
func execute(t *testing.T, a *service, q string, args ...any) {
	t.Helper()
	_, err := a.Exec(q, args...)
	requireOK(t, err)
}
func pairs(t *testing.T, a *service, q string, args ...any) map[string]string {
	t.Helper()
	rows, err := a.Query(q, args...)
	requireOK(t, err)
	defer rows.Close()
	result := map[string]string{}
	for rows.Next() {
		var k, v string
		requireOK(t, rows.Scan(&k, &v))
		result[k] = v
	}
	requireOK(t, rows.Err())
	return result
}

func FuzzFollowReplay(f *testing.F) {
	f.Add([]byte{1, 4, 64, 192})
	f.Add([]byte{0, 4, 17, 64, 192, 128, 96, 32, 224})
	f.Fuzz(func(t *testing.T, history []byte) {
		a := openStore(t, ":memory:")
		configure(t, a)
		records := map[string]map[string]string{actor: {}, owner: {}}
		seq := 0
		for i, b := range history[:min(len(history), 40)] {
			did, subject := actor, targets[int(b)%3]
			if b&64 != 0 {
				did, subject = owner, actor
			}
			key := fmt.Sprint((b >> 2) % 4)
			op := "update"
			if b&128 != 0 {
				op = "delete"
			}
			frame := func(seq int) []byte {
				return []byte(fmt.Sprintf(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","seq":%d,"time":"2026-01-01T00:00:00Z","collection":"app.bsky.graph.follow","did":%q,"rkey":%q,"operation":%q,"record":{"$type":"app.bsky.graph.follow","subject":%q}}}`, seq, did, key, op, subject))
			}
			if i > 0 && b&32 != 0 {
				requireOK(t, a.ingest(t.Context(), frame(seq)))
			} else {
				seq++
				requireOK(t, a.ingest(t.Context(), frame(seq)))
				if op == "delete" {
					delete(records[did], key)
				} else {
					records[did][key] = subject
				}
			}
			for d, want := range records {
				if got := pairs(t, a, "SELECT rkey,target FROM edges WHERE actor=?", d); !maps.Equal(got, want) {
					t.Fatalf("step %d edges %s: %v want %v", i, d, got, want)
				}
			}
			for n, id := range []string{"garden", "other"} {
				shared := map[string]bool{}
				for _, v := range records[actor] {
					if v == targets[n] || v == targets[n+1] {
						shared[v] = true
					}
				}
				published := b&16 != 0
				execute(t, a, "DELETE FROM published WHERE cluster=?", id)
				if published {
					execute(t, a, "INSERT INTO published VALUES(?,?)", id, actor)
				}
				want := len(shared) >= n+1 && len(records[owner]) == 0
				var member string
				var include bool
				err := a.QueryRow(nextChange, id, n+1, owner).Scan(&member, &include)
				if want == published {
					if !errors.Is(err, sql.ErrNoRows) {
						t.Fatalf("step %d unexpected change %s %v %v", i, member, include, err)
					}
				} else if err != nil || member != actor || include != want {
					t.Fatalf("step %d %s got %s %v %v want %v", i, id, member, include, err, want)
				}
			}
		}
	})
}

func FuzzLegacyRestart(f *testing.F) {
	f.Add(uint8(3), int64(90123))
	f.Fuzz(func(t *testing.T, size uint8, due int64) {
		path := filepath.Join(t.TempDir(), "state.db")
		db, err := sql.Open("sqlite", path)
		requireOK(t, err)
		defer db.Close()
		_, err = db.Exec(`CREATE TABLE watched(target TEXT PRIMARY KEY);INSERT INTO watched VALUES('did:example:a');
 CREATE TABLE items(did TEXT PRIMARY KEY,rkey TEXT UNIQUE);CREATE TABLE published(did TEXT PRIMARY KEY);
 CREATE TABLE descriptions(text TEXT PRIMARY KEY);CREATE TABLE pending(did TEXT PRIMARY KEY);CREATE TABLE retries(did TEXT PRIMARY KEY,due INTEGER);`)
		requireOK(t, err)
		_, err = db.Exec("INSERT INTO pending VALUES(?1); INSERT INTO retries VALUES(?1,?2); INSERT INTO descriptions VALUES('prior')", actor, due)
		requireOK(t, err)
		want := map[string]string{}
		for i := 0; i < int(size)%9; i++ {
			k, d := fmt.Sprint(i), fmt.Sprintf("did:example:m%d", i)
			want[k] = d
			_, err = db.Exec("INSERT INTO items VALUES(?,?);INSERT INTO published VALUES(?)", d, k, d)
			requireOK(t, err)
		}
		requireOK(t, db.Close())
		for pass := 0; pass < 2; pass++ {
			a := openStore(t, path)
			a.owner = "did:plc:zntngpowgd6rorjt3haywj36"
			lists := configure(t, a)
			if pass == 0 && lists[0].list != "at://did:plc:zntngpowgd6rorjt3haywj36/app.bsky.graph.list/5jwgdmah5yhep" {
				t.Fatal("legacy list rebound", lists[0].list)
			}
			if len(lists) != 3-pass {
				t.Fatalf("pass %d active lists %d", pass, len(lists))
			}
			var retry int64
			requireOK(t, a.QueryRow(`SELECT due FROM retries JOIN pending USING(cluster,did) JOIN descriptions USING(cluster) WHERE cluster='default' AND did=? AND text='prior'`, actor).Scan(&retry))
			if retry != due {
				t.Fatalf("retry %d want %d", retry, due)
			}
			if got := pairs(t, a, "SELECT rkey,did FROM items WHERE cluster='default'"); !maps.Equal(got, want) {
				t.Fatalf("pass %d got %v want %v", pass, got, want)
			}
			if pass == 0 {
				deadline := time.Now().Add(time.Hour).Unix() + int64(size)
				execute(t, a, "UPDATE inventory_schedule SET due=?", deadline+300)
				execute(t, a, "UPDATE discovery_schedule SET due=?", deadline)
			} else {
				var actual int64
				requireOK(t, a.QueryRow("SELECT due FROM discovery_schedule LIMIT 1").Scan(&actual))
				delay, err := a.backfillStep(t.Context())
				requireOK(t, err)
				remaining := time.Until(time.Unix(actual, 0))
				if delay < time.Minute*59 || delay < remaining-time.Second || delay > remaining+time.Second {
					t.Fatalf("restart delay %s for remaining %s", delay, remaining)
				}
			}
			requireOK(t, a.Close())
		}
	})
}
