package main

import (
	"database/sql"
	"fmt"
	"maps"

	"slices"
	"testing"

	"github.com/alyraffauf/terrarium/trifolium/storage"
)

// Version 1 already supported multiple clusters, but retained a single item
// key per actor in each cluster. Exercise that deployed layout independently
// of the older single-list migration and the current schema.
func FuzzMultiClusterUpgrade(f *testing.F) {
	f.Add([]byte{1, 6, 3, 4, 1})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, history []byte) {
		legacy, err := sql.Open("sqlite", ":memory:")
		requireOK(t, err)
		legacy.SetMaxOpenConns(1)
		t.Cleanup(func() { legacy.Close() })
		_, err = legacy.Exec(`
CREATE TABLE items(cluster TEXT NOT NULL,did TEXT,rkey TEXT,PRIMARY KEY(cluster,did));
CREATE UNIQUE INDEX item_keys ON items(cluster,rkey);
CREATE TABLE pending(cluster TEXT NOT NULL,did TEXT,PRIMARY KEY(cluster,did));
CREATE TABLE published(cluster TEXT NOT NULL,did TEXT,PRIMARY KEY(cluster,did));
CREATE TABLE retries(cluster TEXT NOT NULL,did TEXT,due INTEGER,PRIMARY KEY(cluster,did));
CREATE TABLE descriptions(cluster TEXT NOT NULL,text TEXT,PRIMARY KEY(cluster,text));
PRAGMA user_version=1;`)
		requireOK(t, err)
		expected := map[string]string{}
		for i, b := range history[:min(len(history), 18)] {
			id := []string{"garden", "other"}[b%2]
			did := fmt.Sprintf("did:example:member%d", (b>>1)%4)
			key := fmt.Sprintf("r%d", i)
			_, err := legacy.Exec("INSERT OR REPLACE INTO items VALUES(?,?,?)", id, did, key)
			requireOK(t, err)
			expected[id+"/"+did] = key
		}
		a := &service{store: store{legacy}, owner: owner}
		requireOK(t, storage.Migrate(t.Context(), a.DB))
		configure(t, a)
		got := pairs(t, a, "SELECT cluster||'/'||did,rkey FROM items")
		if !maps.Equal(got, expected) {
			t.Fatalf("migration changed membership keys: %v want %v", got, expected)
		}
		const id, did = "garden", "did:example:member0"
		requireOK(t, a.RememberMember(t.Context(), id, did, "another"))
		want := []string{"another"}
		if old, ok := expected[id+"/"+did]; ok {
			want = append(want, old)
		}
		slices.Sort(want)
		keys, err := a.MemberKeys(t.Context(), id, did)
		requireOK(t, err)
		if !slices.Equal(keys, want) {
			t.Fatalf("cannot reconcile multiple keys after upgrade: %v want %v", keys, want)
		}
	})
}

func TestGooseAdoptsVersionTwo(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	requireOK(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()
	_, err = db.Exec(`PRAGMA user_version=2;
CREATE TABLE items(cluster TEXT NOT NULL,did TEXT NOT NULL,rkey TEXT NOT NULL,PRIMARY KEY(cluster,rkey));
INSERT INTO items VALUES ('garden','did:example:actor','first'),('garden','did:example:actor','second');`)
	requireOK(t, err)
	s := store{db}
	for range 2 {
		requireOK(t, storage.Migrate(t.Context(), s.DB))
		keys, err := s.MemberKeys(t.Context(), "garden", "did:example:actor")
		requireOK(t, err)
		if !slices.Equal(keys, []string{"first", "second"}) {
			t.Fatalf("version 2 adoption changed keys: %v", keys)
		}
		var applied int
		requireOK(t, db.QueryRow("SELECT count(*) FROM goose_db_version WHERE version_id BETWEEN 1 AND 3 AND is_applied").Scan(&applied))
		if applied != 3 {
			t.Fatalf("got %d applied Goose migrations", applied)
		}
	}
}
