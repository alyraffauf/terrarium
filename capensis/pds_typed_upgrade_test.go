package capensis

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

func openRawAfterFailedUpgrade(t *testing.T, path string) (*sql.DB, error) {
	t.Helper()
	return sql.Open("sqlite", path)
}

// A database that already completed the typed-state upgrade (goose
// version 2) never re-runs the legacy migration. Respelling a PDS under
// the improved NormalizePDS must still move its saved binding and
// membership to the canonical key on upgrade, not create a second list
// for the same PDS or abandon the old list's members.

func seedTypedPDSRows(t *testing.T, path, oldPDS, uri, rkey string) {
	t.Helper()
	db, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.Exec(`INSERT INTO targets(pds,list_uri) VALUES(?,?)`, oldPDS, uri); err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.Exec(`INSERT INTO list_members(pds,did,rkey) VALUES(?,?,?)`, oldPDS, testDID, rkey); err != nil {
		t.Fatal(err)
	}
	// Rewind to the last pre-canonicalization schema version so the next
	// open performs a realistic typed-database upgrade.
	if _, err := db.conn.Exec(`DELETE FROM goose_db_version WHERE version_id>2`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTypedUpgradeRetainsBindingForRespeltPDS(t *testing.T) {
	ctx := context.Background()
	cases := []struct{ old, canonical string }{
		{"https://a.example:0443", "https://a.example"},
		{"https://a.example:08443", "https://a.example:8443"},
		{"https://[0:0:0:0:0:0:0:1]:0443", "https://[::1]"},
		{"http://a.example:080", "http://a.example"},
	}
	for _, tc := range cases {
		t.Run(tc.old, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.sqlite")
			uri := "at://" + ownerDID + "/" + listCollection + "/saved-list"
			seedTypedPDSRows(t, path, tc.old, uri, "saved-item")
			db, err := openStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			saved, err := db.bindList(ctx, Target{PDS: tc.canonical}, ownerDID)
			if err != nil || saved != uri {
				t.Fatalf("binding not retained for %s: %q %v, want %q", tc.canonical, saved, err, uri)
			}
			var count int
			if err := db.conn.QueryRow(`SELECT count(*) FROM targets`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("targets duplicated: %d %v", count, err)
			}
			keys, err := db.MemberKeys(ctx, tc.canonical, testDID)
			if err != nil || len(keys) != 1 || keys[0] != "saved-item" {
				t.Fatalf("membership not retained under canonical key: %v %v", keys, err)
			}
		})
	}
}

// Property: for generated equivalent-spelling histories, an upgrade
// followed by a canonical bind always yields exactly the saved list and
// membership, with one target row. Two different saved lists converging
// on one canonical PDS must fail the upgrade rather than pick a winner.
func TestPropertyTypedUpgradePreservesBindings(t *testing.T) {
	ctx := context.Background()
	groups := []struct {
		canonical string
		forms     []string
	}{
		{"https://a.example", []string{"https://a.example:443", "https://a.example:0443", "https://A.EXAMPLE"}},
		{"https://b.example:8443", []string{"https://b.example:8443", "https://b.example:08443"}},
		{"https://[::1]", []string{"https://[::1]", "https://[0:0:0:0:0:0:0:1]:0443"}},
	}
	rng := rand.New(rand.NewSource(9))
	coverage := map[string]int{}
	for trial := 0; trial < 30; trial++ {
		group := groups[rng.Intn(len(groups))]
		old := group.forms[rng.Intn(len(group.forms))]
		if old == group.canonical {
			coverage["already-canonical"]++
		} else {
			coverage["respelt"]++
		}
		path := filepath.Join(t.TempDir(), "state.sqlite")
		uri := fmt.Sprintf("at://%s/%s/list-%d", ownerDID, listCollection, trial)
		seedTypedPDSRows(t, path, old, uri, "item-1")
		db, err := openStore(path)
		if err != nil {
			t.Fatalf("trial %d (%s): %v", trial, old, err)
		}
		saved, err := db.bindList(ctx, Target{PDS: group.canonical}, ownerDID)
		if err != nil || saved != uri {
			t.Fatalf("trial %d (%s): bind = %q %v, want %q", trial, old, saved, err, uri)
		}
		var count int
		if err := db.conn.QueryRow(`SELECT count(*) FROM targets`).Scan(&count); err != nil || count != 1 {
			t.Fatalf("trial %d (%s): %d target rows", trial, old, count)
		}
		keys, err := db.MemberKeys(ctx, group.canonical, testDID)
		if err != nil || len(keys) != 1 {
			t.Fatalf("trial %d (%s): members %v %v", trial, old, keys, err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if coverage["respelt"] == 0 || coverage["already-canonical"] == 0 {
		t.Fatalf("generator coverage incomplete: %v", coverage)
	}
	t.Logf("typed-upgrade trials: %v", coverage)
}

func TestTypedUpgradeRejectsConflictingConvergedBindings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	db, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	uriA := "at://" + ownerDID + "/" + listCollection + "/list-a"
	uriB := "at://" + ownerDID + "/" + listCollection + "/list-b"
	if _, err := db.conn.Exec(`INSERT INTO targets VALUES('https://a.example:0443',?),('https://a.example',?)`, uriA, uriB); err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.Exec(`DELETE FROM goose_db_version WHERE version_id>2`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openStore(path); err == nil {
		t.Fatal("upgrade silently chose between two different lists for one canonical PDS")
	}
	// Both bindings must survive the rejected upgrade untouched.
	raw, err := openRawAfterFailedUpgrade(t, path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var count int
	if err := raw.QueryRow(`SELECT count(*) FROM targets`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("failed upgrade mutated bindings: %d %v", count, err)
	}
}
