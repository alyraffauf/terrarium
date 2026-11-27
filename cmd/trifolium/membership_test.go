package main

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
)

func TestTrifoliumAndOwnerFollowChanges(t *testing.T) {
	ctx := t.Context()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	if err = (store{db}).initialize(ctx); err != nil {
		t.Fatal(err)
	}
	const owner = "did:example:owner"
	const actor = "did:example:actor"
	const first = "did:example:first"
	const second = "did:example:second"
	a := service{store: store{db}, owner: owner, targets: map[string]bool{first: true, second: true}}
	a.owner = "did:example:owner"
	accounts := []string{first, second}
	if _, err = a.setupClusters(ctx, map[string]clusterConfig{"default": {Accounts: accounts, Threshold: 2}}, 2); err != nil {
		t.Fatal(err)
	}
	seq := 0
	event := func(did, key, op, target string) {
		t.Helper()
		seq++
		raw := fmt.Sprintf(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","seq":%d,"time":"2026-01-01T00:00:00Z","collection":"app.bsky.graph.follow","did":%q,"rkey":%q,"operation":%q,"record":{"$type":"app.bsky.graph.follow","subject":%q}}}`, seq, did, key, op, target)
		if err := a.ingest(ctx, []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	check := func(threshold int, want string) {
		t.Helper()
		var did string
		var include bool
		err := db.QueryRow(nextChange, defaultCluster, threshold, owner).Scan(&did, &include)
		if errors.Is(err, sql.ErrNoRows) && want == "none" {
			return
		}
		if err != nil || did != actor || fmt.Sprint(include) != want {
			t.Fatalf("threshold %d: %s %v %v; want %s", threshold, did, include, err, want)
		}
	}
	event(actor, "a", "create", first)
	event(actor, "duplicate", "create", first)
	check(1, "true")
	check(2, "none")
	event(actor, "b", "create", second)
	check(2, "true")
	check(3, "none")
	if _, err = db.Exec("INSERT INTO published VALUES('default',?)", actor); err != nil {
		t.Fatal(err)
	}
	check(3, "false")
	if _, err = db.Exec(dirty, owner); err != nil {
		t.Fatal(err)
	}
	event(owner, "exclusion", "create", actor)
	check(2, "false")
	event(owner, "exclusion", "delete", "")
	check(2, "none")
	if _, err = db.Exec("DELETE FROM published WHERE cluster='default' AND did=?", actor); err != nil {
		t.Fatal(err)
	}
	check(2, "none")
	if _, err = db.Exec("DELETE FROM scans WHERE did=?", owner); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("INSERT INTO published VALUES('default',?)", actor); err != nil {
		t.Fatal(err)
	}
	event(actor, "duplicate", "delete", "")
	check(2, "none")
	if _, err = db.Exec(dirty, actor); err != nil {
		t.Fatal(err)
	}
	event(actor, "a", "delete", "")
	check(2, "false")
	if err = a.savePage(ctx, verifyJob{did: actor, version: 1}, "", "", map[string]string{"a": first, "b": second}); err != nil {
		t.Fatal(err)
	}
	check(2, "false")
}
