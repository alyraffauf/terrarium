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
	if _, err = db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	const owner = "did:example:owner"
	const actor = "did:example:actor"
	const first = "did:example:first"
	const second = "did:example:second"
	a := service{store: store{db}, owner: owner, targets: map[string]bool{first: true, second: true}}
	if err = a.configure(ctx, owner, a.targets); err != nil {
		t.Fatal(err)
	}
	seq := 0
	event := func(did, key, op, target string) {
		t.Helper()
		seq++
		raw := fmt.Sprintf(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","seq":%d,"collection":"app.bsky.graph.follow","did":%q,"rkey":%q,"operation":%q,"record":{"subject":%q}}}`, seq, did, key, op, target)
		if err := a.ingest(ctx, []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	check := func(threshold int, want string) {
		t.Helper()
		var did string
		var include bool
		err := db.QueryRow(nextChange, threshold, owner).Scan(&did, &include)
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
	if _, err = db.Exec("INSERT INTO published VALUES(?)", actor); err != nil {
		t.Fatal(err)
	}
	check(3, "false")
	event(owner, "exclusion", "create", actor)
	check(2, "false")
	event(owner, "exclusion", "delete", "")
	check(2, "none")
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
