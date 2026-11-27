package main

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluesky-social/indigo/xrpc"
)

func TestExactRecordCannotUndoConcurrentUnfollow(t *testing.T) {
	ctx := t.Context()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err = (store{db}).initialize(ctx); err != nil {
		t.Fatal(err)
	}
	const did = "did:example:actor"
	const target = "did:example:target"
	a := service{store: store{db}, targets: map[string]bool{target: true}}
	a.owner = "did:example:owner"
	accounts := []string{target}
	if _, err = a.setupClusters(ctx, map[string]clusterConfig{"default": {Accounts: accounts, Threshold: 2}}, 2); err != nil {
		t.Fatal(err)
	}
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("rkey") != "abc" || r.URL.Path != "/xrpc/com.atproto.repo.getRecord" {
			t.Error("unexpected request", r.URL)
		}
		if deleted {
			err := a.ingest(ctx, []byte(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","seq":10,"time":"2026-01-01T00:00:00Z","collection":"app.bsky.graph.follow","did":"did:example:actor","rkey":"abc","operation":"delete"}}`))
			if err != nil {
				t.Error(err)
			}
		}
		fmt.Fprint(w, `{"uri":"at://did:example:actor/app.bsky.graph.follow/abc","value":{"$type":"app.bsky.graph.follow","subject":"did:example:target"}}`)
	}))
	defer server.Close()
	for _, want := range []int{1, 0} {
		deleted = want == 0
		res, err := db.Exec("INSERT INTO hints(did,rkey,target) VALUES(?,?,?)", did, "abc", target)
		if err != nil {
			t.Fatal(err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		if err = a.checkHint(ctx, &xrpc.Client{Host: server.URL}, verifyJob{did: did, key: "abc", target: target, id: id}); err != nil {
			t.Fatal(err)
		}
		var count int
		if err = db.QueryRow("SELECT count(*) FROM edges").Scan(&count); err != nil || count != want {
			t.Fatalf("edges=%d, want %d; err=%v", count, want, err)
		}
	}
}

func TestScanPagesPreserveInventoryUntilCompletion(t *testing.T) {
	ctx := t.Context()
	a := openTestService(t)
	const actor = "did:example:actor"
	const target = "did:example:target"
	a.targets[target] = true
	if _, err := a.Exec("INSERT INTO edges VALUES(?,'old',?)", actor, target); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Exec("INSERT INTO scans(did) VALUES(?)", actor); err != nil {
		t.Fatal(err)
	}
	var cursor string
	var response string
	client := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("cursor"); got != cursor {
			t.Errorf("cursor = %q, want %q", got, cursor)
		}
		fmt.Fprint(w, response)
	}))
	defer client.Close()
	scan := func(expectedCursor, body string, wantError bool) {
		t.Helper()
		cursor, response = expectedCursor, body
		err := a.scanPage(ctx, &xrpc.Client{Host: client.URL}, verifyJob{did: actor, version: 1})
		if (err != nil) != wantError {
			t.Fatalf("scan page error = %v, want error %v", err, wantError)
		}
	}
	hasKey := func(key string, want bool) {
		t.Helper()
		var found bool
		if err := a.QueryRow("SELECT EXISTS(SELECT 1 FROM edges WHERE actor=? AND rkey=?)", actor, key).Scan(&found); err != nil || found != want {
			t.Fatalf("key %q present=%v, want %v: %v", key, found, want, err)
		}
	}
	page := func(cursor, key string) string {
		return fmt.Sprintf(`{"cursor":%q,"records":[{"uri":"at://%s/app.bsky.graph.follow/%s","value":{"$type":"app.bsky.graph.follow","subject":%q}}]}`, cursor, actor, key, target)
	}

	scan("", page("p2", "first"), false)
	hasKey("old", true)
	hasKey("first", true)
	scan("p2", `{"records":null}`, true)
	hasKey("old", true)
	scan("p2", page("p2", "bogus"), false)
	hasKey("old", true)
	hasKey("bogus", false)
	scan("", page("p2", "first"), false)
	scan("p2", page("", "second"), false)
	hasKey("old", false)
	hasKey("first", true)
	hasKey("second", true)
}

func TestScanClaimsExcludeInFlightActorsAndIgnoreInvalidatedResult(t *testing.T) {
	ctx := t.Context()
	a := openTestService(t)
	const actor = "did:example:actor"
	const target = "did:example:target"
	if _, err := a.Exec("INSERT INTO scans(did) VALUES(?)", actor); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Exec("INSERT INTO hints(did,rkey,target) VALUES(?,'key',?)", actor, target); err != nil {
		t.Fatal(err)
	}
	job, err := a.claim(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.claim(ctx, true); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("second worker claimed an in-flight actor: %v", err)
	}
	if _, err := a.Exec(dirty, actor); err != nil {
		t.Fatal(err)
	}
	if err := a.savePage(ctx, job, "", "", map[string]string{"key": target}); err != nil {
		t.Fatal(err)
	}
	var edges, version int
	if err := a.QueryRow("SELECT (SELECT count(*) FROM edges),version FROM scans WHERE did=?", actor).Scan(&edges, &version); err != nil || edges != 0 || version != 2 {
		t.Fatalf("stale scan changed state: edges=%d version=%d err=%v", edges, version, err)
	}
}

func openTestService(t *testing.T) *service {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	if err := (store{db}).initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	return &service{store: store{db}, owner: "did:example:owner", targets: map[string]bool{}}
}
