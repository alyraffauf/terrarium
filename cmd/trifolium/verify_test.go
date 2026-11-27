package main

import (
	"database/sql"
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
	if _, err = db.Exec(schema); err != nil {
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
			err := a.ingest(ctx, []byte(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","seq":10,"collection":"app.bsky.graph.follow","did":"did:example:actor","rkey":"abc","operation":"delete"}}`))
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
