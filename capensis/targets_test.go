package capensis

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/xrpc"
)

func TestListBindingsSurviveMigrationAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = legacy.Exec(`CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
 CREATE TABLE members(did TEXT PRIMARY KEY,rkey TEXT NOT NULL);
 INSERT INTO metadata VALUES('pds','https://first.example');`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec(`INSERT INTO metadata VALUES('owner',?)`, ownerDID); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec(`INSERT INTO members VALUES(?,'existing-item')`, testDID); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	original := "at://" + ownerDID + "/" + listCollection + "/" + recordKey("https://first.example")
	first, err := db.bindList(ctx, Target{PDS: "https://first.example"}, ownerDID)
	if err != nil || first != original {
		t.Fatalf("legacy list changed: %s %v", first, err)
	}
	keys, err := db.MemberKeys(ctx, "https://first.example", testDID)
	if err != nil || len(keys) != 1 || keys[0] != "existing-item" {
		t.Fatalf("legacy membership lost: %v %v", keys, err)
	}
	explicit := "at://" + ownerDID + "/" + listCollection + "/specific-list"
	second, err := db.bindList(ctx, Target{PDS: "https://second.example", ListURI: explicit}, ownerDID)
	if err != nil || second != explicit {
		t.Fatalf("explicit binding failed: %s %v", second, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	second, err = db.bindList(ctx, Target{PDS: "https://second.example"}, ownerDID)
	if err != nil || second != explicit {
		t.Fatalf("restart lost explicit list: %s %v", second, err)
	}
	first, err = db.bindList(ctx, Target{PDS: "https://first.example"}, ownerDID)
	if err != nil || first != original {
		t.Fatalf("reordered configuration changed first list: %s %v", first, err)
	}
	if _, err := db.bindList(ctx, Target{PDS: "https://second.example", ListURI: original}, ownerDID); err == nil {
		t.Fatal("allowed an existing PDS binding to change")
	}
	if _, err := db.bindList(ctx, Target{PDS: "https://third.example", ListURI: explicit}, ownerDID); err == nil {
		t.Fatal("allowed two PDSes to manage the same list")
	}
}

func TestExplicitListUsesItsRecordKey(t *testing.T) {
	const key = "existing-list"
	const uri = "at://" + ownerDID + "/" + listCollection + "/" + key
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/xrpc/com.atproto.repo.getRecord" || r.URL.Query().Get("rkey") != key {
			t.Errorf("unexpected request %s", r.URL)
		}
		fmt.Fprintf(w, `{"value":{"$type":"app.bsky.graph.list","name":%q,"description":"Users on pds.example","purpose":"app.bsky.graph.defs#modlist","createdAt":"2026-11-16T00:00:00Z"}}`, palustre.ListTitle(uri))
	}))
	defer server.Close()
	api := &listTarget{session: newSession(Config{}, nil, &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: ownerDID}}), pds: "https://pds.example", listURI: uri}
	if err := api.ensureList(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("unexpected writes to existing list: %d requests", requests)
	}
}

func TestTargetBindingsNormalizeHostnames(t *testing.T) {
	uri := "at://" + ownerDID + "/" + listCollection + "/specific-list"
	targets, err := ParseTargets([]string{"FIRST.example,https://second.example/"}, []string{"https://first.example/=" + uri})
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 || targets[0].PDS != "https://first.example" || targets[0].ListURI != uri {
		t.Fatalf("unexpected targets %+v", targets)
	}
	if _, err := ParseTargets([]string{"first.example", "https://first.example/"}, nil); err == nil {
		t.Fatal("accepted duplicate normalized PDS")
	}
	if _, err := ParseTargets([]string{"first.example"}, []string{"second.example=" + uri}); err == nil {
		t.Fatal("accepted binding for unwatched PDS")
	}
}
