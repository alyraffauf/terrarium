package capensis

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/xrpc"

	"github.com/alyraffauf/terrarium/palustre"
)

func TestRescansPreserveQueueAndBackoff(t *testing.T) {
	db := testStore(t)
	ctx := context.Background()
	for _, did := range []string{testDID, ownerDID} {
		if err := db.queueScan(ctx, did); err != nil {
			t.Fatal(err)
		}
	}
	first, err := db.next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.retry(ctx, first); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if err := db.queueScan(ctx, first.DID); err != nil {
			t.Fatal(err)
		}
	}
	next, err := db.next(ctx)
	if err != nil || next.DID == first.DID {
		t.Fatalf("rescan bypassed retry: %+v %v", next, err)
	}
	if err := db.finish(ctx, next); err != nil {
		t.Fatal(err)
	}
	var attempts int
	if err := db.conn.QueryRow(`SELECT attempts FROM pending WHERE did=?`, first.DID).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("backoff reset: %d %v", attempts, err)
	}
	if err := db.queueScan(ctx, next.DID); err != nil {
		t.Fatal(err)
	}
	// Make the backfill entry older than the live event.
	if _, err := db.conn.Exec(`UPDATE pending SET ready=1 WHERE did=?`, next.DID); err != nil {
		t.Fatal(err)
	}
	if err := db.ingest(ctx, first.DID, checkpoint{}, false); err != nil {
		t.Fatal(err)
	}
	reopened, err := db.next(ctx)
	if err != nil || reopened.DID != first.DID || reopened.Attempts != 0 || reopened.Generation <= first.Generation {
		t.Fatalf("new event did not supersede backoff and backlog: %+v %v", reopened, err)
	}
}

func TestBindingsRollbackAndCanonicalMigration(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	db, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	uri := "at://" + ownerDID + "/" + listCollection + "/old"
	if _, err := db.bindLists(ctx, []Target{{PDS: "https://a.example", ListURI: uri}, {PDS: "https://b.example", ListURI: uri}}, ownerDID); err == nil {
		t.Fatal("duplicate list accepted")
	}
	var count int
	if err := db.conn.QueryRow(`SELECT count(*) FROM targets`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial bindings: %d %v", count, err)
	}
	db.Close()
	// Build the unversioned schema that existed before Goose was introduced.
	path = filepath.Join(t.TempDir(), "legacy.sqlite")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec(`CREATE TABLE targets(pds TEXT PRIMARY KEY,list_uri TEXT NOT NULL UNIQUE);
 CREATE TABLE list_members(pds TEXT NOT NULL,did TEXT NOT NULL,rkey TEXT NOT NULL,PRIMARY KEY(pds,rkey));
 INSERT INTO targets VALUES('https://a.example:443',?);`, uri); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec(`INSERT INTO list_members VALUES(?,?,?)`, "https://a.example:443", testDID, "item"); err != nil {
		t.Fatal(err)
	}
	legacy.Close()

	db, err = openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	saved, err := db.bindList(ctx, Target{PDS: "https://a.example"}, ownerDID)
	if err != nil || saved != uri {
		t.Fatalf("binding changed: %s %v", saved, err)
	}
	keys, err := db.MemberKeys(ctx, "https://a.example", testDID)
	if err != nil || len(keys) != 1 {
		t.Fatalf("membership lost: %v %v", keys, err)
	}
	for _, raw := range []string{"http://a.example:80", "https://a.example:443"} {
		normalized, err := normalizePDS(raw)
		if err != nil || normalized == raw {
			t.Fatalf("default port not normalized: %s %v", normalized, err)
		}
	}
}

func TestLegacyPendingColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.sqlite")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`CREATE TABLE pending(did TEXT PRIMARY KEY); INSERT INTO pending VALUES(?)`, testDID); err != nil {
		t.Fatal(err)
	}
	raw.Close()
	db, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	work, err := db.next(context.Background())
	if err != nil || work.DID != testDID {
		t.Fatalf("legacy queue lost: %+v %v", work, err)
	}
}

func TestInvalidEventRecoveryDoesNotSkipStorageFailure(t *testing.T) {
	db := testStore(t)
	ctx := context.Background()
	if err := db.ingest(ctx, "", checkpoint{TimeUS: 100, Seq: 10, Endpoint: "stream"}, false); err != nil {
		t.Fatal(err)
	}
	frame := []byte(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#identity","seq":11,"did":"invalid","time":"invalid"}}`)
	if err := ingestFrame(ctx, db, "stream", ownerDID, frame, true); err != nil {
		t.Fatal(err)
	}
	cursor, err := db.resumeCursor(ctx, "stream")
	if err != nil || cursor != 11 {
		t.Fatalf("poison event stuck: %d %v", cursor, err)
	}
	var requested string
	if err := db.conn.QueryRow(`SELECT rescan_requested FROM sync_state WHERE id=1`).Scan(&requested); err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.Exec(`CREATE TRIGGER fail_checkpoint BEFORE UPDATE ON sync_state BEGIN SELECT RAISE(ABORT,'disk failure'); END`); err != nil {
		t.Fatal(err)
	}
	frame = []byte(fmt.Sprintf(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#identity","seq":12,"did":%q,"time":"2026-01-01T00:00:00Z"}}`, testDID))
	if err := ingestFrame(ctx, db, "stream", ownerDID, frame, true); err == nil {
		t.Fatal("storage failure swallowed")
	}
	cursor, err = db.resumeCursor(ctx, "stream")
	if err != nil || cursor != 11 {
		t.Fatalf("failed event checkpointed: %d %v", cursor, err)
	}
}

func TestMovedItemIsNeverDeleted(t *testing.T) {
	db := testStore(t)
	ctx := context.Background()
	pds := "https://pds.example"
	if err := db.RememberMember(ctx, pds, testDID, "moved"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.Exec(`INSERT INTO follows VALUES(?)`, testDID); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/com.atproto.repo.getRecord" {
			t.Errorf("unexpected mutation %s", r.URL.Path)
			http.Error(w, "unexpected", 500)
			return
		}
		mockItem(w, "moved", testDID, "at://"+ownerDID+"/"+listCollection+"/another")
	}))
	defer server.Close()
	api := &listTarget{session: newSession(Config{}, nil, &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: ownerDID}}), pds: pds, listURI: "at://" + ownerDID + "/" + listCollection + "/managed"}
	if err := api.reconcile(ctx, job{DID: testDID}, db); err != nil {
		t.Fatal(err)
	}
	keys, err := db.MemberKeys(ctx, pds, testDID)
	if err != nil || len(keys) != 0 {
		t.Fatalf("stale association retained: %v %v", keys, err)
	}
}

func TestMalformedFollowsAreSkipped(t *testing.T) {
	db := testStore(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"records":[
			{"uri":"at://%s/app.bsky.graph.follow/malformed","value":{"$type":"app.bsky.graph.follow","subject":42}},
			{"uri":"at://%s/app.bsky.graph.follow/invalid","value":{"$type":"app.bsky.graph.follow","subject":"bad"}},
			{"uri":"at://%s/app.bsky.graph.follow/valid","value":{"$type":"app.bsky.graph.follow","subject":%q}}
		]}`, ownerDID, ownerDID, ownerDID, testDID)
	}))
	defer server.Close()
	api := &listTarget{session: newSession(Config{}, nil, &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: ownerDID}})}
	if err := api.syncFollows(context.Background(), db, true); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.conn.QueryRow(`SELECT count(*) FROM follows`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("valid follow lost: %d %v", count, err)
	}
}

func TestInventoryYieldsBetweenPagesAndSchedulesFromCompletion(t *testing.T) {
	db := testStore(t)
	pages := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pages++
		time.Sleep(2 * time.Millisecond)
		if pages == 1 {
			fmt.Fprint(w, `{"records":[],"cursor":"next"}`)
		} else {
			fmt.Fprint(w, `{"records":[]}`)
		}
	}))
	defer server.Close()
	api := &listTarget{session: newSession(Config{}, nil, &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: ownerDID}}), rescanInterval: time.Hour, listURI: "list"}
	scan := &inventoryScan{checks: map[string]time.Time{"list": time.Now().Add(time.Hour)}}
	if err := scan.step(context.Background(), []*listTarget{api}, db); err != nil {
		t.Fatal(err)
	}
	if pages != 1 || scan.cursor != "next" {
		t.Fatal("inventory failed to yield")
	}
	if err := scan.step(context.Background(), []*listTarget{api}, db); err != nil {
		t.Fatal(err)
	}
	if pages != 2 || time.Until(scan.next) < 59*time.Minute {
		t.Fatal("inventory deadline predates completion")
	}
}

func TestRemovalUsesCIDAndRetainsJobOnConcurrentEdit(t *testing.T) {
	db := testStore(t)
	ctx := context.Background()
	pds := "https://pds.example"
	list := "at://" + ownerDID + "/" + listCollection + "/managed"
	if err := db.RememberMember(ctx, pds, testDID, "item"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.Exec(`INSERT INTO follows VALUES(?)`, testDID); err != nil {
		t.Fatal(err)
	}
	checked := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.repo.getRecord":
			mockItem(w, "item", testDID, list)
		case "/xrpc/com.atproto.repo.deleteRecord":
			var input struct {
				SwapRecord string `json:"swapRecord"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
			}
			if input.SwapRecord != "bafyreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
				t.Error("missing CID precondition")
			}
			checked = true
			http.Error(w, `{"error":"InvalidSwap"}`, 400)
		default:
			t.Error("unexpected request", r.URL.Path)
		}
	}))
	defer server.Close()
	api := &listTarget{session: newSession(Config{}, nil, &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: ownerDID}}), pds: pds, listURI: list}
	if err := api.reconcile(ctx, job{DID: testDID}, db); err == nil {
		t.Fatal("concurrent edit accepted")
	}
	keys, err := db.MemberKeys(ctx, pds, testDID)
	if err != nil || len(keys) != 1 || !checked {
		t.Fatalf("failed removal forgotten: %v %v", keys, err)
	}
}

func TestFailedPDSDoesNotRescanHealthyTargets(t *testing.T) {
	db := testStore(t)
	calls := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls[r.URL.Path]++
		if r.URL.Path == "/bad/xrpc/com.atproto.sync.listRepos" {
			http.Error(w, `{"error":"Unavailable"}`, 503)
			return
		}
		fmt.Fprint(w, `{"repos":[]}`)
	}))
	defer server.Close()
	targets := []*listTarget{}
	for _, name := range []string{"bad", "good"} {
		targets = append(targets, &listTarget{session: newSession(Config{}, nil, nil), pds: server.URL + "/" + name, rescanInterval: 6 * time.Hour, target: &xrpc.Client{Host: server.URL + "/" + name, Client: server.Client()}})
	}
	next := make([]time.Time, 2)
	scanDue(context.Background(), targets, db, next, time.Now())
	scanDue(context.Background(), targets, db, next, time.Now().Add(2*time.Minute))
	if calls["/bad/xrpc/com.atproto.sync.listRepos"] != 2 || calls["/good/xrpc/com.atproto.sync.listRepos"] != 1 {
		t.Fatalf("failure retried healthy target: %v", calls)
	}
}

func TestTypedStateMigrationPreservesResumeAndPendingWork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-state.sqlite")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	// This fixture is the unversioned schema, independent of the current migrations.
	_, err = legacy.Exec(`CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
 INSERT INTO metadata VALUES('owner','did:plc:bbbbbbbbbbbbbbbbbbbbbbbb'),('cursor','10000000'),('stream_seq','42'),('stream_endpoint','wss://old.example'),('follow_version','8'),('follow_applied','7'),('rescan_requested','1'),('invalid_frame','bad frame');
 CREATE TABLE pending(did TEXT PRIMARY KEY,generation INTEGER NOT NULL DEFAULT 1,attempts INTEGER NOT NULL DEFAULT 0,ready INTEGER NOT NULL DEFAULT 0);
 INSERT INTO pending VALUES('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa',9,3,2000000000);
 CREATE TABLE follows(did TEXT PRIMARY KEY);
 INSERT INTO follows VALUES('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa');`)
	if err != nil {
		t.Fatal(err)
	}
	legacy.Close()
	for range 2 {
		db, err := openStore(path)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		cursor, err := db.resumeCursor(ctx, "wss://old.example")
		if err != nil || cursor != 42 {
			t.Fatalf("sequence lost: %d %v", cursor, err)
		}
		cursor, err = db.resumeCursor(ctx, "wss://new.example")
		if err != nil || cursor != 5000000 {
			t.Fatalf("timestamp lost: %d %v", cursor, err)
		}
		state, err := db.queries.GetState(ctx)
		if err != nil || state.FollowVersion != 8 || state.FollowApplied != 7 || state.RescanRequested != 1 || state.InvalidFrame != "bad frame" {
			t.Fatalf("state lost: %+v %v", state, err)
		}
		followed, err := db.isFollowed(ctx, testDID)
		if err != nil || !followed {
			t.Fatalf("follow exclusion lost: %v %v", followed, err)
		}
		var generation, attempts, ready int64
		if err := db.conn.QueryRow(`SELECT generation,attempts,ready FROM pending WHERE did=?`, testDID).Scan(&generation, &attempts, &ready); err != nil || generation != 9 || attempts != 3 || ready != 2000000000 {
			t.Fatalf("pending work changed: %d %d %d %v", generation, attempts, ready, err)
		}
		db.Close()
	}
}

func TestFollowSnapshotPreservesConcurrentInvalidation(t *testing.T) {
	db := testStore(t)
	ctx := context.Background()
	version, _, err := db.followVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ingest(ctx, "", checkpoint{TimeUS: 100}, true); err != nil {
		t.Fatal(err)
	}
	if err := db.applyFollowSnapshot(ctx, version, map[string]bool{testDID: true}); !errors.Is(err, errFollowSnapshotSuperseded) {
		t.Fatalf("superseded exclusion refresh returned %v", err)
	}
	if excluded, err := db.isFollowed(ctx, testDID); err != nil || excluded {
		t.Fatalf("superseded exclusion refresh installed its results: %t %v", excluded, err)
	}
	current, applied, err := db.followVersion(ctx)
	if err != nil || current == applied {
		t.Fatalf("concurrent follow event lost: %d %d %v", current, applied, err)
	}
}

func TestRejectedListPausesMembershipAndLeavesNewBindingUnset(t *testing.T) {
	db := testStore(t)
	ctx := context.Background()
	const pds = "https://first.example"
	uri := "at://" + ownerDID + "/" + listCollection + "/list"
	managed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/xrpc/com.atproto.repo.getRecord" || r.URL.Query().Get("collection") != listCollection {
			t.Errorf("paused list allowed an unexpected request: %s", r.URL)
			http.Error(w, `{"error":"UnexpectedRequest"}`, http.StatusBadRequest)
			return
		}
		name, description := "Unrelated list", "Managed by someone else"
		if managed {
			name, description = palustre.ListTitle(uri), listName(pds)
		}
		fmt.Fprintf(w, `{"value":{"$type":%q,"name":%q,"description":%q,"purpose":"app.bsky.graph.defs#modlist","createdAt":"2026-01-01T00:00:00Z"}}`, listCollection, name, description)
	}))
	defer server.Close()
	api := newSession(Config{Targets: []Target{{PDS: pds, ListURI: uri}}}, server.Client(), &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: ownerDID}})
	targets, err := api.prepareTargets(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	target := targets[0]
	assertPaused := func() {
		t.Helper()
		if err := target.checkList(ctx, db); !errors.Is(err, palustre.ErrUnrelatedList) {
			t.Fatalf("unexpected list check result: %v", err)
		}
		if err := target.reconcileSubject(ctx, subjectState{did: testDID}, db); err != nil {
			t.Fatal(err)
		}
	}
	assertPaused()
	if _, err := db.queries.GetList(ctx, pds); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rejected list has a saved binding: %v", err)
	}
	managed = true
	if err := target.checkList(ctx, db); err != nil {
		t.Fatal(err)
	}
	if bound, err := db.queries.GetList(ctx, pds); err != nil || bound != uri {
		t.Fatalf("verified binding missing: %q %v", bound, err)
	}
	managed = false
	assertPaused()
}
