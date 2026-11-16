package helix

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
)

const ownerDID = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
const followerDID = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
const firstDID = "did:plc:cccccccccccccccccccccccc"
const secondDID = "did:plc:dddddddddddddddddddddddd"

func testStore(t *testing.T) *store {
	t.Helper()
	db, err := openStore(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
func frame(did, operation, subject string, seq int) []byte {
	record := "null"
	if operation != "delete" {
		record = fmt.Sprintf(`{"subject":%q}`, subject)
	}
	return []byte(fmt.Sprintf(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","did":%q,"seq":%d,"time":"2026-11-16T00:00:00Z","collection":"app.bsky.graph.follow","operation":%q,"rkey":"follow-key","record":%s}}`, did, seq, operation, record))
}

func TestFollowAndDeleteSurviveRestartAndCheckpointAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	db, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	watched := map[string]bool{firstDID: true}
	if err := ingestFrame(ctx, db, "stream", ownerDID, frame(followerDID, "create", firstDID, 10), watched, false); err != nil {
		t.Fatal(err)
	}
	work, err := db.next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.finish(ctx, work); err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, err = openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := ingestFrame(ctx, db, "stream", ownerDID, frame(followerDID, "delete", "", 11), watched, false); err != nil {
		t.Fatal(err)
	}
	work, err = db.next(ctx)
	if err != nil || work.DID != followerDID {
		t.Fatalf("delete lost: %+v %v", work, err)
	}
	if err := ingestFrame(ctx, db, "stream", ownerDID, frame(followerDID, "update", secondDID, 12), watched, false); err != nil {
		t.Fatal(err)
	}
	if err := db.finish(ctx, work); err != nil {
		t.Fatal(err)
	}
	if _, err := db.next(ctx); err != nil {
		t.Fatal("newer edit lost", err)
	}
	if _, err := db.conn.Exec(`CREATE TRIGGER fail_checkpoint BEFORE UPDATE ON sync_state BEGIN SELECT RAISE(ABORT,'disk failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := ingestFrame(ctx, db, "stream", ownerDID, frame(secondDID, "create", firstDID, 13), watched, false); err == nil {
		t.Fatal("storage error ignored")
	}
	cursor, err := db.resumeCursor(ctx, "stream")
	if err != nil || cursor != 12 {
		t.Fatalf("checkpoint skipped failed work: %d %v", cursor, err)
	}
	known, err := db.knownCandidate(ctx, secondDID)
	if err != nil || known {
		t.Fatalf("partial candidate committed: %v %v", known, err)
	}
}

func TestMultipleListsFollowExclusionsAndDeletion(t *testing.T) {
	ctx := context.Background()
	db := testStore(t)
	outgoing := []string{firstDID, secondDID}
	ownerFollows := false
	failPage := false
	active := true
	resolve := true
	records := map[string]json.RawMessage{}
	puts := 0
	resolutions := 0
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/xrpc/blue.microcosm.identity.resolveMiniDoc":
			resolutions++
			if !resolve {
				http.Error(w, `{"error":"NotFound"}`, 404)
				return
			}
			json.NewEncoder(w).Encode(map[string]string{"did": followerDID, "pds": server.URL})
		case "/xrpc/com.atproto.sync.getRepoStatus":
			json.NewEncoder(w).Encode(map[string]any{"did": followerDID, "active": active})
		case "/xrpc/com.atproto.repo.listRecords":
			if failPage {
				http.Error(w, `{"error":"Unavailable"}`, 503)
				return
			}
			subjects := outgoing
			if r.URL.Query().Get("repo") == ownerDID {
				subjects = nil
				if ownerFollows {
					subjects = []string{followerDID}
				}
			}
			values := []any{}
			for _, did := range subjects {
				values = append(values, map[string]any{"value": map[string]string{"subject": did}})
			}
			json.NewEncoder(w).Encode(map[string]any{"records": values})
		case "/xrpc/com.atproto.repo.getRecord":
			key := r.URL.Query().Get("rkey")
			record, ok := records[key]
			if !ok {
				http.Error(w, `{"error":"RecordNotFound"}`, 404)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"uri": "at://" + ownerDID + "/" + itemCollection + "/" + key, "cid": "bafyreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "value": record})
		case "/xrpc/com.atproto.repo.putRecord":
			var input struct {
				Rkey       string
				Record     json.RawMessage
				SwapRecord json.RawMessage
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
			}
			if string(input.SwapRecord) != "null" {
				t.Error("creation did not use null precondition")
			}
			records[input.Rkey] = input.Record
			puts++
			fmt.Fprint(w, `{}`)
		case "/xrpc/com.atproto.repo.deleteRecord":
			var input struct {
				Rkey       string
				SwapRecord string
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
			}
			if input.SwapRecord == "" {
				t.Error("deletion lacked CID precondition")
			}
			delete(records, input.Rkey)
			fmt.Fprint(w, `{}`)
		default:
			t.Error("unexpected request", r.URL.Path)
			http.Error(w, `{"error":"Unexpected"}`, 500)
		}
	}))
	defer server.Close()
	api := newSession(Config{Slingshot: server.URL}, server.Client(), &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: ownerDID}})
	targets := []*listTarget{{session: api, actor: firstDID, listURI: "at://" + ownerDID + "/" + listCollection + "/first"}, {session: api, actor: secondDID, listURI: "at://" + ownerDID + "/" + listCollection + "/second"}}
	run := func(did string) error {
		api.LastWrite = time.Time{}
		return reconcileTargets(ctx, targets, job{DID: did}, db)
	}
	if err := api.syncFollows(ctx, db, true); err != nil {
		t.Fatal(err)
	}
	if err := run(followerDID); err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || resolutions != 1 {
		t.Fatalf("multi-target membership: %d records, %d resolutions", len(records), resolutions)
	}
	if err := run(followerDID); err != nil {
		t.Fatal(err)
	}
	if puts != 2 {
		t.Fatal("unchanged members rewritten")
	}
	failPage = true
	if err := run(followerDID); err == nil {
		t.Fatal("partial snapshot accepted")
	}
	if len(records) != 2 {
		t.Fatal("failure removed memberships")
	}
	failPage = false
	outgoing = []string{secondDID}
	if err := run(followerDID); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatal("unfollow not removed")
	}
	ownerFollows = true
	watched := map[string]bool{firstDID: true, secondDID: true}
	if err := ingestFrame(ctx, db, "stream", ownerDID, frame(ownerDID, "create", followerDID, 20), watched, false); err != nil {
		t.Fatal(err)
	}
	if err := api.syncFollows(ctx, db, false); err != nil {
		t.Fatal(err)
	}
	if err := run(followerDID); err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatal("owner's follow remained listed")
	}
	if err := run(ownerDID); err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatal("owner added")
	}
	ownerFollows = false
	if err := ingestFrame(ctx, db, "stream", ownerDID, frame(ownerDID, "delete", "", 21), watched, false); err != nil {
		t.Fatal(err)
	}
	if err := api.syncFollows(ctx, db, false); err != nil {
		t.Fatal(err)
	}
	if err := run(followerDID); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatal("owner unfollow did not restore membership")
	}
	active = false
	resolve = false
	if err := run(followerDID); err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatal("deleted account retained")
	}
}

func TestBackfillPaginatesAndRechecksFormerMembers(t *testing.T) {
	db := testStore(t)
	ctx := context.Background()
	pages := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/app.bsky.graph.getFollowers" || r.URL.Query().Get("actor") != firstDID {
			t.Error("wrong follower request", r.URL)
		}
		pages++
		if pages == 1 {
			fmt.Fprintf(w, `{"followers":[{"did":%q}],"cursor":"next"}`, followerDID)
		} else {
			if r.URL.Query().Get("cursor") != "next" {
				t.Error("missing cursor")
			}
			fmt.Fprintf(w, `{"followers":[{"did":%q}]}`, secondDID)
		}
	}))
	defer server.Close()
	api := &listTarget{session: newSession(Config{AppView: server.URL}, server.Client(), nil), actor: firstDID}
	if err := db.RememberMember(ctx, firstDID, ownerDID, "old"); err != nil {
		t.Fatal(err)
	}
	if err := api.backfill(ctx, db); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for {
		work, err := db.next(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		seen[work.DID] = true
		if err := db.finish(ctx, work); err != nil {
			t.Fatal(err)
		}
	}
	if pages != 2 || !seen[followerDID] || !seen[secondDID] || !seen[ownerDID] {
		t.Fatalf("incomplete backfill: %v", seen)
	}
}

func TestListBindingsUseResolvedDIDs(t *testing.T) {
	db := testStore(t)
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"did":%q,"pds":"https://pds.example"}`, firstDID)
	}))
	defer server.Close()
	api := newSession(Config{Slingshot: server.URL, Targets: []Target{{Actor: "old.example"}}}, server.Client(), &xrpc.Client{Auth: &xrpc.AuthInfo{Did: ownerDID}})
	first, err := api.prepareTargets(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	api.cfg.Targets = []Target{{Actor: "new.example"}}
	second, err := api.prepareTargets(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if first[0].listURI != second[0].listURI || second[0].actor != firstDID {
		t.Fatal("handle change replaced list")
	}
	api.cfg.Targets = []Target{{Actor: "new.example"}, {Actor: firstDID}}
	if _, err := api.prepareTargets(ctx, db); err == nil {
		t.Fatal("duplicate target DID accepted")
	}
}
