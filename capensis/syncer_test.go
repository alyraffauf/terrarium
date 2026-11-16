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
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gorilla/websocket"
)

const testDID = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
const ownerDID = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"

func testStore(t *testing.T) *store {
	t.Helper()
	db, err := openStore(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestCheckpointAndNewerEventSurviveRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	db, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ingest(ctx, testDID, checkpoint{TimeUS: 100}, false); err != nil {
		t.Fatal(err)
	}
	first, err := db.next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ingest(ctx, testDID, checkpoint{TimeUS: 101}, true); err != nil {
		t.Fatal(err)
	}
	if err := db.finish(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.next(ctx); err != nil {
		t.Fatalf("newer event lost: %v", err)
	}
	var cursor, version int64
	if err := db.conn.QueryRow(`SELECT cursor_us FROM sync_state WHERE id=1`).Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	if err := db.conn.QueryRow(`SELECT follow_version FROM sync_state WHERE id=1`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if cursor != 101 || version != 1 {
		t.Fatalf("cursor=%d follow version=%d", cursor, version)
	}
	if err := db.ingest(ctx, "", checkpoint{TimeUS: 99}, false); err != nil {
		t.Fatal(err)
	}
	if err := db.conn.QueryRow(`SELECT cursor_us FROM sync_state WHERE id=1`).Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	if cursor != 101 {
		t.Fatal("replay moved checkpoint backwards")
	}
}

func TestMembershipMigrationDeletionAndFollowExclusion(t *testing.T) {
	ctx := context.Background()
	db := testStore(t)
	active, followed, failFollowPage, ambiguousWrite := true, false, false, false
	currentPDS := ""
	records := map[string]json.RawMessage{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/xrpc/blue.microcosm.identity.resolveMiniDoc":
			if !active {
				http.Error(w, `{"error":"NotFound"}`, 404)
				return
			}
			json.NewEncoder(w).Encode(map[string]string{"did": testDID, "pds": currentPDS})
		case "/xrpc/com.atproto.sync.getRepoStatus":
			json.NewEncoder(w).Encode(map[string]any{"did": testDID, "active": active})
		case "/xrpc/com.atproto.repo.getRecord":
			key := r.URL.Query().Get("rkey")
			value, ok := records[key]
			if !ok {
				http.Error(w, `{"error":"RecordNotFound"}`, 404)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"uri": "at://" + ownerDID + "/" + itemCollection + "/" + key, "cid": "bafyreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "value": value})
		case "/xrpc/com.atproto.repo.putRecord":
			var input struct {
				Rkey   string          `json:"rkey"`
				Record json.RawMessage `json:"record"`
				Swap   json.RawMessage `json:"swapRecord"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
			}
			if string(input.Swap) != "null" {
				t.Error("put must use create-only swapRecord")
			}
			records[input.Rkey] = input.Record
			if ambiguousWrite {
				http.Error(w, `{"error":"InternalServerError"}`, 500)
				return
			}
			fmt.Fprint(w, `{}`)
		case "/xrpc/com.atproto.repo.deleteRecord":
			var input struct {
				Rkey string `json:"rkey"`
			}
			json.NewDecoder(r.Body).Decode(&input)
			delete(records, input.Rkey)
			fmt.Fprint(w, `{}`)
		case "/xrpc/com.atproto.repo.listRecords":
			if failFollowPage {
				http.Error(w, `{"error":"Unavailable"}`, 503)
				return
			}
			if followed {
				fmt.Fprintf(w, `{"records":[{"value":{"subject":%q}}]}`, testDID)
			} else {
				fmt.Fprint(w, `{"records":[]}`)
			}
		default:
			t.Errorf("unexpected request %s", r.URL)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	currentPDS = server.URL
	api := &listTarget{session: newSession(Config{Slingshot: server.URL}, server.Client(), nil), pds: server.URL, listURI: "at://" + ownerDID + "/app.bsky.graph.list/2222222222222"}
	api.target = &xrpc.Client{Host: server.URL, Client: server.Client()}
	api.Client = &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: ownerDID}}
	other := &listTarget{session: newSession(Config{Slingshot: server.URL}, server.Client(), api.Client), pds: "https://second.example", target: api.target, listURI: "at://" + ownerDID + "/app.bsky.graph.list/3333333333333"}
	run := func() error {
		t.Helper()
		api.LastWrite = time.Time{}
		other.LastWrite = time.Time{}
		if err := db.queries.EnqueueEvent(ctx, testDID); err != nil {
			t.Fatal(err)
		}
		work, err := db.next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return reconcileTargets(ctx, []*listTarget{api, other}, work, db)
	}
	if err := api.syncFollows(ctx, db, true); err != nil {
		t.Fatal(err)
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatal("active account was not added")
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatal("replay duplicated membership")
	}
	followed = true
	if err := api.syncFollows(ctx, db, true); err != nil {
		t.Fatal(err)
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatal("followed account remained listed")
	}
	failFollowPage = true
	if err := api.syncFollows(ctx, db, true); err == nil {
		t.Fatal("failed follow inventory accepted")
	}
	var count int
	if err := db.conn.QueryRow(`SELECT count(*) FROM follows`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("failed snapshot discarded exclusions")
	}
	failFollowPage, followed = false, false
	if err := api.syncFollows(ctx, db, true); err != nil {
		t.Fatal(err)
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatal("unfollow did not restore membership")
	}
	currentPDS = "https://elsewhere.example"
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatal("migrated account remained listed")
	}
	currentPDS = server.URL
	ambiguousWrite = true
	if err := run(); err == nil {
		t.Fatal("ambiguous write should fail")
	}
	ambiguousWrite, active = false, false
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatal("deletion failed to remove ambiguous write")
	}
	if _, err := db.next(ctx); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("job not completed: %v", err)
	}
	active = true
	currentPDS = other.pds
	if err := run(); err != nil {
		t.Fatal(err)
	}
	otherKeys, err := db.MemberKeys(ctx, other.pds, testDID)
	if err != nil || len(otherKeys) != 1 {
		t.Fatalf("second list membership: %v %v", otherKeys, err)
	}
	currentPDS = api.pds
	ambiguousWrite = true
	if err := run(); err == nil {
		t.Fatal("failed write was accepted")
	}
	if _, err := db.next(ctx); err != nil {
		t.Fatal("partly completed migration lost its queued job", err)
	}
	otherKeys, err = db.MemberKeys(ctx, other.pds, testDID)
	if err != nil || len(otherKeys) != 0 {
		t.Fatalf("old list was not cleaned after another target failed: %v %v", otherKeys, err)
	}
	ambiguousWrite = false
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatal("migration retry duplicated membership")
	}
}

func TestBackfillPaginatesAndRechecksFormerMembers(t *testing.T) {
	db := testStore(t)
	if _, err := db.conn.Exec(`INSERT INTO list_members VALUES('', ?, 'old-key')`, ownerDID); err != nil {
		t.Fatal(err)
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		switch r.URL.Query().Get("cursor") {
		case "":
			fmt.Fprintf(w, `{"cursor":"page2","repos":[{"did":%q,"active":true}]}`, testDID)
		case "page2":
			fmt.Fprint(w, `{"repos":[]}`)
		default:
			t.Error("unexpected cursor")
			http.Error(w, "bad cursor", 400)
		}
	}))
	defer server.Close()
	api := &listTarget{session: newSession(Config{}, nil, nil), target: &xrpc.Client{Host: server.URL, Client: server.Client()}}
	if err := api.backfill(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.conn.QueryRow(`SELECT count(*) FROM pending`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if requests != 2 || count != 2 {
		t.Fatalf("requests=%d pending=%d", requests, count)
	}
}

func TestStreamQueuesIdentityAndOwnerFollowChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db := testStore(t)
	if err := db.bind(ctx, "owner", ownerDID); err != nil {
		t.Fatal(err)
	}
	if err := db.ingest(ctx, "", checkpoint{TimeUS: 10_000_000}, false); err != nil {
		t.Fatal(err)
	}
	upgrader := websocket.Upgrader{Subprotocols: []string{"xrpc.v1.json"}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("cursor") != "5000000" {
			t.Error("missing replay overlap")
		}
		if r.URL.Query().Get("collections") != "app.bsky.graph.follow" {
			t.Error("missing follow collection")
		}
		if r.URL.Query().Get("dids") != "" {
			t.Error("identity events must include all DIDs")
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		if conn.Subprotocol() != "xrpc.v1.json" {
			t.Error("missing JSON subscription protocol")
		}
		messages := []string{
			fmt.Sprintf(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#identity","did":%q,"seq":31,"time":"1970-01-01T00:00:10.000001Z"}}`, testDID),
			fmt.Sprintf(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","did":%q,"seq":32,"time":"1970-01-01T00:00:10.000002Z","collection":"app.bsky.graph.follow","operation":"delete"}}`, ownerDID),
			fmt.Sprintf(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","did":%q,"seq":33,"time":"1970-01-01T00:00:10.000003Z","collection":"app.bsky.graph.follow","operation":"create"}}`, testDID),
		}
		for _, message := range messages {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
				t.Error(err)
				return
			}
		}
	}))
	defer server.Close()
	if err := stream(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), db); err == nil {
		t.Fatal("expected disconnect")
	}
	work, err := db.next(ctx)
	if err != nil || work.DID != testDID {
		t.Fatalf("identity was not queued: %+v %v", work, err)
	}
	var version, cursor int64
	if err := db.conn.QueryRow(`SELECT follow_version FROM sync_state WHERE id=1`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.conn.QueryRow(`SELECT cursor_us FROM sync_state WHERE id=1`).Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	if version != 1 || cursor != 10000002 {
		t.Fatalf("follow version=%d cursor=%d", version, cursor)
	}
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http")
	if resumed, err := db.resumeCursor(ctx, endpoint); err != nil || resumed != 32 {
		t.Fatalf("same instance resume = %d, %v", resumed, err)
	}
	if resumed, err := db.resumeCursor(ctx, "wss://another.example"); err != nil || resumed != 5000002 {
		t.Fatalf("new instance resume = %d, %v", resumed, err)
	}
}

func TestStreamGapAndWitnessedTimestamp(t *testing.T) {
	ctx := context.Background()
	db := testStore(t)
	for _, payload := range []string{
		`{"$type":"error","error":"CursorTooOld"}`,
		`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#info","name":"OutdatedCursor"}}`,
		`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#info","name":"FutureCursor"}}`,
	} {
		if err := ingestFrame(ctx, db, "wss://test", ownerDID, []byte(payload)); !errors.Is(err, errStreamGap) {
			t.Fatalf("gap not detected: %v", err)
		}
	}
	payload := fmt.Sprintf(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#sync","did":%q,"seq":45,"time":"2026-01-01T00:00:00Z","witnessedAt":"2026-11-16T00:00:00Z"}}`, ownerDID)
	if err := ingestFrame(ctx, db, "wss://test", ownerDID, []byte(payload)); err != nil {
		t.Fatal(err)
	}
	resumed, err := db.resumeCursor(ctx, "wss://other")
	if err != nil || resumed != time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC).UnixMicro()-5_000_000 {
		t.Fatalf("wrong portable cursor %d: %v", resumed, err)
	}
	if err := db.resetStream(ctx); err != nil {
		t.Fatal(err)
	}
	resumed, err = db.resumeCursor(ctx, "wss://test")
	if err != nil || resumed < time.Now().Add(-2*time.Minute).UnixMicro() {
		t.Fatalf("cursor reset failed: %d %v", resumed, err)
	}
}

func mockItem(w http.ResponseWriter, key, did, list string) {
	json.NewEncoder(w).Encode(map[string]any{"uri": "at://" + ownerDID + "/" + itemCollection + "/" + key, "cid": "bafyreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "value": map[string]string{"$type": itemCollection, "subject": did, "list": list, "createdAt": "2026-01-01T00:00:00Z"}})
}
