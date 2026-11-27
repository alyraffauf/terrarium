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
)

func TestDeletedAccountEventRemovesAllListsAfterRestartAndRetry(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	db, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	// Multiple lists can temporarily contain the account during migration.
	for _, pds := range []string{"https://first.example", "https://second.example"} {
		if err := db.RememberMember(ctx, pds, testDID, recordKey(pds)); err != nil {
			t.Fatal(err)
		}
	}
	payload := fmt.Sprintf(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#account","did":%q,"seq":100,"time":"2026-11-16T00:00:00Z","account":{"did":%q,"active":false,"status":"deleted","seq":900,"time":"2026-11-16T00:00:00Z"}}}`, testDID, testDID)
	if err := ingestFrame(ctx, db, "wss://jetstream.example", ownerDID, []byte(payload), true); err != nil {
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
	if err := db.applyFollowSnapshot(ctx, 0, nil); err != nil {
		t.Fatal(err)
	}
	work, err := db.next(ctx)
	if err != nil || work.DID != testDID {
		t.Fatalf("deletion event lost across restart: %+v %v", work, err)
	}
	failed := false
	removed := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/xrpc/blue.microcosm.identity.resolveMiniDoc":
			http.Error(w, `{"error":"NotFound"}`, 404)
		case "/xrpc/com.atproto.sync.getRepoStatus":
			fmt.Fprintf(w, `{"did":%q,"active":false,"status":"deleted"}`, testDID)
		case "/xrpc/com.atproto.repo.getRecord":
			mockItem(w, r.URL.Query().Get("rkey"), testDID, "at://"+ownerDID+"/"+listCollection+"/"+r.URL.Query().Get("rkey"))
		case "/xrpc/com.atproto.repo.deleteRecord":
			if !failed {
				failed = true
				http.Error(w, `{"error":"Unavailable"}`, 503)
				return
			}
			// The second target must still progress when the first target fails.
			var input struct {
				Rkey string `json:"rkey"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
			}
			removed[input.Rkey] = true
			fmt.Fprint(w, `{}`)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.Error(w, `{"error":"UnexpectedRequest"}`, 500)
		}
	}))
	defer server.Close()
	var targets []*listTarget
	for _, pds := range []string{"https://first.example", "https://second.example"} {
		targets = append(targets, &listTarget{session: newSession(Config{Slingshot: server.URL}, server.Client(), &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: ownerDID}}), pds: pds, target: &xrpc.Client{Host: server.URL, Client: server.Client()}, listURI: "at://" + ownerDID + "/" + listCollection + "/" + recordKey(pds)})
	}
	if err := reconcileTargets(ctx, targets, work, db); err == nil {
		t.Fatal("accepted failed removal")
	}
	var remaining int
	if err := db.conn.QueryRow(`SELECT count(*) FROM list_members WHERE did=?`, testDID).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 || len(removed) != 1 {
		t.Fatalf("remaining memberships after partial failure: %d", remaining)
	}
	work, err = db.next(ctx)
	if err != nil {
		t.Fatal("deletion retry was lost", err)
	}
	for _, target := range targets {
		target.LastWrite = time.Time{}
	}
	if err := reconcileTargets(ctx, targets, work, db); err != nil {
		t.Fatal(err)
	}
	if err := db.conn.QueryRow(`SELECT count(*) FROM list_members WHERE did=?`, testDID).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatal("deleted account remained listed")
	}
	if !removed[recordKey("https://first.example")] || !removed[recordKey("https://second.example")] {
		t.Fatal("wrong remote records removed")
	}
	if _, err := db.next(ctx); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("deletion job not completed: %v", err)
	}
}

func TestFormerHostDeletionKeepsActiveMigratedAccount(t *testing.T) {
	ctx := context.Background()
	db := testStore(t)
	const pds = "https://current.example"
	writes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/xrpc/blue.microcosm.identity.resolveMiniDoc":
			fmt.Fprintf(w, `{"did":%q,"pds":%q}`, testDID, pds)
		case "/xrpc/com.atproto.sync.getRepoStatus":
			fmt.Fprintf(w, `{"did":%q,"active":true}`, testDID)
		case "/xrpc/com.atproto.repo.getRecord":
			mockItem(w, "current-item", testDID, "at://"+ownerDID+"/"+listCollection+"/current-list")
		case "/xrpc/com.atproto.repo.createRecord":
			writes++
			fmt.Fprint(w, `{}`)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.Error(w, `{"error":"UnexpectedRequest"}`, 500)
		}
	}))
	defer server.Close()
	if err := db.RememberMember(ctx, pds, testDID, "current-item"); err != nil {
		t.Fatal(err)
	}
	payload := fmt.Sprintf(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#account","did":%q,"seq":101,"time":"2026-11-16T00:00:00Z","account":{"did":%q,"active":false,"status":"deleted","seq":901,"time":"2026-11-16T00:00:00Z"}}}`, testDID, testDID)
	if err := ingestFrame(ctx, db, "wss://jetstream.example", ownerDID, []byte(payload), true); err != nil {
		t.Fatal(err)
	}
	work, err := db.next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	api := &listTarget{session: newSession(Config{Slingshot: server.URL}, server.Client(), &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: ownerDID}}), pds: pds, target: &xrpc.Client{Host: server.URL, Client: server.Client()}, listURI: "at://" + ownerDID + "/" + listCollection + "/current-list"}
	if err := reconcileTargets(ctx, []*listTarget{api}, work, db); err != nil {
		t.Fatal(err)
	}
	keys, err := db.MemberKeys(ctx, pds, testDID)
	if err != nil || len(keys) != 1 || keys[0] != "current-item" || writes != 0 {
		t.Fatalf("active migrated account lost membership: %v %v", keys, err)
	}
}
