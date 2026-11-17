package helix

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/bluesky-social/indigo/xrpc"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBatchAppliesReadyResultBeforeSlowPDSAndRechecksExclusions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := testStore(t)
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	added := make(chan string, 2)
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/xrpc/blue.microcosm.identity.resolveMiniDoc":
			did := r.URL.Query().Get("identifier")
			if did == followerDID {
				close(slowStarted)
				select {
				case <-releaseSlow:
				case <-r.Context().Done():
					return
				}
			} else {
				select {
				case <-slowStarted:
				case <-r.Context().Done():
					return
				}
			}
			json.NewEncoder(w).Encode(map[string]string{"did": did, "pds": server.URL})
		case "/xrpc/com.atproto.sync.getRepoStatus":
			fmt.Fprint(w, `{"active":true}`)
		case "/xrpc/com.atproto.repo.listRecords":
			subject := firstDID
			if r.URL.Query().Get("repo") == ownerDID {
				subject = followerDID
			}
			fmt.Fprintf(w, `{"records":[{"value":{"subject":%q}}]}`, subject)
		case "/xrpc/com.atproto.repo.getRecord":
			http.Error(w, `{"error":"RecordNotFound"}`, 404)
		case "/xrpc/com.atproto.repo.putRecord":
			var input struct{ Record struct{ Subject string } }
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
			}
			added <- input.Record.Subject
			fmt.Fprint(w, `{}`)
		default:
			t.Error("unexpected endpoint", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	api := newSession(Config{Slingshot: server.URL}, server.Client(), &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: ownerDID}})
	targets := []*listTarget{{session: api, actor: firstDID, listURI: "at://" + ownerDID + "/" + listCollection + "/first"}}
	for _, did := range []string{followerDID, secondDID} {
		if err := db.queueScan(ctx, did); err != nil {
			t.Fatal(err)
		}
	}
	// The initial owner snapshot excludes nobody. An event invalidates it during reads.
	if err := db.applyFollowSnapshot(ctx, 0, map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- processBatch(ctx, targets, []job{{DID: followerDID, Generation: 1}, {DID: secondDID, Generation: 1}}, db)
	}()
	select {
	case did := <-added:
		if did != secondDID {
			t.Fatalf("slow job completed first: %s", did)
		}
	case <-ctx.Done():
		t.Fatal("slow PDS blocked the ready result")
	}
	if err := db.ingest(ctx, "", checkpoint{}, true); err != nil {
		t.Fatal(err)
	}
	close(releaseSlow)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case did := <-added:
		t.Fatalf("added newly followed account %s", did)
	default:
	}
	if api.ChecksCompleted.Load() != 2 || api.MembersAdded.Load() != 1 {
		t.Fatal("incorrect progress counters")
	}
}
