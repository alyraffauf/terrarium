package helix

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/xrpc"
)

func TestStableTitleAndDescriptionFollowHandleAcrossRestarts(t *testing.T) {
	db := testStore(t)
	var mutex sync.Mutex
	handle := "old.example.com"
	purpose, description := "app.bsky.graph.defs#modlist", "Preserve this"
	list := &bsky.GraphList{LexiconTypeID: listCollection, Name: listName(firstDID), Purpose: &purpose, Description: &description, CreatedAt: "2026-01-01T00:00:00Z"}
	writes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mutex.Lock()
		defer mutex.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/xrpc/app.bsky.actor.getProfile":
			if r.URL.Query().Get("actor") != firstDID {
				t.Error("profile lookup did not use target DID")
			}
			json.NewEncoder(w).Encode(map[string]string{"did": firstDID, "handle": handle})
		case "/xrpc/com.atproto.repo.getRecord":
			json.NewEncoder(w).Encode(map[string]any{"cid": "record-cid", "value": list})
		case "/xrpc/com.atproto.repo.putRecord":
			var input atproto.RepoPutRecord_Input
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
				return
			}
			if input.Rkey != "stable-key" || input.SwapRecord == nil || *input.SwapRecord != "record-cid" {
				t.Error("rename lost list identity or concurrency check")
			}
			list = input.Record.Val.(*bsky.GraphList)
			writes++
			w.Write([]byte(`{}`))
		default:
			t.Error("unexpected request", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	uri := "at://" + ownerDID + "/app.bsky.graph.list/stable-key"
	for _, next := range []string{"old.example.com", "new.example.com", "new.example.com"} {
		mutex.Lock()
		handle = next
		mutex.Unlock()
		api := &listTarget{session: newSession(Config{AppView: server.URL}, server.Client(), &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: ownerDID}}), actor: firstDID, listURI: uri, db: db}
		if err := api.ensureList(context.Background()); err != nil {
			t.Fatal(err)
		}
		if api.name == "" || list.Name != api.name || list.Description == nil || *list.Description != description+"\nFollowers of "+next {
			t.Fatalf("incorrect list name %q", api.name)
		}
	}
	mutex.Lock()
	defer mutex.Unlock()
	if writes != 2 {
		t.Fatalf("got %d renames, want 2", writes)
	}
	if list.Description == nil || list.CreatedAt != "2026-01-01T00:00:00Z" {
		t.Fatal("rename changed unrelated metadata")
	}
}

func TestSuspendedTargetSkipsListAndMembershipWrites(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/app.bsky.actor.getProfile" {
			t.Errorf("unexpected request %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"AccountTakedown","message":"Account has been suspended"}`))
	}))
	defer server.Close()
	target := &listTarget{actor: firstDID, session: newSession(Config{AppView: server.URL}, server.Client(), nil)}
	ctx := context.Background()
	if err := target.ensureList(ctx); err != nil {
		t.Fatal(err)
	}
	if !target.suspended.Load() {
		t.Fatal("suspended profile still enabled")
	}
	if err := target.reconcileSubject(ctx, subjectState{did: ownerDID, follows: map[string]bool{firstDID: true}}, nil); err != nil {
		t.Fatal(err)
	}
}
