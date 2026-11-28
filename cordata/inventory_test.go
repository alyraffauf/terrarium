package cordata

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/xrpc"
)

func inventoryClient(t *testing.T, handler http.HandlerFunc) *xrpc.Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: owner}}
}

func inventoryRow(listURI, did, key string) map[string]any {
	return map[string]any{
		"uri":   fmt.Sprintf("at://%s/%s/%s", owner, palustre.ItemCollection, key),
		"value": remoteItem{Type: palustre.ItemCollection, List: listURI, Subject: did},
	}
}

func TestSharedInventoryKeepsWalksSeparateAcrossPostPasses(t *testing.T) {
	const actor = "did:example:liker"
	var snapshots ownerInventories
	first := snapshots.request(list)
	slow := snapshots.request(otherList)
	var walks atomic.Int64
	started := make(chan struct{})
	resume := make(chan struct{})
	client := inventoryClient(t, func(w http.ResponseWriter, r *http.Request) {
		index := walks.Add(1)
		if index == 1 {
			close(started)
			select {
			case <-resume:
			case <-r.Context().Done():
				return
			}
		}
		rows := []map[string]any{
			inventoryRow(list, actor, fmt.Sprintf("mine-%d", index)),
			inventoryRow(otherList, actor, fmt.Sprintf("theirs-%d", index)),
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"records": rows})
	})

	// Both posts were due before the walk; a new post requesting records
	// during the walk must wait for a fresh view.
	read := func(req *inventoryRequest, key string) {
		t.Helper()
		defer req.close()
		got, err := req.read(t.Context(), client)
		want := inventory{}
		if key != "" {
			want[actor] = []string{key}
		}
		if err != nil || len(got) != len(want) || !reflect.DeepEqual(got[actor], want[actor]) {
			t.Errorf("read %s: got %v, %v; want %v", req.list, got, err, want)
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		read(first, "mine-1")
	}()
	<-started
	late := snapshots.request("at://" + owner + "/app.bsky.graph.list/late")
	close(resume)
	<-done
	read(late, "")
	// A worker that finished writing starts a new pass after the second
	// walk, even when another post is still reading its first snapshot.
	read(snapshots.request(list), "mine-3")
	read(slow, "theirs-1")
	if got := walks.Load(); got != 3 {
		t.Fatalf("made %d owner inventories, want 3", got)
	}
}

func TestFailedOwnerInventoryDiscardsEveryPartition(t *testing.T) {
	var snapshots ownerInventories
	requests := []*inventoryRequest{snapshots.request(list), snapshots.request(otherList)}
	var pages atomic.Int64
	client := inventoryClient(t, func(w http.ResponseWriter, r *http.Request) {
		pages.Add(1)
		if r.URL.Query().Get("cursor") != "" {
			http.Error(w, `{"error":"Unavailable"}`, 503)
			return
		}
		rows := []map[string]any{inventoryRow(list, "did:example:liker", "key")}
		_ = json.NewEncoder(w).Encode(map[string]any{"cursor": "next", "records": rows})
	})
	for _, req := range requests {
		if got, err := req.read(t.Context(), client); err == nil || got != nil {
			t.Fatalf("published partial owner inventory: %v, %v", got, err)
		}
		req.close()
	}
	if got := pages.Load(); got != 2 {
		t.Fatalf("failed pass made %d page requests, want 2", got)
	}
}
