package cordata

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

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

// The hook runs inside the first page request, after the walk has selected its readers.
type inventoryTransport struct {
	*testRepository
	onWalk func()
	walks  int
}

func (transport *inventoryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Path == "/xrpc/com.atproto.repo.listRecords" && req.URL.Query().Get("cursor") == "" {
		transport.walks++
		if transport.onWalk != nil {
			transport.onWalk()
		}
	}
	return transport.testRepository.RoundTrip(req)
}

func FuzzSharedOwnerInventory(f *testing.F) {
	f.Add([]byte{0, 1, 8 + 64, 10, 9, 0, 8})
	f.Add([]byte{0, 1, 8 + 32 + 64, 9, 0, 8, 10})
	f.Add([]byte{2, 0, 6, 1, 8, 9 + 128, 1, 9})
	f.Fuzz(func(t *testing.T, history []byte) {
		const actor = "did:example:liker"
		lists := []string{list, otherList, "at://" + owner + "/app.bsky.graph.list/third"}
		var coordinator ownerInventories
		type worker struct {
			request                  *inventoryRequest
			captured, failed, member bool
		}
		var workers [3]worker
		var members [3]bool
		repo := &testRepository{items: map[string]remoteItem{}}
		// Unmanaged records force a second page, even with empty managed lists.
		for i := 0; i < 3; i++ {
			repo.items[fmt.Sprintf("external-%d", i)] = remoteItem{Type: palustre.ItemCollection, List: "at://" + owner + "/app.bsky.graph.list/external", Subject: actor}
		}
		transport := &inventoryTransport{testRepository: repo}
		client := &xrpc.Client{Host: "https://repo.test", Client: &http.Client{Transport: transport}, Auth: &xrpc.AuthInfo{Did: owner}}
		api := &palustre.Session{Client: client, HTTP: client.Client, Config: palustre.Config{WriteInterval: time.Nanosecond}}
		register := func(i int) {
			if workers[i].request == nil {
				workers[i] = worker{request: coordinator.request(lists[i])}
			}
		}
		wantWalks := 0
		for step, b := range history[:min(len(history), 48)] {
			i := int(b&3) % len(lists)
			w := &workers[i]
			switch (b >> 2) & 3 {
			case 0:
				register(i)
			case 1:
				if w.request != nil {
					w.request.close()
					*w = worker{}
				}
			default:
				if w.request == nil {
					continue
				}
				repo.fault = 0
				if b&32 != 0 {
					repo.fault = 6
				}
				transport.onWalk = nil
				if !w.captured {
					wantWalks++
					for j := range workers {
						v := &workers[j]
						if v.request != nil && !v.captured {
							v.captured, v.failed, v.member = true, repo.fault != 0, members[j]
						}
					}
					if b&64 != 0 {
						transport.onWalk = func() { register((i + 2) % len(lists)) }
					}
				}
				items, err := w.request.read(t.Context(), client)
				if (err != nil) != w.failed || transport.walks != wantWalks {
					t.Fatalf("step %d: error=%v walks=%d, want failed=%t walks=%d", step, err, transport.walks, w.failed, wantWalks)
				}
				if w.failed {
					if items != nil {
						t.Fatalf("step %d published incomplete inventory: %v", step, items)
					}
				} else {
					keys := items[actor]
					if len(items) != btoi(w.member) || len(keys) != btoi(w.member) || (w.member && repo.items[keys[0]].List != lists[i]) {
						t.Fatalf("step %d list %s partition=%v want member=%t", step, lists[i], items, w.member)
					}
					include := b&128 == 0
					err := api.Reconcile(t.Context(), items, palustre.Membership{ListURI: lists[i], DID: actor, Include: include})
					if err != nil {
						t.Fatal(err)
					}
					members[i] = include
				}
				w.request.close()
				*w = worker{}
				counts := map[string]int{}
				for _, item := range repo.items {
					counts[item.List]++
				}
				for j, listURI := range lists {
					if counts[listURI] != btoi(members[j]) {
						t.Fatalf("step %d list %s has %d members, want present=%t", step, listURI, counts[listURI], members[j])
					}
				}
				if len(repo.items) != 3+counts[lists[0]]+counts[lists[1]]+counts[lists[2]] {
					t.Fatalf("step %d changed unmanaged membership", step)
				}
			}
		}
	})
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
