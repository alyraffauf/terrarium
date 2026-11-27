package palustre

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/xrpc"
)

// A buggy or hostile PDS that cycles cursors a->b->a must produce an error,
// not an endless pagination loop. Candidate PDS hosts are not trusted.
func TestFetchFollowsRejectsCursorCycle(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("cursor") {
		case "":
			fmt.Fprint(w, `{"cursor":"a","records":[{"uri":"at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.follow/first","value":{"$type":"app.bsky.graph.follow","subject":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"}}]}`)
		case "a":
			fmt.Fprint(w, `{"cursor":"b","records":[]}`)
		default:
			fmt.Fprint(w, `{"cursor":"a","records":[]}`)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := &xrpc.Client{Host: server.URL, Client: server.Client()}
	if _, err := FetchFollows(ctx, client, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"); err == nil {
		t.Fatal("cycling pagination cursor was accepted")
	}
	if requests > 10 {
		t.Fatalf("pagination made %d requests before failing", requests)
	}
}

func TestFetchFollowsRequiresRecordsOnFinalPage(t *testing.T) {
	for _, test := range []struct {
		name, body string
		valid      bool
	}{
		{"explicit empty array", `{"records":[]}`, true},
		{"missing records", `{}`, false},
		{"null records", `{"records":null}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			did := "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Query().Get("cursor") == "" {
					fmt.Fprintf(w, `{"cursor":"next","records":[{"uri":"at://%s/app.bsky.graph.follow/first","value":{"$type":"app.bsky.graph.follow","subject":%q}}]}`, did, did)
					return
				}
				fmt.Fprint(w, test.body)
			}))
			defer server.Close()
			client := &xrpc.Client{Host: server.URL, Client: server.Client()}
			follows, err := FetchFollows(t.Context(), client, did)
			if test.valid {
				if err != nil || !follows[did] {
					t.Fatalf("expected full snapshot containing %s, got %v, %v", did, follows, err)
				}
			} else if err == nil || follows != nil {
				t.Fatalf("expected error and no snapshot, got %v, %v", follows, err)
			}
		})
	}
}

func TestFollowSnapshotRejectsInvalidRecordURI(t *testing.T) {
	const owner = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const followed = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
	for _, uri := range []string{"", "at://" + followed + "/app.bsky.graph.follow/key", "at://" + owner + "/app.bsky.graph.block/key"} {
		t.Run(uri, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"records":[
					{"uri":"at://%s/app.bsky.graph.follow/first","value":{"$type":"app.bsky.graph.follow","subject":%q}},
					{"uri":%q,"value":{"$type":"app.bsky.graph.follow","subject":%q}}
				]}`, owner, followed, uri, followed)
			}))
			defer server.Close()
			client := &xrpc.Client{Host: server.URL, Client: server.Client()}
			follows, err := FetchTargetFollows(t.Context(), client, owner, map[string]bool{followed: true})
			if err == nil || follows != nil {
				t.Fatalf("expected invalid snapshot, got %v, %v", follows, err)
			}
		})
	}
}

func TestFollowSnapshotSkipsWrongRecordTypes(t *testing.T) {
	const owner = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const followed = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
	const unrelated = "did:plc:cccccccccccccccccccccccc"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"records":[
			{"uri":"at://%s/app.bsky.graph.follow/first","value":{"$type":"app.bsky.graph.follow","subject":%q}},
			{"uri":"at://%s/app.bsky.graph.follow/wrongtype","value":{"$type":"app.bsky.graph.block","subject":%q}},
			{"uri":"at://%s/app.bsky.graph.follow/missingtype","value":{"subject":%q}}
		]}`, owner, followed, owner, unrelated, owner, unrelated)
	}))
	defer server.Close()
	client := &xrpc.Client{Host: server.URL, Client: server.Client()}
	follows, err := FetchFollows(t.Context(), client, owner)
	if err != nil || len(follows) != 1 || !follows[followed] {
		t.Fatalf("expected one follow for %s, got %v, %v", followed, follows, err)
	}
}
