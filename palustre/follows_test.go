package palustre

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluesky-social/indigo/xrpc"
)

func TestTargetFollowsStopsOnlyWhenAllTargetsAreFound(t *testing.T) {
	const first = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const second = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
	for _, scenario := range []struct {
		name       string
		targets    map[string]bool
		full, fail bool
		pages      int
	}{
		{name: "one target", targets: map[string]bool{first: true}, pages: 1},
		{name: "multiple targets", targets: map[string]bool{first: true, second: true}, pages: 2},
		{name: "full exclusion snapshot", full: true, pages: 2},
		{name: "missing target requires all pages", targets: map[string]bool{second: true}, fail: true, pages: 2},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			pages := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				pages++
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Query().Get("cursor") == "" {
					fmt.Fprintf(w, `{"cursor":"next","records":[{"uri":"at://%s/app.bsky.graph.follow/first","value":{"$type":"app.bsky.graph.follow","subject":%q}}]}`, first, first)
					return
				}
				if scenario.fail {
					http.Error(w, `{"error":"Unavailable"}`, 503)
					return
				}
				fmt.Fprintf(w, `{"records":[{"uri":"at://%s/app.bsky.graph.follow/second","value":{"$type":"app.bsky.graph.follow","subject":%q}}]}`, first, second)
			}))
			defer server.Close()
			client := &xrpc.Client{Host: server.URL, Client: server.Client()}
			var follows map[string]bool
			var err error
			if scenario.full {
				follows, err = FetchFollows(context.Background(), client, first)
			} else {
				follows, err = FetchTargetFollows(context.Background(), client, first, scenario.targets)
			}
			if scenario.fail {
				if err == nil || follows != nil {
					t.Fatal("failed pagination returned usable partial results")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				for did := range scenario.targets {
					if !follows[did] {
						t.Fatalf("missing target %s", did)
					}
				}
				if scenario.full && (!follows[first] || !follows[second]) {
					t.Fatal("incomplete owner snapshot")
				}
			}
			if pages != scenario.pages {
				t.Fatalf("read %d pages, want %d", pages, scenario.pages)
			}
		})
	}
}
