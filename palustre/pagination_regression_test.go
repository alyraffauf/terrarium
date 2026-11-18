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
			fmt.Fprint(w, `{"cursor":"a","records":[{"value":{"subject":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"}}]}`)
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
