package cordata

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/xrpc"
)

func TestListSurvivesRestartAndIncompleteLikesPage(t *testing.T) {
	const owner = "did:example:owner"
	const liker = "did:example:liker"
	const post = "at://did:example:author/app.bsky.feed.post/demo"
	const list = "at://did:example:owner/app.bsky.graph.list/demo"
	liked, incomplete := true, false
	var item json.RawMessage
	var key string
	puts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch strings.TrimPrefix(r.URL.Path, "/xrpc/") {
		case "app.bsky.feed.getLikes":
			switch {
			case incomplete && r.URL.Query().Get("cursor") == "":
				fmt.Fprint(w, `{"likes":[],"cursor":"next"}`)
			case incomplete:
				http.Error(w, `{"error":"Unavailable"}`, 503)
			case liked:
				fmt.Fprintf(w, `{"likes":[{"actor":{"did":%q}}]}`, liker)
			default:
				fmt.Fprint(w, `{"likes":[]}`)
			}
		case "com.atproto.repo.listRecords":
			if item == nil {
				fmt.Fprint(w, `{"records":[]}`)
				return
			}
			fmt.Fprintf(w, `{"records":[{"uri":%q,"value":%s}]}`, "at://"+owner+"/"+palustre.ItemCollection+"/"+key, item)
		case "com.atproto.repo.getRecord":
			if item == nil {
				http.Error(w, `{"error":"RecordNotFound"}`, 404)
				return
			}
			fmt.Fprintf(w, `{"cid":"current","value":%s}`, item)
		case "com.atproto.repo.putRecord", "com.atproto.repo.createRecord":
			var in struct {
				Rkey   string
				Record json.RawMessage
			}
			if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
				t.Error(err)
				return
			}
			item, key = in.Record, in.Rkey
			puts++
			fmt.Fprint(w, "{}")
		case "com.atproto.repo.deleteRecord":
			item = nil
			fmt.Fprint(w, "{}")
		default:
			t.Error(r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	scan := func() error {
		client := &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: owner}}
		api := &palustre.Session{Client: client, Refreshed: time.Now()}
		return syncPost(context.Background(), api, client, post, list)
	}
	for range 2 {
		if err := scan(); err != nil {
			t.Fatal(err)
		}
	}
	if puts != 1 || item == nil {
		t.Fatal("expected exactly one membership write")
	}
	incomplete = true
	if err := scan(); err == nil || item == nil {
		t.Fatal("incomplete scan must preserve membership")
	}
	incomplete, liked = false, false
	if err := scan(); err != nil {
		t.Fatal(err)
	}
	if item != nil {
		t.Fatal("unlike left a list item behind")
	}
}
