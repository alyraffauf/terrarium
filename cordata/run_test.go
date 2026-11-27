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
	malformed := ""
	liked, incomplete := true, false
	var item json.RawMessage
	var key string
	puts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch strings.TrimPrefix(r.URL.Path, "/xrpc/") {
		case "app.bsky.feed.getLikes":
			switch {
			case malformed != "":
				fmt.Fprint(w, malformed)
			case incomplete && r.URL.Query().Get("cursor") == "":
				fmt.Fprint(w, `{"uri":"at://did:example:author/app.bsky.feed.post/demo","likes":[],"cursor":"next"}`)
			case incomplete:
				http.Error(w, `{"error":"Unavailable"}`, 503)
			case liked:
				fmt.Fprintf(w, `{"uri":"at://did:example:author/app.bsky.feed.post/demo","likes":[{"actor":{"did":%q}}]}`, liker)
			default:
				fmt.Fprint(w, `{"uri":"at://did:example:author/app.bsky.feed.post/demo","likes":[]}`)
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
		case "com.atproto.repo.createRecord", "com.atproto.repo.putRecord":
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
	for _, malformed = range []string{`{}`, `{"likes":null}`, `{"likes":[],"uri":"at://did:example:other/app.bsky.feed.post/demo"}`} {
		if err := scan(); err == nil || item == nil {
			t.Fatal("malformed scan removed existing membership")
		}
	}
	malformed = ""
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

func TestPostTarget(t *testing.T) {
	const post = "at://author.example/app.bsky.feed.post/demo"
	for _, input := range []string{post, "https://bsky.app/profile/author.example/post/demo"} {
		got, err := parsePost(input)
		if err != nil || got.String() != post {
			t.Fatalf("parse %q: %q, %v", input, got, err)
		}
	}
	for _, input := range []string{"http://bsky.app/profile/a.example/post/key", "https://example.org/profile/a.example/post/key", "https://bsky.app/profile/a.example", "at://author.example/app.bsky.graph.list/key", "at://author.example/app.bsky.feed.post"} {
		if _, err := parsePost(input); err == nil {
			t.Errorf("accepted %q", input)
		}
	}
}

func TestRepeatedLikesCursor(t *testing.T) {
	if err := pages(func(string) (*string, error) { cursor := "same"; return &cursor, nil }); err == nil {
		t.Fatal("accepted repeated cursor")
	}
}
