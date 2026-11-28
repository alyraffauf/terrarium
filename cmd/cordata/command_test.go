package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alyraffauf/terrarium/internal/clitest"
)

func TestPostLinkAndURIProduceSamePublishedList(t *testing.T) {
	const author = "did:example:author"
	const owner = "did:example:owner"
	const liker = "did:example:liker"
	const post = "at://" + author + "/app.bsky.feed.post/demo"
	var firstList string
	for _, input := range []string{"https://bsky.app/profile/author.example/post/demo", post} {
		t.Run(input, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			created := make(chan string, 1)
			failedLookup := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/xrpc/blue.microcosm.identity.resolveMiniDoc":
					if r.URL.Query().Get("identifier") == "missing.example" {
						close(failedLookup)
						http.Error(w, `{"error":"NotFound"}`, 404)
						return
					}
					fmt.Fprintf(w, `{"did":%q,"handle":"author.example","pds":"https://author.example"}`, author)
				case "/xrpc/com.atproto.server.createSession":
					fmt.Fprintf(w, `{"did":%q,"handle":"owner.example","accessJwt":"access","refreshJwt":"refresh"}`, owner)
				case "/xrpc/com.atproto.repo.getRecord":
					http.Error(w, `{"error":"RecordNotFound"}`, 404)
				case "/xrpc/blue.microcosm.links.getBacklinks":
					fmt.Fprint(w, `{"records":[]}`)
				case "/xrpc/app.bsky.feed.getRepostedBy":
					fmt.Fprintf(w, `{"uri":%q,"repostedBy":[]}`, post)
				case "/xrpc/app.bsky.feed.getLikes":
					if r.URL.Query().Get("uri") != post {
						t.Error("queried wrong post")
					}
					fmt.Fprintf(w, `{"uri":%q,"likes":[{"actor":{"did":%q}}]}`, post, liker)
				case "/xrpc/com.atproto.repo.listRecords":
					fmt.Fprint(w, `{"records":[]}`)
				case "/xrpc/com.atproto.repo.createRecord":
					var in struct {
						Repo, Rkey, Collection string
						Record                 struct{ Name, Description, List, Subject string }
					}
					if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
						t.Error(err)
						cancel()
						return
					}
					if in.Repo != owner {
						t.Error("wrong list owner")
					}
					switch in.Collection {
					case "app.bsky.graph.list":
						if in.Record.Description != post || len(strings.Fields(in.Record.Name)) != 2 {
							t.Errorf("unexpected list metadata: %+v", in.Record)
						}
					case "app.bsky.graph.listitem":
						if in.Record.Subject != liker {
							t.Error("wrong liker")
						}
						select {
						case <-failedLookup:
						case <-ctx.Done():
							t.Error("unavailable post was never attempted")
						}
						created <- in.Record.List
						cancel()
					default:
						t.Errorf("unexpected collection %s", in.Collection)
						cancel()
					}
					fmt.Fprint(w, "{}")
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
					cancel()
				}
			}))
			defer server.Close()
			t.Setenv("CORDATA_IDENTIFIER", "owner.example")
			t.Setenv("CORDATA_APP_PASSWORD", "test-password")
			cmd := command()
			cmd.SetArgs([]string{"--constellation=" + server.URL, "--appview=" + server.URL, "--login-pds=" + server.URL, "--slingshot=" + server.URL, "--write-interval=1ns", "at://missing.example/app.bsky.feed.post/post", input, input})
			if err := cmd.ExecuteContext(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case uri := <-created:
				if !strings.HasPrefix(uri, "at://"+owner+"/app.bsky.graph.list/") {
					t.Fatal(uri)
				}
				if firstList != "" && firstList != uri {
					t.Fatalf("same post produced different lists: %s and %s", firstList, uri)
				}
				firstList = uri
			default:
				t.Fatal("daemon exited without publishing its member")
			}
		})
	}
}

func TestIntervalSourcePrecedence(t *testing.T) {
	clitest.CheckIntervalPrecedence(t, command, "CORDATA", "at://did:example:author/app.bsky.feed.post/example")
}
