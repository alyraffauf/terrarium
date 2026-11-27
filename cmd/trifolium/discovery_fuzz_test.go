package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"testing"

	"github.com/alyraffauf/terrarium/palustre"
)

// Both indexes can omit actors or return stale claims. Only the actor's PDS
// supplies the follows that may enter the local graph.
func FuzzIndexCandidatesAgainstRepos(f *testing.F) {
	f.Add([]byte{7, 3, 2, 5})
	f.Add([]byte{0, 0, 0})
	f.Add([]byte{6, 6, 4, 4})
	f.Fuzz(func(t *testing.T, evidence []byte) {
		a := openStore(t, ":memory:")
		configure(t, a)
		target := targets[0]
		followers, links := []string{}, []string{}
		repositories := map[string]bool{}
		want := map[string]string{}
		for i, bits := range evidence[:min(len(evidence), 12)] {
			did := fmt.Sprintf("did:example:actor%d", i)
			if bits&1 != 0 {
				followers = append(followers, did)
			}
			if bits&2 != 0 {
				links = append(links, did)
			}
			repositories[did] = bits&4 != 0
			if bits&3 != 0 && repositories[did] {
				want[did] = target
			}
		}
		a.http = &http.Client{Transport: remoteFunc(func(r *http.Request) (*http.Response, error) {
			body := map[string]any{}
			q := r.URL.Query()
			switch r.URL.Path {
			case "/xrpc/app.bsky.graph.getFollowers", "/xrpc/blue.microcosm.links.getBacklinks":
				isApp := r.URL.Path == "/xrpc/app.bsky.graph.getFollowers"
				ids := links
				if isApp {
					if q.Get("actor") != target {
						return nil, fmt.Errorf("unexpected actor")
					}
					ids = followers
					body["subject"] = map[string]string{"did": target}
				} else if q.Get("subject") != target || q.Get("source") != followCollection+":subject" {
					return nil, fmt.Errorf("unexpected backlinks query")
				}
				mid := len(ids) / 2
				if q.Get("cursor") == "" {
					body["cursor"] = "tail"
					ids = ids[:mid]
				} else if q.Get("cursor") == "tail" {
					ids = ids[mid:]
				} else {
					return nil, fmt.Errorf("unexpected cursor")
				}
				rows := []any{}
				for _, id := range ids {
					rows = append(rows, map[string]string{"did": id, "collection": followCollection, "rkey": "follow"})
				}
				if isApp {
					body["followers"] = rows
				} else {
					body["records"] = rows
				}
			case "/xrpc/blue.microcosm.identity.resolveMiniDoc":
				body = map[string]any{"did": q.Get("identifier"), "pds": "https://pds.invalid"}
			case "/xrpc/com.atproto.repo.listRecords", "/xrpc/com.atproto.repo.getRecord":
				did := q.Get("repo")
				row := map[string]any{"uri": "at://" + did + "/" + followCollection + "/follow", "value": map[string]string{"$type": followCollection, "subject": target}}
				if r.URL.Path == "/xrpc/com.atproto.repo.getRecord" {
					if !repositories[did] {
						return reply(404, `{"error":"RecordNotFound"}`), nil
					}
					body = row
				} else {
					rows := []any{}
					if repositories[did] {
						rows = append(rows, row)
					}
					body["records"] = rows
				}
			default:
				return nil, fmt.Errorf("unexpected request %s", r.URL)
			}
			data, err := json.Marshal(body)
			return reply(200, string(data)), err
		})}
		a.api = &palustre.Session{HTTP: a.http, Config: palustre.Config{Slingshot: "https://resolver.invalid"}}
		for _, backlinks := range []bool{false, true} {
			requireOK(t, a.discover(t.Context(), target, backlinks))
		}
		if got := pairs(t, a, "SELECT actor,target FROM edges"); len(got) != 0 {
			t.Fatalf("unverified index entries %v", got)
		}
		jobs := len(followers) + len(links)
		for i := 0; i < jobs; i++ {
			requireOK(t, a.verify(t.Context(), i%2 == 0))
		}
		if err := a.verify(t.Context(), true); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("queue not drained: %v", err)
		}
		if got := pairs(t, a, "SELECT actor,target FROM edges"); !maps.Equal(got, want) {
			t.Fatalf("got actors %v want %v", slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(want)))
		}
	})
}
