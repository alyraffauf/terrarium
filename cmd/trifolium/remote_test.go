package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/xrpc"
)

type remoteFunc func(*http.Request) (*http.Response, error)

func (f remoteFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func reply(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}
func connect(a *service, remote remoteFunc) {
	a.api = &palustre.Session{Refreshed: time.Now(), Client: &xrpc.Client{Host: "https://repo.invalid", Auth: &xrpc.AuthInfo{Did: owner}, Client: &http.Client{Transport: remote}}, Config: palustre.Config{WriteInterval: time.Nanosecond}}
}

func FuzzAuditSnapshots(f *testing.F) {
	f.Add([]byte{1, 3, 130, 2, 0, 4})
	f.Fuzz(func(t *testing.T, h []byte) {
		a := openStore(t, ":memory:")
		lists := configure(t, a)
		want := map[string]string{}
		for step, b := range h[:min(len(h), 16)] {
			next := map[string]string{}
			var records []string
			for i := 0; i < 5; i++ {
				if b&(1<<i) == 0 {
					continue
				}
				k := fmt.Sprint(i)
				did := fmt.Sprintf("did:example:m%d", (i+step)%3)
				list := lists[i%2].list
				records = append(records, fmt.Sprintf(`{"uri":"at://%s/app.bsky.graph.listitem/%s","value":{"$type":"app.bsky.graph.listitem","subject":%q,"list":%q}}`, owner, k, did, list))
				next[lists[i%2].id+"/"+k] = did
			}
			connect(a, func(r *http.Request) (*http.Response, error) {
				if r.URL.Query().Get("cursor") == "" {
					return reply(200, `{"cursor":"two","records":[`+strings.Join(records, ",")+`]}`), nil
				}
				if b&128 != 0 {
					return reply(200, `{"records":null}`), nil
				}
				return reply(200, `{"records":[]}`), nil
			})
			err := a.auditMembers(t.Context(), lists)
			if b&128 != 0 {
				if err == nil {
					t.Fatal("accepted incomplete inventory")
				}
			} else {
				requireOK(t, err)
				want = next
			}
			if got := pairs(t, a, "SELECT cluster||'/'||rkey,did FROM items"); !maps.Equal(got, want) {
				t.Fatalf("step %d: got %v want %v", step, got, want)
			}
			for _, c := range lists {
				var did string
				var inc bool
				if err := a.QueryRow(nextChange, c.id, c.threshold, owner).Scan(&did, &inc); !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("unverified removal %s %v", did, err)
				}
			}
		}
	})
}

func FuzzPublishRetry(f *testing.F) {
	f.Add(true, uint8(2))
	f.Add(false, uint8(0))
	f.Fuzz(func(t *testing.T, fail bool, name uint8) {
		a := openStore(t, ":memory:")
		c := configure(t, a)[0]
		did := fmt.Sprintf("did:example:m%d", name)
		execute(t, a, "INSERT INTO edges VALUES(?,'a',?),(?,'b',?)", did, targets[0], did, targets[1])
		description := fmt.Sprintf("Accounts clustered around @m%d.example.", name)
		execute(t, a, "INSERT INTO descriptions VALUES(?,?)", c.id, description)
		a.http = &http.Client{Transport: remoteFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("profiles offline") })}
		writes := 0
		connect(a, func(r *http.Request) (*http.Response, error) {
			switch r.URL.Path {
			case "/xrpc/com.atproto.repo.getRecord":
				if r.URL.Query().Get("collection") == palustre.ListCollection {
					return reply(200, fmt.Sprintf(`{"uri":%q,"value":{"$type":"app.bsky.graph.list","purpose":"app.bsky.graph.defs#modlist","name":%q,"description":%q}}`, c.list, palustre.ListTitle(c.list), description)), nil
				}
				return reply(404, `{"error":"RecordNotFound"}`), nil
			case "/xrpc/com.atproto.repo.createRecord":
				if fail {
					return reply(503, `{"error":"Unavailable"}`), nil
				}
				var payload struct {
					Record struct{ List, Subject string }
				}
				requireOK(t, json.NewDecoder(r.Body).Decode(&payload))
				if payload.Record.List != c.list || payload.Record.Subject != did {
					t.Fatal(payload)
				}
				writes++
				return reply(200, "{}"), nil
			default:
				t.Fatalf("unexpected %s", r.URL)
				return nil, nil
			}
		})
		if fail {
			if err := c.publishWithDescription(t.Context()); err == nil {
				t.Fatal("lost failed write")
			}
			if len(pairs(t, a, "SELECT did,cluster FROM published")) != 0 || writes != 0 {
				t.Fatal("committed failed write")
			}
			execute(t, a, "UPDATE retries SET due=0")
			fail = false
		}
		requireOK(t, c.publishWithDescription(t.Context()))
		if err := c.publishWithDescription(t.Context()); !errors.Is(err, sql.ErrNoRows) || writes != 1 {
			t.Fatalf("writes %d after retry, err %v", writes, err)
		}
	})
}
