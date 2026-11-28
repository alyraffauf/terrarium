package helix

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/alyraffauf/terrarium/helix/storage"
	"github.com/alyraffauf/terrarium/internal/followers"
	m "github.com/alyraffauf/terrarium/internal/syncmodel"
	"github.com/bluesky-social/indigo/xrpc"
)

type hintPDS struct {
	*m.World
	valid        bool
	exact, pages int
}

func (p *hintPDS) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Path == "/xrpc/com.atproto.repo.listRecords" {
		p.pages++
	}
	if r.URL.Path == "/xrpc/com.atproto.repo.getRecord" && r.URL.Query().Get("collection") == "app.bsky.graph.follow" {
		p.exact++
		if p.valid {
			body := fmt.Sprintf(`{"uri":"at://%s/app.bsky.graph.follow/key","value":{"$type":"app.bsky.graph.follow","subject":"%s"}}`, m.Subject, m.Actors[0])
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		}
	}
	return p.World.RoundTrip(r)
}

// Each operation changes the discovery evidence for one pending account.
// A verified key can establish one positive; unrelated lists require a full read.
func FuzzFollowHints(f *testing.F) {
	f.Add([]byte{0, 1, 3, 2, 4, 0, 5, 1, 0})
	f.Fuzz(func(t *testing.T, history []byte) {
		db, err := openStore(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		p := &hintPDS{World: &m.World{Active: true, Follows: 3}, valid: true}
		client := &http.Client{Transport: p}
		api := newSession(Config{Slingshot: "https://identity.example"}, client, &xrpc.Client{Client: client, Auth: &xrpc.AuthInfo{Did: m.Owner}})
		ctx := t.Context()
		var pending, full, hasKey bool
		var first string
		for _, b := range history[:min(24, len(history))] {
			op := b % 6
			switch op {
			case 0, 1, 2:
				target := m.Actors[0]
				key := ""
				if op == 0 {
					key = "key"
				}
				if op == 2 {
					target = m.Actors[1]
				}
				err = db.queueDiscoveredFollows(ctx, []storage.QueueDiscoveredFollowParams{{Did: m.Subject, HintTarget: target, HintRkey: key}})
				if pending && target != first {
					full = true
				}
				if !pending {
					first = target
				}
				hasKey = hasKey || key != ""
				pending = true
			case 3:
				p.valid = !p.valid
			case 4:
				err = db.Ingest(ctx, m.Subject, followers.Checkpoint{}, false)
				full = true
				pending = true
			case 5:
				if pending {
					var j job
					j, err = db.next(ctx)
					if err == nil {
						err = db.finish(ctx, j)
					}
				}
				pending = false
				full = false
				hasKey = false
				first = ""
			}
			if err != nil {
				t.Fatal(err)
			}
			if !pending {
				continue
			}
			p.exact, p.pages = 0, 0
			state, err := api.subject(ctx, db, m.Subject, m.Actors[:]...)
			if err != nil {
				t.Fatal(err)
			}
			probe := !full && hasKey
			quick := probe && p.valid
			if (p.exact == 1) != probe || (p.pages == 0) != quick || (state.onlyTarget == m.Actors[0]) != quick || !state.follows[m.Actors[0]] || state.follows[m.Actors[1]] == quick {
				t.Fatalf("op=%d full=%t hint=%t valid=%t exact=%d pages=%d state=%+v", op, full, hasKey, p.valid, p.exact, p.pages, state)
			}
		}
	})
}
