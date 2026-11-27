package syncmodel

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

const Owner = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
const Subject = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"

var Actors = [2]string{"did:plc:cccccccccccccccccccccccc", "did:plc:dddddddddddddddddddddddd"}
var Hosts = [2]string{"https://one.example", "https://two.example"}

type World struct {
	MoveOnDelete     string
	BeforeRecordRead func()
	BeforeResolve    func(*http.Request) error
	AfterCreate      func(string)
	RejectDelete     string
	Unresolved       bool
	Handle           string
	Suspended        bool

	Host      int
	Active    bool
	Follows   byte
	ambiguous bool
	records   map[string]map[string]any
}

func (w *World) RoundTrip(r *http.Request) (*http.Response, error) {
	code := 200
	var value any = map[string]any{}
	q := r.URL.Query()
	switch strings.TrimPrefix(r.URL.Path, "/xrpc/") {
	case "app.bsky.actor.getProfile":
		if w.Suspended {
			code = 400
			value = map[string]string{"error": "AccountTakedown"}
		} else {
			value = map[string]string{"did": q.Get("actor"), "handle": w.Handle}
		}
	case "blue.microcosm.identity.resolveMiniDoc":
		if w.BeforeResolve != nil {
			if err := w.BeforeResolve(r); err != nil {
				return nil, err
			}
		}
		if w.Unresolved {
			code = 404
			value = map[string]string{"error": "NotFound"}
		} else {
			value = map[string]any{"did": q.Get("identifier"), "pds": Hosts[w.Host]}
		}
	case "com.atproto.sync.getRepoStatus":
		value = map[string]any{"did": q.Get("did"), "active": w.Active}
	case "com.atproto.repo.listRecords":
		records := make([]any, 0, len(Actors))
		for i, actor := range Actors {
			if w.Follows&(1<<i) != 0 {
				records = append(records, map[string]any{"uri": fmt.Sprintf("at://%s/app.bsky.graph.follow/%d", q.Get("repo"), i), "value": map[string]any{"$type": "app.bsky.graph.follow", "subject": actor}})
			}
		}
		value = map[string]any{"records": records}
	case "com.atproto.repo.getRecord":
		if w.BeforeRecordRead != nil {
			hook := w.BeforeRecordRead
			w.BeforeRecordRead = nil
			hook()
		}
		rec, ok := w.records[q.Get("rkey")]
		if !ok {
			code = 404
			value = map[string]any{"error": "RecordNotFound"}
		} else {
			value = map[string]any{"uri": "at://" + Owner + "/" + q.Get("collection") + "/" + q.Get("rkey"), "value": rec, "cid": "bafyreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
		}
	case "com.atproto.repo.createRecord", "com.atproto.repo.deleteRecord", "com.atproto.repo.putRecord":
		var in struct {
			Rkey, SwapRecord string
			Record           map[string]any
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			return nil, err
		}
		if in.Record != nil {
			if w.records == nil {
				w.records = map[string]map[string]any{}
			}
			w.records[in.Rkey] = in.Record
			if w.AfterCreate != nil {
				subject, _ := in.Record["subject"].(string)
				w.AfterCreate(subject)
			}
			if w.ambiguous {
				code = 500
				value = map[string]any{"error": "InternalServerError"}
			}
		} else {
			if in.SwapRecord != "bafyreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
				return nil, fmt.Errorf("unguarded deletion")
			}
			if w.MoveOnDelete == in.Rkey {
				w.SeedItem(in.Rkey, List(2), Subject)
				code = 400
				value = map[string]string{"error": "InvalidSwap"}
			} else if w.RejectDelete == in.Rkey {
				code = 503
				value = map[string]string{"error": "Unavailable"}
			} else {
				delete(w.records, in.Rkey)
			}
		}
	default:
		return nil, fmt.Errorf("unexpected request %s", r.URL)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(data)), Request: r}, nil
}
func (w *World) Record(key string) map[string]any { return w.records[key] }
func (w *World) RecordCount() int                 { return len(w.records) }

func List(i int) string { return fmt.Sprintf("at://%s/app.bsky.graph.list/list%d", Owner, i) }

// Each round changes the external facts, then reconciles one account on both targets.
func Service(f *testing.F, byHost bool, setup func(*testing.T, *World) func(string, bool, int, bool) error) {
	f.Add([]byte{7, 15, 39, 135, 23, 7, 0, 7, 71})
	f.Fuzz(func(t *testing.T, trace []byte) {
		w := &World{records: map[string]map[string]any{}}
		run := setup(t, w)
		var expected [2]bool
		for step, b := range trace[:min(len(trace), 24)] {
			w.Active = b&4 != 0
			w.Host = int(b >> 3 & 1)
			w.Follows = b & 3
			w.ambiguous = b&128 != 0
			did := Subject
			if b&64 != 0 {
				did = Owner
			}
			excluded := b&16 != 0
			paused := -1
			if b&32 != 0 {
				paused = w.Host
			}
			if err := run(did, excluded, paused, b&128 != 0); err != nil {
				t.Fatalf("step %d: %v", step, err)
			}
			if did == Subject {
				for i := range expected {
					if i == paused {
						continue
					}
					eligible := w.Follows&(1<<i) != 0
					if byHost {
						eligible = w.Host == i
					}
					expected[i] = w.Active && !excluded && eligible
				}
			}
			counts := map[any]int{}
			for _, rec := range w.records {
				if rec["subject"] != Subject {
					t.Fatalf("unexpected subject %v", rec)
				}
				counts[rec["list"]]++
			}
			for i, yes := range expected {
				n := 0
				if yes {
					n = 1
				}
				if counts[List(i)] != n {
					t.Fatalf("step %d target %d got %v want %v", step, i, counts, expected)
				}
				delete(counts, List(i))
			}
			if len(counts) > 0 {
				t.Fatal("unknown lists", counts)
			}
		}
	})
}

func (w *World) SeedItem(key, list, subject string) {
	if w.records == nil {
		w.records = map[string]map[string]any{}
	}
	w.records[key] = map[string]any{"$type": "app.bsky.graph.listitem", "list": list, "subject": subject}
}
