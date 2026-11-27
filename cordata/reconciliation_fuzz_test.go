package cordata

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/xrpc"
)

const owner = "did:example:owner"
const post = "at://did:example:author/app.bsky.feed.post/post"
const list = "at://" + owner + "/app.bsky.graph.list/managed"
const otherList = "at://" + owner + "/app.bsky.graph.list/manual"

type remoteItem struct {
	Type    string `json:"$type"`
	List    string `json:"list"`
	Subject string `json:"subject"`
}
type testRepository struct {
	likes  map[string]bool
	items  map[string]remoteItem
	fault  byte
	writes int
}

func wire(status int, body any) (*http.Response, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
}

// Two records per page makes even short histories cross cursor boundaries.
func window[T any](rows []T, cursor string) ([]T, string, error) {
	start := 0
	if cursor != "" {
		var err error
		start, err = strconv.Atoi(cursor)
		if err != nil || start < 0 || start > len(rows) {
			return nil, "", fmt.Errorf("invalid cursor %q", cursor)
		}
	}
	end := min(start+2, len(rows))
	next := ""
	if end < len(rows) {
		next = strconv.Itoa(end)
	}
	return rows[start:end], next, nil
}

func (repo *testRepository) RoundTrip(req *http.Request) (*http.Response, error) {
	q := req.URL.Query()
	switch strings.TrimPrefix(req.URL.Path, "/xrpc/") {
	case "app.bsky.feed.getLikes":
		if q.Get("uri") != post {
			return nil, fmt.Errorf("wrong post %q", q.Get("uri"))
		}
		likes := make([]any, 0, len(repo.likes))
		for _, did := range slices.Sorted(maps.Keys(repo.likes)) {
			likes = append(likes, map[string]any{"actor": map[string]string{"did": did}})
		}
		page, next, err := window(likes, q.Get("cursor"))
		if err != nil {
			return nil, err
		}
		reply := map[string]any{"uri": post, "likes": page, "cursor": next}
		switch repo.fault {
		case 1:
			if next == "" {
				delete(reply, "likes")
			}
		case 2:
			reply["likes"] = nil
		case 3:
			reply["uri"] = otherList
		case 4:
			reply["cursor"] = "0"
		case 5:
			if next == "" {
				reply["likes"] = []any{map[string]int{"actor": 99}}
			}
		}
		return wire(200, reply)
	case "com.atproto.repo.listRecords":
		if q.Get("repo") != owner || q.Get("collection") != palustre.ItemCollection {
			return nil, fmt.Errorf("wrong collection")
		}
		records := make([]any, 0, len(repo.items))
		for _, key := range slices.Sorted(maps.Keys(repo.items)) {
			records = append(records, map[string]any{"uri": "at://" + owner + "/" + palustre.ItemCollection + "/" + key, "value": repo.items[key]})
		}
		page, next, err := window(records, q.Get("cursor"))
		if err != nil {
			return nil, err
		}
		if next == "" && repo.fault == 6 {
			return wire(200, map[string]any{"records": nil})
		}
		return wire(200, map[string]any{"records": page, "cursor": next})
	case "com.atproto.repo.getRecord":
		v, ok := repo.items[q.Get("rkey")]
		if !ok {
			return wire(404, map[string]string{"error": "RecordNotFound"})
		}
		return wire(200, map[string]any{"cid": "current", "value": v})
	case "com.atproto.repo.createRecord", "com.atproto.repo.deleteRecord":
		var in struct {
			Rkey, Repo, Collection, SwapRecord string
			Record                             remoteItem
		}
		if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
			return nil, err
		}
		if in.Repo != owner || in.Collection != palustre.ItemCollection {
			return nil, fmt.Errorf("wrong write destination")
		}
		if strings.HasSuffix(req.URL.Path, "createRecord") {
			if _, ok := repo.items[in.Rkey]; ok {
				return wire(409, map[string]string{"error": "RecordAlreadyExists"})
			}
			repo.items[in.Rkey] = in.Record
		} else {
			if in.SwapRecord != "current" {
				return nil, fmt.Errorf("unconditional deletion")
			}
			delete(repo.items, in.Rkey)
		}
		repo.writes++
		return wire(200, map[string]string{})
	default:
		return nil, fmt.Errorf("unexpected request %s", req.URL)
	}
}

func (repo *testRepository) scan(ctx context.Context) error {
	client := &xrpc.Client{Host: "https://repo.test", Client: &http.Client{Transport: repo}, Auth: &xrpc.AuthInfo{Did: owner}}
	api := &palustre.Session{Client: client, Refreshed: time.Now(), Config: palustre.Config{WriteInterval: time.Nanosecond}}
	return syncPost(ctx, api, client, post, list)
}

func FuzzLikerMembership(f *testing.F) {
	f.Add([]byte{0, 8, 16, 0, 3, 4, 1, 5, 63})
	f.Add([]byte{2, 10, 18, 25, 31, 39, 47, 55, 63, 7})
	f.Add([]byte{0, 6, 2, 6}) // A published liker unlikes between scans.
	f.Add([]byte{3, 5, 6})    // Remove a manual member while keeping an unrelated list intact.
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, history []byte) {
		repo := testRepository{likes: map[string]bool{}, items: map[string]remoteItem{}}
		check := func(fault byte) {
			t.Helper()
			repo.fault = fault
			before, writes := maps.Clone(repo.items), repo.writes
			err := repo.scan(t.Context())
			if fault != 0 {
				if err == nil || writes != repo.writes || !maps.Equal(before, repo.items) {
					t.Fatalf("incomplete scan %d changed membership or succeeded", fault)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			count := map[string]int{}
			for key, it := range repo.items {
				if it.List == list {
					count[it.Subject]++
				} else if original, ok := before[key]; !ok || original != it {
					t.Fatal("unrelated item changed")
				}
			}
			for key, it := range before {
				if it.List != list && repo.items[key] != it {
					t.Fatal("unrelated item removed")
				}
			}
			if len(count) != len(repo.likes) {
				t.Fatalf("members %v, likes %v", count, repo.likes)
			}
			for did := range repo.likes {
				if count[did] != 1 {
					t.Fatalf("expected exactly one membership for %s", did)
				}
			}
		}
		for i, b := range history[:min(len(history), 32)] {
			did := fmt.Sprintf("did:example:actor%d", (b>>3)%4)
			switch b % 8 {
			case 0, 1:
				repo.likes[did] = true
			case 2:
				delete(repo.likes, did)
			case 3, 4:
				repo.items[fmt.Sprintf("manual%d", i)] = remoteItem{palustre.ItemCollection, list, did}
			case 5:
				repo.items[fmt.Sprintf("other%d", i)] = remoteItem{palustre.ItemCollection, otherList, did}
			case 6:
				check(0)
			case 7:
				check(1 + (b>>3)%6)
			}
		}
		check(0)
		writes := repo.writes
		check(0)
		if repo.writes != writes {
			t.Fatal("stable list required another write")
		}
	})
}

func FuzzPostReference(f *testing.F) {
	f.Add([]byte("a.example"), uint8(0))
	f.Add([]byte("b"), uint8(3))
	f.Add([]byte{0xff, ':', '/'}, uint8(7))
	f.Fuzz(func(t *testing.T, data []byte, mode uint8) {
		// Start from valid components so mutations exercise successful parsing as
		// well as targeted corruption. Also feed the unmodified bytes to the parser.
		data = data[:min(len(data), 64)]
		_, _ = parsePost(string(data))
		token := fmt.Sprintf("a%x", data[:min(len(data), 24)])
		actor := token + ".example"
		if mode&8 != 0 {
			actor = "did:example:" + token
		}
		key := fmt.Sprintf("k%x", data)
		want := "at://" + actor + "/app.bsky.feed.post/" + key
		link := "https://bsky.app/profile/" + actor + "/post/" + key
		input := link
		switch mode % 8 {
		case 0:
			input = want
		case 1: // Valid bare link.
		case 2:
			input += "?lang=en"
		case 3:
			input = strings.Replace(link, "/post/", "/feed/", 1)
		case 4:
			input = strings.Replace(link, "https://bsky.app", "https://other.example", 1)
		case 5:
			input = want + "/extra"
		case 6:
			input = strings.TrimSuffix(want, key)
		case 7:
			input = strings.Replace(want, "app.bsky.feed.post", "app.bsky.graph.list", 1)
		}
		got, err := parsePost(input)
		if mode%8 <= 2 {
			if err != nil || got.String() != want {
				t.Fatalf("parse %q = %q, %v; want %q", input, got, err, want)
			}
		} else if err == nil {
			t.Fatalf("accepted invalid post %q", input)
		}
	})
}
