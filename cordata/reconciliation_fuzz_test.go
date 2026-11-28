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
	postScan          postScan
	unavailable       map[string]bool
	verificationReads map[string]int
	links             map[string]string
	likes             map[string]bool
	items             map[string]remoteItem
	fault             byte
	writes            int
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
	case "blue.microcosm.links.getBacklinks":
		if q.Get("subject") != post || q.Get("source") != "app.bsky.feed.like:subject.uri" {
			return nil, fmt.Errorf("wrong backlinks query")
		}
		rows := []any{}
		for did := range repo.links {
			rows = append(rows, map[string]string{"did": did, "collection": "app.bsky.feed.like", "rkey": "record"})
		}
		if repo.fault == 7 {
			if q.Get("cursor") != "" {
				return wire(503, map[string]string{"error": "Unavailable"})
			}
			return wire(200, map[string]any{"records": rows, "cursor": "next"})
		}
		return wire(200, map[string]any{"records": rows})
	case "blue.microcosm.identity.resolveMiniDoc":
		return wire(200, map[string]string{"did": q.Get("identifier"), "pds": "https://actor.test"})
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
		if req.URL.Host == "actor.test" && q.Get("collection") == "app.bsky.feed.like" {
			did := q.Get("repo")
			if repo.verificationReads != nil {
				repo.verificationReads[did]++
			}
			if repo.unavailable[did] {
				return wire(400, map[string]string{"error": "InvalidRequest", "message": "Could not find repo: " + did})
			}
			return wire(200, map[string]any{"uri": "at://" + did + "/app.bsky.feed.like/record", "value": map[string]any{"$type": "app.bsky.feed.like", "subject": map[string]string{"uri": repo.links[did], "cid": "bafyreig3iv5ohbzqhbwj3swk6bib7pdry4gderik2x7dg2b5v3ak75sh2e"}}})
		}
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

func (repo *testRepository) scan(ctx context.Context) error { return repo.scanAt(ctx, time.Now()) }
func (repo *testRepository) scanAt(ctx context.Context, now time.Time) error {
	client := &xrpc.Client{Host: "https://repo.test", Client: &http.Client{Transport: repo}, Auth: &xrpc.AuthInfo{Did: owner}}
	api := &palustre.Session{HTTP: client.Client, Client: client, Refreshed: time.Now(), Config: palustre.Config{WriteInterval: time.Nanosecond}}
	repo.postScan.post, repo.postScan.list = post, list
	return repo.postScan.sync(ctx, &ownerSession{Session: api}, client, client.Host, func() time.Time { return now })
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

func TestVerifiedBacklinksDuringIncompleteDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fault byte
	}{{"incomplete AppView", 2}, {"incomplete Constellation", 7}} {
		t.Run(tc.name, func(t *testing.T) {
			const liker = "did:example:liker"
			repo := testRepository{likes: map[string]bool{}, links: map[string]string{liker: post, "did:example:other": otherList}, items: map[string]remoteItem{"old": {Type: palustre.ItemCollection, List: list, Subject: "did:example:old"}}, fault: tc.fault}
			if err := repo.scan(t.Context()); err == nil {
				t.Fatal("incomplete discovery reported success")
			}
			if len(repo.items) != 2 || repo.items["old"].Subject == "" {
				t.Fatalf("lost existing member: %v", repo.items)
			}
			for key, item := range repo.items {
				if key != "old" && item.Subject != liker {
					t.Fatalf("wrong backlink member: %v", item)
				}
			}
		})
	}
}

func TestUnreachableLikerOnlyProtectsTheirMembership(t *testing.T) {
	const missing = "did:example:missing"
	const missingNew = "did:example:missingnew"
	const added = "did:example:added"
	const stale = "did:example:stale"
	repo := testRepository{
		links:             map[string]string{missing: post, missingNew: post, added: post},
		unavailable:       map[string]bool{missing: true, missingNew: true},
		verificationReads: map[string]int{},
		items:             map[string]remoteItem{"keep": {palustre.ItemCollection, list, missing}, "remove": {palustre.ItemCollection, list, stale}},
	}
	if err := repo.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(repo.items) != 2 || repo.items["keep"].Subject != missing {
		t.Fatalf("want existing unresolved liker and confirmed new liker, got %v", repo.items)
	}
	for key, item := range repo.items {
		if key != "keep" && item.Subject != added {
			t.Fatal("unexpected member", item)
		}
	}
	// Replaying immediately must leave the same two members without probing an
	// unreachable PDS on every scan. Once the index drops its backlink, the
	// stale retry must no longer protect an existing member.
	if err := repo.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if repo.verificationReads[missing] != 1 || repo.verificationReads[missingNew] != 1 {
		t.Fatal("retried before due", repo.verificationReads)
	}
	delete(repo.links, missing)
	if err := repo.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, ok := repo.items["keep"]; ok {
		t.Fatal("stale verification retry prevented removal")
	}
}

func TestInterruptedBacklinkWalkPreservesOverdueRetry(t *testing.T) {
	const actor = "did:example:missing"
	key := likeRecordKey{actor, "record"}
	repo := testRepository{fault: 7, postScan: postScan{retries: map[likeRecordKey]verificationRetry{key: {failures: 4}}}, verificationReads: map[string]int{}, unavailable: map[string]bool{actor: true}}
	if err := repo.scan(t.Context()); err == nil {
		t.Fatal("expected second page to fail")
	}
	repo.fault = 0
	repo.links = map[string]string{actor: post}
	if err := repo.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := repo.postScan.retries[key].failures; got != 5 {
		t.Fatalf("failure count after interrupted walk = %d, want 5", got)
	}
}

// Each step changes one external fact or advances the clock, then runs a scan. The high bit interrupts backlink pagination.
func FuzzLikerVerificationHistory(f *testing.F) {
	f.Add([]byte{0, 2, 3, 5, 5, 4, 1})          // Keep a member during an outage, then recover.
	f.Add([]byte{2, 0, 8, 3, 5, 5, 1, 0, 6, 7}) // Retry a newcomer while another joins.
	f.Add([]byte{0, 129, 1})                    // Delay removal until a complete scan.
	f.Add([]byte{2, 0, 129, 128, 3, 5})         // Preserve a pending retry across a missing page.
	f.Fuzz(func(t *testing.T, history []byte) {
		repo := testRepository{likes: map[string]bool{}, links: map[string]string{}, unavailable: map[string]bool{}, verificationReads: map[string]int{}, items: map[string]remoteItem{}}
		var members [3]bool
		var probes, nextProbe, delay [3]int
		minute := 0
		actors := [3]string{"did:example:a", "did:example:b", "did:example:c"}
		for step, b := range history[:min(32, len(history))] {
			previousMembers, previousWrites := members, repo.writes
			interrupted := b&128 != 0
			repo.fault = 0
			if interrupted {
				repo.fault = 7
			}
			did := actors[int((b&127)/8)%len(actors)]
			switch b % 8 {
			case 0:
				repo.links[did] = post
			case 1:
				delete(repo.links, did)
			case 2:
				repo.unavailable[did] = true
			case 3:
				delete(repo.unavailable, did)
			case 4:
				minute += 60
			case 5:
				minute++
			case 6:
				repo.likes[did] = true
			case 7:
				delete(repo.likes, did)
			}
			for i, actor := range actors {
				switch {
				case !repo.likes[actor] && repo.links[actor] == "" && interrupted:
					continue
				case repo.likes[actor] || repo.links[actor] == "":
					members[i] = repo.likes[actor]
					delay[i] = 0
					nextProbe[i] = 0
				case minute >= nextProbe[i]:
					probes[i]++
					if repo.unavailable[actor] {
						delay[i] = min(60, max(1, 2*delay[i]))
						nextProbe[i] = minute + delay[i]
					} else {
						members[i] = true
						delay[i] = 0
						nextProbe[i] = 0
					}
				}
			}
			if err := repo.scanAt(t.Context(), time.Unix(int64(minute)*60, 0)); (err != nil) != interrupted {
				t.Fatalf("step %d: interrupted=%t, scan error=%v", step, interrupted, err)
			}
			writes := 0
			for i := range members {
				if members[i] != previousMembers[i] {
					writes++
				}
			}
			if got := repo.writes - previousWrites; got != writes {
				t.Fatalf("step %d: membership writes=%d, want %d", step, got, writes)
			}
			counts := map[string]int{}
			for _, item := range repo.items {
				counts[item.Subject]++
			}
			for i, actor := range actors {
				if (counts[actor] == 1) != members[i] || counts[actor] > 1 || repo.verificationReads[actor] != probes[i] {
					t.Fatalf("step %d %s: members=%d probes=%d, want member=%t probes=%d", step, actor, counts[actor], repo.verificationReads[actor], members[i], probes[i])
				}
			}
		}
	})
}
