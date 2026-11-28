package palustre

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/xrpc"
)

const owner = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
const listURI = "at://" + owner + "/" + ListCollection + "/sample"

type object = map[string]any
type localRPC func(*http.Request) (int, any)

func (serve localRPC) RoundTrip(r *http.Request) (*http.Response, error) {
	status, body := serve(r)
	w := httptest.NewRecorder()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	err := json.NewEncoder(w).Encode(body)
	return w.Result(), err
}
func session(serve localRPC) *Session {
	h := &http.Client{Transport: serve}
	return &Session{Config: Config{LoginPDS: "https://pds.invalid", WriteInterval: time.Nanosecond}, HTTP: h, Client: &xrpc.Client{Host: "https://pds.invalid", Client: h, Auth: &xrpc.AuthInfo{Did: owner, AccessJwt: "access", RefreshJwt: "refresh"}}}
}

type keys map[string]bool

func (k keys) MemberKeys(context.Context, string, string) ([]string, error) {
	return slices.Sorted(maps.Keys(k)), nil
}
func (k keys) RememberMember(_ context.Context, _, _, key string) error { k[key] = true; return nil }
func (k keys) ForgetMember(_ context.Context, _, key string) error      { delete(k, key); return nil }

func FuzzMembership(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 0, 1})
	f.Fuzz(func(t *testing.T, history []byte) {
		db := keys{}
		remote := map[string]string{}
		writes := 0
		raceKey := ""
		ambiguousCreate := false
		api := session(func(r *http.Request) (int, any) {
			key := r.URL.Query().Get("rkey")
			var req struct {
				Rkey, SwapRecord string
				Record           struct{ List, Subject string }
			}
			if r.Body != nil {
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Fatal(err)
				}
				key = req.Rkey
			}
			switch strings.TrimPrefix(r.URL.Path, "/xrpc/com.atproto.repo.") {
			case "getRecord":
				if subject, ok := remote[key]; ok {
					return 200, object{"uri": listURI, "cid": "cid-" + subject, "value": object{"$type": ItemCollection, "subject": subject, "list": listURI}}
				}
				return 400, object{"error": "RecordNotFound"}
			case "createRecord":
				if req.Record.Subject != owner || req.Record.List != listURI {
					t.Fatal(req)
				}
				if _, occupied := remote[key]; occupied {
					return 400, object{"error": "RecordAlreadyExists"}
				}
				remote[key] = owner
				writes++
				if ambiguousCreate {
					return 503, object{"error": "Unavailable"}
				}
			case "deleteRecord":
				if key == raceKey {
					raceKey = ""
					remote[key] = "did:web:other.example"
				}
				if req.SwapRecord != "cid-"+remote[key] {
					return 400, object{"error": "InvalidSwap"}
				}
				delete(remote, key)
				writes++
			default:
				t.Fatal(r.URL.Path)
			}
			return 200, object{}
		})
		for i, op := range history[:min(48, len(history))] {
			key := fmt.Sprint(i)
			inc := op&1 != 0
			ambiguousCreate = op&8 != 0
			switch op % 8 {
			case 2, 3:
				db[key] = true
				remote[key] = owner
			case 4, 5:
				db[key] = true
				remote[key] = "did:web:other.example"
			case 6:
				db[key] = true
				remote[key] = owner
				raceKey = key
			case 7:
				db[key] = true
			}
			member := Membership{Target: "target", DID: owner, ListURI: listURI, Include: inc}
			prior := maps.Clone(remote)
			err := api.Reconcile(t.Context(), db, member)
			if XRPCCode(err, "InvalidSwap") {
				prior = maps.Clone(remote)
				err = api.Reconcile(t.Context(), db, member)
			}
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for k, v := range remote {
				if v == owner {
					count++
					if !db[k] {
						t.Fatal("untracked member", k)
					}
				}
			}
			for k, v := range prior {
				if v != owner && remote[k] != v {
					t.Fatal("foreign edit lost", k)
				}
			}
			if count != len(db) || count > 1 || (count == 1) != inc {
				t.Fatalf("include=%v store=%v remote=%v", inc, db, remote)
			}
			before := writes
			if err := api.Reconcile(t.Context(), db, member); err != nil || before != writes {
				t.Fatal("replay", err)
			}
		}
	})
}

func FuzzInventory(f *testing.F) {
	for mode := byte(0); mode < 6; mode++ {
		f.Add([]byte{1, 2, 1}, mode, false)
		f.Add([]byte{1, 2}, mode, true)
	}
	f.Fuzz(func(t *testing.T, rows []byte, mode byte, items bool) {
		rows = rows[:min(24, len(rows))]
		mode %= 6
		calls := 0
		expected := map[string]bool{}
		targets := map[string]bool{"did:web:1.example": true, "did:web:2.example": true}
		api := session(func(r *http.Request) (int, any) {
			i := calls
			calls++
			if i > len(rows)+1 {
				t.Fatal("unbounded pagination")
			}
			cursor := r.URL.Query().Get("cursor")
			if i > 0 && cursor != fmt.Sprint(i) {
				t.Fatalf("cursor %q page %d", cursor, i)
			}
			if i >= len(rows) {
				switch mode {
				case 1:
					return 200, object{}
				case 2:
					if i > 0 {
						return 200, object{"cursor": cursor, "records": []any{}}
					}
				case 3:
					return 200, object{"records": []any{object{"uri": "invalid"}}}
				}
				return 200, object{"records": []any{}}
			}
			subject := fmt.Sprintf("did:web:%d.example", rows[i]%4)
			collection := FollowCollection
			if items {
				collection = ItemCollection
			}
			typ := collection
			if mode == 4 {
				typ = ListCollection
			}
			return 200, object{"records": []any{object{"uri": fmt.Sprintf("at://%s/%s/%d", owner, collection, i), "value": object{"$type": typ, "subject": subject, "list": listURI}}}, "cursor": fmt.Sprint(i + 1)}
		})
		scrape := api.metricsHandler(nil)
		reader := api.PDSVerificationClient(api.Client)
		pages := len(rows) + 1
		for i, b := range rows {
			s := fmt.Sprintf("did:web:%d.example", b%4)
			if mode != 4 && (items || mode != 5 || targets[s]) {
				expected[s] = true
				if !items && mode == 5 && len(expected) == len(targets) {
					pages = i + 1
					break
				}
			}
		}
		got := map[string]bool{}
		var err error
		if items {
			err = WalkListItems(t.Context(), api.Client, func(l, d, k string) error {
				if l != listURI || k != fmt.Sprint(calls-1) {
					t.Fatal(l, d, k)
				}
				got[d] = true
				return nil
			})
		} else if mode == 5 {
			got, err = FetchTargetFollows(t.Context(), reader, owner, targets)
		} else {
			got, err = FetchFollows(t.Context(), reader, owner)
		}
		result := httptest.NewRecorder()
		scrape.ServeHTTP(result, httptest.NewRequest("GET", "/metrics", nil))
		const count = "terrarium_pds_verification_read_duration_seconds_count"
		want := fmt.Sprintf("\n%s{method=\"com.atproto.repo.listRecords\",result=\"success\"} %d\n", count, calls)
		if items && strings.Contains(result.Body.String(), count) || !items && !strings.Contains(result.Body.String(), want) {
			t.Fatalf("inventory requests=%d, metrics=%s", calls, result.Body.String())
		}
		bad := mode == 1 || mode == 3 || mode == 2 && len(rows) > 0 || items && mode == 4 && len(rows) > 0
		if bad != (err != nil) || err == nil && (!maps.Equal(expected, got) || calls != pages) || err != nil && !items && got != nil {
			t.Fatalf("want %v/%d bad=%v got %v/%d %v", expected, pages, bad, got, calls, err)
		}
	})
}
