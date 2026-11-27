package palustre

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/xrpc"
)

const testListOwner = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
const testListURI = "at://" + testListOwner + "/" + ListCollection + "/test"

func TestEnsureListChecksConcurrentCreation(t *testing.T) {
	for _, test := range []struct {
		name, title string
		wantError   error
	}{
		{"unrelated list", "Another list", ErrUnrelatedList},
		{"matching list", ListTitle(testListURI), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			// The first lookup misses, then another writer claims the key.
			concurrentList := &bsky.GraphList{
				LexiconTypeID: ListCollection, Name: test.title,
				CreatedAt: "2026-01-01T00:00:00Z", Purpose: new(moderationListPurpose), Description: new("Target"),
			}
			var recordExists atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/xrpc/com.atproto.repo.getRecord":
					if !recordExists.Load() {
						w.WriteHeader(http.StatusBadRequest)
						fmt.Fprint(w, `{"error":"RecordNotFound"}`)
						return
					}
					json.NewEncoder(w).Encode(map[string]any{"cid": "concurrent-cid", "uri": testListURI, "value": concurrentList})
				case "/xrpc/com.atproto.repo.createRecord":
					var input atproto.RepoCreateRecord_Input
					if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.Rkey == nil || *input.Rkey != "test" {
						t.Errorf("invalid create request: %+v, %v", input, err)
					}
					recordExists.Store(true)
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, `{"error":"RecordAlreadyExists"}`)
				default:
					t.Errorf("unexpected mutation %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			api := newTestListSession(server)
			if err := api.EnsureList(t.Context(), testListURI, "Target"); !errors.Is(err, test.wantError) {
				t.Fatalf("create conflict result = %v, want %v", err, test.wantError)
			}
		})
	}
}

func TestEnsureListKeepsFacetsInUnchangedDescription(t *testing.T) {
	description := "Keep example\nPrevious target"
	list := &bsky.GraphList{
		LexiconTypeID: ListCollection, Name: ListTitle(testListURI), Description: &description,
		CreatedAt: "2026-01-01T00:00:00Z", Purpose: new(moderationListPurpose),
	}
	for _, index := range []bsky.RichtextFacet_ByteSlice{{ByteStart: 5, ByteEnd: 12}, {ByteStart: 13, ByteEnd: 21}} {
		list.DescriptionFacets = append(list.DescriptionFacets, &bsky.RichtextFacet{
			Index: &index, Features: []*bsky.RichtextFacet_Features_Elem{
				{RichtextFacet_Link: &bsky.RichtextFacet_Link{Uri: "https://example.com"}},
			},
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/xrpc/com.atproto.repo.getRecord":
			json.NewEncoder(w).Encode(map[string]any{"cid": "list-cid", "uri": testListURI, "value": list})
		case "/xrpc/com.atproto.repo.putRecord":
			var input atproto.RepoPutRecord_Input
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Errorf("decode update: %v", err)
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			updated := input.Record.Val.(*bsky.GraphList)
			if input.SwapRecord == nil || *input.SwapRecord != "list-cid" {
				t.Errorf("lost CID precondition: %v", input.SwapRecord)
			}
			if updated.Description == nil || *updated.Description != "Keep example\nNext" {
				t.Errorf("description = %v", updated.Description)
			}
			if len(updated.DescriptionFacets) != 1 || *updated.DescriptionFacets[0].Index != *list.DescriptionFacets[0].Index {
				t.Errorf("wrong facets kept: %v", updated.DescriptionFacets)
			}
			fmt.Fprint(w, `{}`)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	isOldClusterDescription := func(description string) bool { return description == "Previous target" }
	if err := newTestListSession(server).EnsureClusterList(t.Context(), testListURI, "Next", isOldClusterDescription); err != nil {
		t.Fatal(err)
	}
}

func TestListDescriptionLimits(t *testing.T) {
	for _, test := range []struct {
		name, text string
		valid      bool
	}{
		{"combining characters", strings.Repeat("e\u0301", 300), true},
		{"too many graphemes", strings.Repeat("x", 301), false},
		{"too many bytes", "x" + strings.Repeat("\u0301", 1500), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateListDescription(test.text); (err == nil) != test.valid {
				t.Fatalf("expected valid = %v, got %v", test.valid, err)
			}
		})
	}
}

func newTestListSession(server *httptest.Server) *Session {
	return &Session{
		Config: Config{WriteInterval: time.Nanosecond},
		Client: &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: testListOwner}},
	}
}
