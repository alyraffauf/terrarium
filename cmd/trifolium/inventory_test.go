package main

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/xrpc"
)

func TestRestoredInventoryRequiresConfirmedRemoval(t *testing.T) {
	const owner, actor = "did:example:owner", "did:example:actor"
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err = (store{db}).initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	a := service{store: store{db}, owner: owner, targets: map[string]bool{}}
	lists, err := a.setupClusters(t.Context(), map[string]clusterConfig{"garden": {Accounts: []string{"did:example:target"}, Threshold: 1}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	malformed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if malformed {
			fmt.Fprint(w, `{"records":null}`)
			return
		}
		fmt.Fprintf(w, `{"records":[{"uri":%q,"value":{"$type":"app.bsky.graph.listitem","subject":%q,"list":%q}}]}`, "at://"+owner+"/"+palustre.ItemCollection+"/manual", actor, lists[0].list)
	}))
	defer server.Close()
	a.api = &palustre.Session{Refreshed: time.Now(), Client: &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: owner}}}
	if err := a.auditMembers(t.Context(), lists); err != nil {
		t.Fatal(err)
	}
	var did string
	var include bool
	if err := db.QueryRow(nextChange, "garden", 1, owner).Scan(&did, &include); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unverified actor published: %s %v %v", did, include, err)
	}
	keys, err := a.MemberKeys(t.Context(), "garden", actor)
	if err != nil || len(keys) != 1 || keys[0] != "manual" {
		t.Fatal(keys, err)
	}
	malformed = true
	if err := a.auditMembers(t.Context(), lists); err == nil {
		t.Fatal("accepted malformed inventory")
	}
	if _, err := db.Exec("DELETE FROM scans WHERE did=?", actor); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(nextChange, "garden", 1, owner).Scan(&did, &include); err != nil || did != actor || include {
		t.Fatalf("expected verified removal: %s %v %v", did, include, err)
	}
}

type responseTransport func(*http.Request) (*http.Response, error)

func (f responseTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestProfileOutageStillPublishesToOwnedList(t *testing.T) {
	const owner, actor = "did:example:owner", "did:example:actor"
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err = (store{db}).initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	profiles := 0
	a := service{store: store{db}, owner: owner, targets: map[string]bool{}, http: &http.Client{Transport: responseTransport(func(r *http.Request) (*http.Response, error) {
		profiles++
		return nil, errors.New("AppView unavailable")
	})}}
	lists, err := a.setupClusters(t.Context(), map[string]clusterConfig{"garden": {Accounts: []string{"did:example:a", "did:example:b"}, Threshold: 2}}, 2)
	if err != nil {
		t.Fatal(err)
	}
	c := lists[0]
	const description = "Accounts clustered around @a.example."
	if _, err = db.Exec("INSERT INTO descriptions VALUES(?,?)", c.id, description); err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO edges VALUES(?, 'a','did:example:a'),(?, 'b','did:example:b')", actor, actor)
	if err != nil {
		t.Fatal(err)
	}
	additions := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.repo.getRecord":
			if r.URL.Query().Get("collection") == palustre.ListCollection {
				fmt.Fprintf(w, `{"uri":%q,"value":{"$type":"app.bsky.graph.list","name":%q,"description":%q,"purpose":"app.bsky.graph.defs#modlist"}}`, c.list, palustre.ListTitle(c.list), description)
			} else {
				http.Error(w, `{"error":"RecordNotFound"}`, 404)
			}
		case "/xrpc/com.atproto.repo.createRecord":
			additions++
			fmt.Fprint(w, "{}")
		default:
			t.Errorf("unexpected request %s", r.URL)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	a.api = &palustre.Session{Refreshed: time.Now(), Client: &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: owner}}}
	if err := c.publishWithDescription(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := c.publishWithDescription(t.Context()); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	if profiles != 1 || additions != 1 {
		t.Fatalf("profile lookups=%d additions=%d", profiles, additions)
	}
}

func TestRecognizeRestoredDescription(t *testing.T) {
	for _, line := range []string{"Accounts with overlapping follows.", "Accounts clustered around @a.example.", "Accounts clustered around @a.example and @b.example.", "Accounts clustered around @a.example, @b.example, and @c.example."} {
		if !isClusterDescription(line) {
			t.Errorf("failed to recognize %q", line)
		}
	}
	for _, line := range []string{"My favorites", "Accounts clustered around a.example.", "Accounts clustered around @a.example, @b.example.", "Accounts clustered around @a.example. More prose."} {
		if isClusterDescription(line) {
			t.Errorf("accepted %q", line)
		}
	}
}
