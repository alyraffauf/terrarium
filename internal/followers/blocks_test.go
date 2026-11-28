package followers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/xrpc"
)

func TestBlockRefreshPreservesExistingExclusionsOnInterruptedPage(t *testing.T) {
	ctx := t.Context()
	db, err := palustre.OpenSQLite(ctx, t.TempDir()+"/followers")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec("CREATE TABLE follower_blocks(target TEXT,actor TEXT,PRIMARY KEY(target,actor)); INSERT INTO follower_blocks VALUES('did:example:target','did:example:old')")
	if err != nil {
		t.Fatal(err)
	}
	complete := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("source") != "app.bsky.graph.block:subject" || q.Get("subject") != "did:example:target" {
			t.Error(q)
		}
		if q.Get("cursor") == "" {
			fmt.Fprint(w, `{"records":[{"did":"did:example:new","collection":"app.bsky.graph.block","rkey":"block"}],"cursor":"2"}`)
			return
		}
		if !complete {
			http.Error(w, "failed", 503)
			return
		}
		fmt.Fprint(w, `{"records":[]}`)
	}))
	defer server.Close()
	changes := map[string]int{}
	refresh := func() error {
		return refreshBlocks(ctx, db, &xrpc.Client{Host: server.URL}, "did:example:target", func(_ context.Context, _ *sql.Tx, actor string) error { changes[actor]++; return nil })
	}
	if refresh() == nil || len(changes) != 0 {
		t.Fatal("partial snapshot queued membership changes")
	}
	complete = true
	if err := refresh(); err != nil {
		t.Fatal(err)
	}
	if err := refresh(); err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 || changes["did:example:old"] != 1 || changes["did:example:new"] != 1 {
		t.Fatal(changes)
	}
}
