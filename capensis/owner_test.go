package capensis

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluesky-social/indigo/xrpc"
)

func TestOwnerIsExcludedAndExistingSelfMembershipIsRemoved(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing=%t", existing), func(t *testing.T) {
			ctx := context.Background()
			db := testStore(t)
			removed := make(map[string]bool)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/xrpc/com.atproto.repo.getRecord" {
					key := r.URL.Query().Get("rkey")
					mockItem(w, key, ownerDID, "at://"+ownerDID+"/"+listCollection+"/"+key)
					return
				}
				if r.URL.Path != "/xrpc/com.atproto.repo.deleteRecord" {
					t.Errorf("self-exclusion made an unexpected request: %s", r.URL.Path)
					http.Error(w, `{"error":"UnexpectedRequest"}`, 500)
					return
				}
				var input struct {
					Repo string `json:"repo"`
					Rkey string `json:"rkey"`
				}
				if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
					t.Error(err)
				}
				if input.Repo != ownerDID {
					t.Error("removed record from wrong owner")
				}
				removed[input.Rkey] = true
				fmt.Fprint(w, `{}`)
			}))
			defer server.Close()
			owner := &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: ownerDID}}
			var targets []*listTarget
			for _, pds := range []string{"https://first.example", "https://second.example"} {
				targets = append(targets, &listTarget{session: newSession(Config{Slingshot: server.URL}, server.Client(), owner), pds: pds, target: owner, listURI: "at://" + ownerDID + "/" + listCollection + "/" + recordKey(pds)})
				if existing {
					if err := db.RememberMember(ctx, pds, ownerDID, recordKey(pds)); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := db.queries.EnqueueEvent(ctx, ownerDID); err != nil {
				t.Fatal(err)
			}
			work, err := db.next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := reconcileTargets(ctx, targets, work, db); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := db.conn.QueryRow(`SELECT count(*) FROM list_members WHERE did=?`, ownerDID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatal("owner remained in managed lists")
			}
			if existing && (!removed[recordKey("https://first.example")] || !removed[recordKey("https://second.example")]) {
				t.Fatal("existing self-memberships were not removed")
			}
			if !existing && len(removed) != 0 {
				t.Fatal("deleted records for an unlisted owner")
			}
		})
	}
}
