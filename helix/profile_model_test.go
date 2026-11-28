package helix

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	m "github.com/alyraffauf/terrarium/internal/syncmodel"
	"github.com/bluesky-social/indigo/xrpc"
)

func FuzzProfileChanges(f *testing.F) {
	f.Add([]byte{1, 2, 9, 4})
	f.Fuzz(func(t *testing.T, trace []byte) {
		w := &m.World{}
		client := &http.Client{Transport: w}
		api := newSession(Config{AppView: "https://profiles.example", WriteInterval: time.Nanosecond}, client, &xrpc.Client{Host: "https://owner.example", Client: client, Auth: &xrpc.AuthInfo{Did: m.Owner}})
		path := t.TempDir() + "/db"
		var title, description string
		for i, b := range trace[:min(len(trace), 12)] {
			db, err := openStore(path)
			if err != nil {
				t.Fatal(err)
			}
			w.Handle = fmt.Sprintf("account%d.example", b%8)
			w.Suspended = b&8 != 0
			target := listTarget{session: api, actor: m.Actors[0], db: db, listURI: m.List(0)}
			err = target.ensureList(t.Context())
			db.Close()
			if err != nil {
				t.Fatal(err)
			}
			if target.suspended.Load() {
				t.Fatal("wrong suspension status")
			}
			record := w.Record("list0")
			description = "Followers of " + w.Handle
			if title == "" {
				title, _ = record["name"].(string)
				if title == "" {
					t.Fatal("missing title")
				}
			}
			if record["name"] != title || record["description"] != description {
				t.Fatalf("profile %d list %v want %q %q", i, record, title, description)
			}
		}
	})
}
