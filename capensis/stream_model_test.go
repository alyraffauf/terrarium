package capensis

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	m "github.com/alyraffauf/terrarium/internal/syncmodel"
)

func FuzzStreamRouting(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 19, 35, 67})
	f.Fuzz(func(t *testing.T, history []byte) {
		db, err := openStore(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		ctx := t.Context()
		var cursor, version int64
		for i, b := range history[:min(len(history), 48)] {
			kind := []string{"sync", "account", "identity", "commit", "commit", "commit", "commit", "broken"}[b%8]
			did := m.Subject
			if b&16 != 0 {
				did = m.Owner
			}
			subject := m.Actors[0]
			if b%8 == 4 {
				subject = m.Actors[1]
			}
			if b%8 == 5 {
				subject = "bad"
			}
			collection := "app.bsky.graph.follow"
			if b%8 == 6 {
				collection = "app.bsky.feed.post"
			}
			payload := []byte(fmt.Sprintf(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#%s","did":%q,"seq":%d,"time":"2026-01-01T00:00:00Z","collection":%q,"operation":"create","rkey":"ok","record":{"$type":"app.bsky.graph.follow","subject":%q}}}`, kind, did, i+1, collection, subject))
			save := b&32 != 0
			err = ingestFrame(ctx, db, "wss://jet.example", m.Owner, payload, save)
			queued := kind == "sync" || kind == "account" || kind == "identity" || b%8 == 5
			if err != nil {
				t.Fatal(err)
			}
			dirty := did == m.Owner && (kind == "sync" || kind == "commit" && collection == "app.bsky.graph.follow")
			if dirty || kind == "broken" {
				version++
			}
			if queued || dirty || save || kind == "broken" {
				cursor = int64(i + 1)
			}
			if got, err := db.resumeCursor(ctx, "wss://jet.example"); err != nil || got != cursor {
				t.Fatalf("frame %d cursor %d want %d %v", i, got, cursor, err)
			}
			if got, _, err := db.FollowVersions(ctx); err != nil || got != version {
				t.Fatalf("frame %d version %d want %d %v", i, got, version, err)
			}
			work, err := db.next(ctx)
			if queued {
				if err != nil || work.DID != did {
					t.Fatalf("frame %d work %+v %v", i, work, err)
				}
				if err := db.finish(ctx, work); err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("frame %d unexpected work %+v %v", i, work, err)
			}
			rescan, err := db.takeRescan(ctx)
			if err != nil || rescan != (kind == "broken") {
				t.Fatalf("frame %d rescan %t %v", i, rescan, err)
			}
		}
	})
}
