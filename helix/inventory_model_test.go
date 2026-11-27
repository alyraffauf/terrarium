package helix

import (
	"net/http/httptest"
	"testing"
	"time"

	m "github.com/alyraffauf/terrarium/internal/syncmodel"
	"github.com/bluesky-social/indigo/xrpc"
)

func FuzzInventory(f *testing.F) {
	m.Inventory(f, func(t *testing.T, srv *httptest.Server, paused int) (m.Store, func() error) {
		db, err := openStore(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		api := newSession(Config{}, srv.Client(), &xrpc.Client{Host: srv.URL, Client: srv.Client(), Auth: &xrpc.AuthInfo{Did: m.Owner}})
		targets := []*listTarget{}
		scan := inventoryScan{checks: map[string]time.Time{}}
		for i := range 2 {
			target := &listTarget{session: api, actor: m.Actors[i], listURI: m.List(i), rescanInterval: time.Hour}
			target.suspended.Store(i == paused)
			targets = append(targets, target)
			scan.checks[m.List(i)] = time.Now().Add(time.Hour)
		}
		return db, func() error { return scan.step(t.Context(), targets, db, nil) }
	})
}
