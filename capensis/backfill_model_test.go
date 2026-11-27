package capensis

import (
	"database/sql"
	"errors"
	"net/http/httptest"
	"testing"

	m "github.com/alyraffauf/terrarium/internal/syncmodel"
	"github.com/bluesky-social/indigo/xrpc"
)

func FuzzBackfill(f *testing.F) {
	m.Backfill(f, true, func(t *testing.T, srv *httptest.Server) ([]string, error) {
		db, err := openStore(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		ctx := t.Context()
		if err = db.RememberMember(ctx, m.Actors[0], m.Owner, "old"); err != nil {
			t.Fatal(err)
		}
		if err = db.RememberMember(ctx, m.Actors[1], m.Subject, "other"); err != nil {
			t.Fatal(err)
		}
		target := listTarget{pds: m.Actors[0], session: newSession(Config{}, srv.Client(), nil), target: &xrpc.Client{Host: srv.URL, Client: srv.Client()}}
		scanErr := target.backfill(ctx, db)
		var got []string
		for {
			j, err := db.next(ctx)
			if errors.Is(err, sql.ErrNoRows) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, j.DID)
			if err := db.finish(ctx, j); err != nil {
				t.Fatal(err)
			}
		}
		return got, scanErr
	})
}
