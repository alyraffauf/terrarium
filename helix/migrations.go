package helix

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"

	"github.com/alyraffauf/terrarium/helix/storage"
	"github.com/alyraffauf/terrarium/palustre"
)

func migrate(db *sql.DB) error {
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, storage.Migrations, goose.WithGoMigrations(
		// Version 1 is the initial SQL migration. Typed databases never
		// re-run it, so canonicalizing saved endpoints needs its own
		// upgrade.
		goose.NewGoMigration(2, &goose.GoFunc{RunTx: func(ctx context.Context, tx *sql.Tx) error {
			return canonicalizeSavedEndpoints(tx)
		}}, nil),
	))
	if err != nil {
		return err
	}
	_, err = provider.Up(context.Background())
	return err
}

// canonicalizeSavedEndpoints rewrites the stream checkpoint endpoint and
// saved candidate PDS values under their canonical spellings.
// Unparseable values are left alone: stream resume falls back to the
// portable timestamp cursor for them, and a candidate host is only a
// dial target.
func canonicalizeSavedEndpoints(tx *sql.Tx) error {
	var endpoint string
	if err := tx.QueryRow(`SELECT stream_endpoint FROM sync_state WHERE id=1`).Scan(&endpoint); err != nil && err != sql.ErrNoRows {
		return err
	}
	if endpoint != "" {
		if canonical, err := palustre.NormalizeJetstream(endpoint); err == nil && canonical != endpoint {
			if _, err := tx.Exec(`UPDATE sync_state SET stream_endpoint=? WHERE id=1`, canonical); err != nil {
				return err
			}
		}
	}
	rows, err := tx.Query(`SELECT did,pds FROM candidates WHERE pds<>'' ORDER BY did`)
	if err != nil {
		return err
	}
	type candidate struct{ did, pds string }
	var saved []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.did, &c.pds); err != nil {
			rows.Close()
			return err
		}
		saved = append(saved, c)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, c := range saved {
		canonical, err := palustre.NormalizePDS(c.pds)
		if err != nil || canonical == c.pds {
			continue
		}
		if _, err := tx.Exec(`UPDATE candidates SET pds=? WHERE did=?`, canonical, c.did); err != nil {
			return err
		}
	}
	return nil
}
