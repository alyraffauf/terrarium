package capensis

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pressly/goose/v3"

	"github.com/alyraffauf/terrarium/capensis/storage"
	"github.com/alyraffauf/terrarium/palustre"
)

func migrate(db *sql.DB) error {
	return palustre.MigrateSQLite(context.Background(), db, storage.Migrations,
		goose.NewGoMigration(1, &goose.GoFunc{RunTx: func(ctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, storage.LegacySchema); err != nil {
				return err
			}
			return migrateLegacySchema(tx)
		}}, nil),
		// Version 2 is the typed-state SQL migration. Databases that
		// already applied it never re-run version 1, so canonicalizing
		// saved PDS keys needs its own upgrade.
		goose.NewGoMigration(3, &goose.GoFunc{RunTx: func(ctx context.Context, tx *sql.Tx) error {
			return canonicalizeTargetKeys(tx)
		}}, nil),
		goose.NewGoMigration(4, &goose.GoFunc{RunTx: func(ctx context.Context, tx *sql.Tx) error {
			return canonicalizeStreamEndpoint(tx)
		}}, nil),
	)
}
func migrateMembers(tx *sql.Tx) error {
	var legacy bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='members')`).Scan(&legacy); err != nil {
		return err
	}
	if !legacy {
		return nil
	}
	var pds, owner string
	err := tx.QueryRow(`SELECT value FROM metadata WHERE key='pds'`).Scan(&pds)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	err = tx.QueryRow(`SELECT value FROM metadata WHERE key='owner'`).Scan(&owner)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if pds != "" && owner != "" {
		uri := "at://" + owner + "/" + listCollection + "/" + recordKey(pds)
		if _, err := tx.Exec(`INSERT OR IGNORE INTO targets VALUES(?,?)`, pds, uri); err != nil {
			return err
		}
	}
	var count int
	if err := tx.QueryRow(`SELECT count(*) FROM members`).Scan(&count); err != nil {
		return err
	}
	if count > 0 && pds == "" {
		return fmt.Errorf("cannot migrate membership without its PDS binding")
	}
	if _, err := tx.Exec(`INSERT INTO list_members SELECT ?,did,rkey FROM members`, pds); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE members`); err != nil {
		return err
	}
	return nil
}

func migrateLegacySchema(tx *sql.Tx) error {
	rows, err := tx.Query(`PRAGMA table_info(pending)`)
	if err != nil {
		return err
	}
	cols := map[string]bool{}
	for rows.Next() {
		var id, notnull, pk int
		var name, typ string
		var def any
		if err := rows.Scan(&id, &name, &typ, &notnull, &def, &pk); err != nil {
			rows.Close()
			return err
		}
		cols[name] = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, name := range []string{"generation", "attempts", "ready"} {
		if !cols[name] {
			initial := 0
			if name == "generation" {
				initial = 1
			}
			if _, err := tx.Exec(fmt.Sprintf("ALTER TABLE pending ADD COLUMN %s INTEGER NOT NULL DEFAULT %d", name, initial)); err != nil {
				return err
			}
		}
	}
	if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS pending_order ON pending(ready,did)`); err != nil {
		return err
	}
	if err := migrateMembers(tx); err != nil {
		return err
	}
	return canonicalizeTargetKeys(tx)
}

// canonicalizeTargetKeys moves saved list bindings and membership from
// every spelling of a PDS to the spelling NormalizePDS now produces.
// The saved list URI is preserved verbatim: its record key was derived
// from the spelling in use when the list was created, and rebinding to
// a newly derived URI would create a duplicate list. Spellings that
// converge on one canonical PDS but carry different list URIs, or the
// same member record key for different DIDs, are a conflict no upgrade
// may resolve by choosing a winner, so the upgrade fails and leaves
// every row untouched.
func canonicalizeTargetKeys(tx *sql.Tx) error {
	rows, err := tx.Query(`SELECT pds,list_uri FROM targets ORDER BY pds`)
	if err != nil {
		return err
	}
	type binding struct{ old, uri string }
	groups := map[string][]binding{}
	var canonicals []string
	for rows.Next() {
		var old, uri string
		if err := rows.Scan(&old, &uri); err != nil {
			rows.Close()
			return err
		}
		canonical, err := normalizePDS(old)
		if err != nil {
			rows.Close()
			return err
		}
		if _, seen := groups[canonical]; !seen {
			canonicals = append(canonicals, canonical)
		}
		groups[canonical] = append(groups[canonical], binding{old, uri})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, canonical := range canonicals {
		group := groups[canonical]
		for _, other := range group[1:] {
			if other.uri != group[0].uri {
				return fmt.Errorf("conflicting canonical PDS bindings: %q and %q both normalize to %q with different lists %q and %q; resolve manually", group[0].old, other.old, canonical, group[0].uri, other.uri)
			}
		}
		keeper := false
		for _, b := range group {
			if b.old == canonical {
				keeper = true
			}
		}
		for _, b := range group {
			if b.old == canonical {
				continue
			}
			members, err := tx.Query(`SELECT did,rkey FROM list_members WHERE pds=? ORDER BY rkey`, b.old)
			if err != nil {
				return err
			}
			type member struct{ did, rkey string }
			var moved []member
			for members.Next() {
				var m member
				if err := members.Scan(&m.did, &m.rkey); err != nil {
					members.Close()
					return err
				}
				moved = append(moved, m)
			}
			if err := members.Err(); err != nil {
				members.Close()
				return err
			}
			members.Close()
			for _, m := range moved {
				var existing string
				err := tx.QueryRow(`SELECT did FROM list_members WHERE pds=? AND rkey=?`, canonical, m.rkey).Scan(&existing)
				if err != nil && err != sql.ErrNoRows {
					return err
				}
				if err == nil && existing != m.did {
					return fmt.Errorf("conflicting canonical PDS membership: record %q under %q belongs to both %q and %q; resolve manually", m.rkey, canonical, existing, m.did)
				}
				if err == sql.ErrNoRows {
					if _, err := tx.Exec(`INSERT INTO list_members(pds,did,rkey) VALUES(?,?,?)`, canonical, m.did, m.rkey); err != nil {
						return err
					}
				}
			}
			if _, err := tx.Exec(`DELETE FROM list_members WHERE pds=?`, b.old); err != nil {
				return err
			}
			if keeper {
				if _, err := tx.Exec(`DELETE FROM targets WHERE pds=?`, b.old); err != nil {
					return err
				}
			} else {
				if _, err := tx.Exec(`UPDATE targets SET pds=? WHERE pds=?`, canonical, b.old); err != nil {
					return err
				}
				keeper = true
			}
		}
	}
	return nil
}

// canonicalizeStreamEndpoint rewrites a checkpoint endpoint saved under
// a pre-canonicalization spelling. Unparseable values are left alone:
// resume falls back to the portable timestamp cursor for them.
func canonicalizeStreamEndpoint(tx *sql.Tx) error {
	var exists bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='sync_state')`).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	var endpoint string
	if err := tx.QueryRow(`SELECT stream_endpoint FROM sync_state WHERE id=1`).Scan(&endpoint); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	if endpoint == "" {
		return nil
	}
	canonical, err := palustre.NormalizeJetstream(endpoint)
	if err != nil {
		return nil
	}
	if canonical == endpoint {
		return nil
	}
	_, err = tx.Exec(`UPDATE sync_state SET stream_endpoint=? WHERE id=1`, canonical)
	return err
}
