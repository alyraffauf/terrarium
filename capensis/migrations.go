package capensis

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pressly/goose/v3"

	"github.com/alyraffauf/terrarium/capensis/storage"
)

func migrate(db *sql.DB) error {
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, storage.Migrations, goose.WithGoMigrations(goose.NewGoMigration(1, &goose.GoFunc{RunTx: func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, storage.LegacySchema); err != nil {
			return err
		}
		return migrateLegacySchema(tx)
	}}, nil)))
	if err != nil {
		return err
	}
	_, err = provider.Up(context.Background())
	return err
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
	rows, err = tx.Query(`SELECT pds FROM targets`)
	if err != nil {
		return err
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return err
		}
		keys = append(keys, key)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, old := range keys {
		canonical, err := normalizePDS(old)
		if err != nil {
			return err
		}
		if old == canonical {
			continue
		}
		if _, err := tx.Exec(`UPDATE targets SET pds=? WHERE pds=?`, canonical, old); err != nil {
			return fmt.Errorf("conflicting canonical PDS bindings: %w", err)
		}
		if _, err := tx.Exec(`UPDATE list_members SET pds=? WHERE pds=?`, canonical, old); err != nil {
			return err
		}
	}
	return nil
}
