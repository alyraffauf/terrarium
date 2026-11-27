package storage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/pressly/goose/v3"
)

// Migrate adopts pre-Goose volumes before installing the current schema.
func Migrate(ctx context.Context, db *sql.DB) error {
	return palustre.MigrateSQLite(ctx, db, Migrations,
		goose.NewGoMigration(1, &goose.GoFunc{RunTx: stageLegacy}, nil),
		goose.NewGoMigration(3, &goose.GoFunc{RunTx: restoreLegacy}, nil),
	)
}

func stageLegacy(ctx context.Context, tx *sql.Tx) error {
	var version int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	switch version {
	case 0:
		var existing bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='items' AND type='table')").Scan(&existing); err != nil || !existing {
			return err
		}
		_, err := tx.ExecContext(ctx, `ALTER TABLE items RENAME TO legacy_items;
ALTER TABLE published RENAME TO legacy_published;
ALTER TABLE pending RENAME TO legacy_pending;
ALTER TABLE retries RENAME TO legacy_retries;
ALTER TABLE descriptions RENAME TO legacy_descriptions;`)
		return err
	case 1:
		_, err := tx.ExecContext(ctx, "ALTER TABLE items RENAME TO legacy_cluster_items")
		return err
	case 2:
		return nil
	default:
		return fmt.Errorf("unknown Trifolium database version %d", version)
	}
}

func restoreLegacy(ctx context.Context, tx *sql.Tx) error {
	var single, multi bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='legacy_items'),
EXISTS(SELECT 1 FROM sqlite_schema WHERE name='legacy_cluster_items')`).Scan(&single, &multi); err != nil {
		return err
	}
	if single {
		_, err := tx.ExecContext(ctx, `INSERT INTO items SELECT 'default',did,rkey FROM legacy_items;
INSERT INTO pending SELECT 'default',did FROM legacy_pending;
INSERT INTO published SELECT 'default',did FROM legacy_published;
INSERT INTO retries SELECT 'default',did,due FROM legacy_retries;
INSERT INTO descriptions SELECT 'default',text FROM legacy_descriptions;
INSERT INTO cluster_targets SELECT 'default',target FROM watched;
INSERT INTO legacy_default_targets SELECT target FROM watched;
DROP TABLE legacy_items;
DROP TABLE legacy_pending;
DROP TABLE legacy_published;
DROP TABLE legacy_retries;
DROP TABLE legacy_descriptions;`)
		if err != nil {
			return err
		}
	}
	if multi {
		if _, err := tx.ExecContext(ctx, "INSERT INTO items SELECT cluster,did,rkey FROM legacy_cluster_items; DROP TABLE legacy_cluster_items"); err != nil {
			return err
		}
	}
	// Retain the schema marker understood by pre-Goose releases.
	_, err := tx.ExecContext(ctx, "PRAGMA user_version=2")
	return err
}
