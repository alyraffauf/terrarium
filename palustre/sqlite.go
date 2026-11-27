package palustre

import (
	"context"
	"database/sql"

	_ "modernc.org/sqlite"
)

// OpenSQLite configures the single writer used by the stateful daemons.
func OpenSQLite(ctx context.Context, path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;"); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
