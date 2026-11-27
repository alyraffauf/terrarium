package palustre

import (
	"context"
	"database/sql"
	"io/fs"

	"github.com/pressly/goose/v3"
)

// MigrateSQLite applies the daemon's embedded SQL and transactional Go migrations.
func MigrateSQLite(ctx context.Context, db *sql.DB, files fs.FS, migrations ...*goose.Migration) error {
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, files, goose.WithGoMigrations(migrations...))
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}
