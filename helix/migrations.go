package helix

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"

	"github.com/alyraffauf/terrarium/helix/storage"
)

func migrate(db *sql.DB) error {
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, storage.Migrations)
	if err != nil {
		return err
	}
	_, err = provider.Up(context.Background())
	return err
}
