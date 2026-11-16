package storage

import (
	"embed"
	"io/fs"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

var Migrations, _ = fs.Sub(migrationFiles, "migrations")
