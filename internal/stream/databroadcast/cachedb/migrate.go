package cachedb

import (
	"context"
	"database/sql"
	"embed"

	mahirondb "github.com/21S1298001/mahiron/internal/db"
)

//go:embed migrations/*.sql migrations/atlas.sum
var migrations embed.FS

func Migrate(ctx context.Context, database *sql.DB) error {
	return mahirondb.ApplyMigrations(ctx, database, migrations, "migrations")
}
