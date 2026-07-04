package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	_ "modernc.org/sqlite"
)

func Open(path string) (*sql.DB, error) {
	dsn, err := sqliteDSN(path)
	if err != nil {
		return nil, fmt.Errorf("build database DSN: %w", err)
	}
	database, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)

	pragmas := []string{}
	if !isInMemory(path) {
		pragmas = append(pragmas, "PRAGMA journal_mode = WAL")
	}
	for _, pragma := range pragmas {
		if _, err := database.Exec(pragma); err != nil {
			return nil, errors.Join(fmt.Errorf("%s: %w", pragma, err), database.Close())
		}
	}

	return database, nil
}

func sqliteDSN(path string) (string, error) {
	if path == ":memory:" {
		return "file::memory:?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", nil
	}
	u, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "foreign_keys(1)")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func OpenInMemory() (*sql.DB, error) {
	database, err := Open(":memory:")
	if err != nil {
		return nil, err
	}
	if err := Migrate(context.Background(), database); err != nil {
		return nil, errors.Join(err, database.Close())
	}
	return database, nil
}

func isInMemory(path string) bool {
	return path == ":memory:" || strings.Contains(path, "mode=memory")
}

func Migrate(ctx context.Context, database *sql.DB) error {
	mg := NewMigrator(database)
	return mg.Apply(ctx)
}
