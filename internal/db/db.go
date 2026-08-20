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

// Open opens a database whose contents cannot be rebuilt, and so commits
// durably: SQLite's default synchronous=FULL flushes the write-ahead log on
// every commit.
func Open(path string) (*sql.DB, error) {
	return open(path, false)
}

// OpenCache opens a database holding only data that can be reacquired from the
// broadcast stream. Such a database trades durability for commit cost by
// running at synchronous=NORMAL, which flushes the write-ahead log at
// checkpoints rather than on every commit. Losing recently committed rows to a
// power cut costs a re-download; it cannot corrupt the database, which is the
// guarantee NORMAL keeps and OFF does not.
func OpenCache(path string) (*sql.DB, error) {
	return open(path, true)
}

func open(path string, cache bool) (*sql.DB, error) {
	dsn, err := sqliteDSN(path, cache)
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

// sqliteDSN carries the pragmas in the DSN so every connection applies them.
// Unlike journal_mode, synchronous is per connection and is not recorded in the
// database file, so setting it once after opening would not survive a
// connection being replaced.
func sqliteDSN(path string, cache bool) (string, error) {
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
	if cache {
		q.Add("_pragma", "synchronous(normal)")
	}
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
