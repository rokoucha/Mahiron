package db

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestOpenInMemoryAppliesAtlasMigrationsIdempotently(t *testing.T) {
	database, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if err := Migrate(context.Background(), database); err != nil {
		t.Fatalf("second migration: %v", err)
	}
	var revisions int
	if err := database.QueryRow("SELECT COUNT(*) FROM atlas_schema_revisions").Scan(&revisions); err != nil {
		t.Fatal(err)
	}
	if revisions != 6 {
		t.Fatalf("revision count = %d, want 6", revisions)
	}
}

func TestOpenEnablesForeignKeys(t *testing.T) {
	database, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	var enabled int
	if err := database.QueryRow("PRAGMA foreign_keys").Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if enabled != 1 {
		t.Fatalf("foreign_keys = %d, want 1", enabled)
	}
}

// SQLite reports synchronous as an integer: 2 is FULL, its default, and 1 is
// NORMAL. Asserting the value the connection actually reports catches both a
// DSN the driver silently ignores and the two constructors being wired to the
// same setting.
func TestOpenCacheRelaxesDurabilityAndOpenDoesNot(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(string) (*sql.DB, error)
		want int
	}{
		{name: "Open keeps FULL", open: Open, want: 2},
		{name: "OpenCache uses NORMAL", open: OpenCache, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database, err := tc.open(filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatalf("open() = %v", err)
			}
			t.Cleanup(func() { _ = database.Close() })

			var got int
			if err := database.QueryRow("PRAGMA synchronous").Scan(&got); err != nil {
				t.Fatalf("PRAGMA synchronous = %v", err)
			}
			if got != tc.want {
				t.Fatalf("PRAGMA synchronous = %d, want %d", got, tc.want)
			}

			// Both remain journalled in WAL mode; only the flush point moves.
			var journal string
			if err := database.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil {
				t.Fatalf("PRAGMA journal_mode = %v", err)
			}
			if journal != "wal" {
				t.Fatalf("PRAGMA journal_mode = %q, want %q", journal, "wal")
			}
		})
	}
}
