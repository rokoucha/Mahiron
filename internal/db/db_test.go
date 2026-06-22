package db

import (
	"context"
	"testing"
)

func TestOpenInMemoryAppliesAtlasMigrationsIdempotently(t *testing.T) {
	database, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := Migrate(context.Background(), database); err != nil {
		t.Fatalf("second migration: %v", err)
	}
	var revisions int
	if err := database.QueryRow("SELECT COUNT(*) FROM atlas_schema_revisions").Scan(&revisions); err != nil {
		t.Fatal(err)
	}
	if revisions != 3 {
		t.Fatalf("revision count = %d, want 3", revisions)
	}
}

func TestOpenEnablesForeignKeys(t *testing.T) {
	database, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var enabled int
	if err := database.QueryRow("PRAGMA foreign_keys").Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if enabled != 1 {
		t.Fatalf("foreign_keys = %d, want 1", enabled)
	}
}
