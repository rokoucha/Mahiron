package db

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestCheckpointWALRunsWithoutErrorOnFileDatabase(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "checkpoint.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if err := Migrate(context.Background(), database); err != nil {
		t.Fatal(err)
	}

	if _, err := database.Write.Exec("INSERT INTO metadata (key, value) VALUES ('k', 'v')"); err != nil {
		t.Fatal(err)
	}

	if err := checkpointWAL(context.Background(), database.Write); err != nil {
		t.Fatalf("checkpointWAL: %v", err)
	}
}

func TestStartWALCheckpointerRunsPeriodicallyOnFileDatabase(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "checkpoint.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if err := Migrate(context.Background(), database); err != nil {
		t.Fatal(err)
	}

	stop := StartWALCheckpointer(context.Background(), database, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	stop()
}

func TestStartWALCheckpointerNoopsOnInMemoryDatabase(t *testing.T) {
	database, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()

	stop := StartWALCheckpointer(context.Background(), database, 10*time.Millisecond)
	stop()
}
