package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
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
	if err := database.Write.QueryRow("SELECT COUNT(*) FROM atlas_schema_revisions").Scan(&revisions); err != nil {
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
	if err := database.Write.QueryRow("PRAGMA foreign_keys").Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if enabled != 1 {
		t.Fatalf("foreign_keys = %d, want 1", enabled)
	}
}

// SQLite reports synchronous as an integer: 2 is FULL, its default, and 1 is
// NORMAL. Asserting the value the connection actually reports catches both a
// DSN the driver silently ignores and a connection that fell back to the
// default. synchronous is set per connection, not persisted to the database
// file, so this must be checked on both the Write and Read pools to confirm
// the DSN reaches every connection.
func TestOpenUsesNormalSynchronousAndWAL(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(string) (*DB, error)
	}{
		{name: "Open", open: Open},
		{name: "OpenCache", open: OpenCache},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database, err := tc.open(filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatalf("open() = %v", err)
			}
			t.Cleanup(func() { _ = database.Close() })

			for _, pool := range []struct {
				name string
				db   *sql.DB
			}{
				{"Write", database.Write},
				{"Read", database.Read},
			} {
				var got int
				if err := pool.db.QueryRow("PRAGMA synchronous").Scan(&got); err != nil {
					t.Fatalf("%s: PRAGMA synchronous = %v", pool.name, err)
				}
				if got != 1 {
					t.Fatalf("%s: PRAGMA synchronous = %d, want 1 (NORMAL)", pool.name, got)
				}

				var journal string
				if err := pool.db.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil {
					t.Fatalf("%s: PRAGMA journal_mode = %v", pool.name, err)
				}
				if journal != "wal" {
					t.Fatalf("%s: PRAGMA journal_mode = %q, want %q", pool.name, journal, "wal")
				}
			}
		})
	}
}

// TestReadsDoNotBlockBehindHeldWriteTransaction guards the core motivation for
// splitting Read from Write: a reader on its own connection must be able to
// query while a write transaction is open (even before it commits), instead
// of waiting behind it in a shared connection pool.
func TestReadsDoNotBlockBehindHeldWriteTransaction(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()

	if _, err := database.Write.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	tx, err := database.Write.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	// Deliberately left uncommitted: the read below must not need the write
	// transaction to finish.

	readDone := make(chan error, 1)
	go func() {
		var count int
		readDone <- database.Read.QueryRow("SELECT COUNT(*) FROM t").Scan(&count)
	}()

	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("read while write tx open: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read blocked behind an open, uncommitted write transaction")
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// TestConcurrentWriteTransactionsSerializeWithoutBusyErrors guards the other
// half of the split: many goroutines opening write transactions at once must
// queue for the single write connection and each succeed in turn, rather than
// racing each other into SQLITE_BUSY.
func TestConcurrentWriteTransactionsSerializeWithoutBusyErrors(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()

	if _, err := database.Write.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	const writers = 16
	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx, err := database.Write.BeginTx(context.Background(), nil)
			if err != nil {
				errs[i] = fmt.Errorf("begin: %w", err)
				return
			}
			if _, err := tx.Exec("INSERT INTO t (id) VALUES (?)", i); err != nil {
				errs[i] = fmt.Errorf("insert: %w", err)
				_ = tx.Rollback()
				return
			}
			if err := tx.Commit(); err != nil {
				errs[i] = fmt.Errorf("commit: %w", err)
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}

	var count int
	if err := database.Write.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != writers {
		t.Fatalf("count = %d, want %d", count, writers)
	}
}

// TestUnixExclVFSLeavesNoSHMFile guards the core motivation for unix-excl:
// the WAL index must live in process heap memory, not the -shm file, so no
// further fcntl locking or cache-invalidating reads happen against the
// NFS-mounted database file after it is opened.
func TestUnixExclVFSLeavesNoSHMFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix-excl VFS is not used on windows")
	}

	path := filepath.Join(t.TempDir(), "test.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()

	if _, err := database.Write.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Write.Exec("INSERT INTO t (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := database.Read.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(path + "-shm"); err == nil {
		t.Fatalf("%s-shm exists, want no -shm file under unix-excl VFS", path)
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

// TestSqliteDSNSetsVFSForFileDatabasesOnly guards that unix-excl is applied
// to file-backed DSNs (and to both the write and read pool, since mixing VFS
// choices for the same database file within a process corrupts the WAL
// index) but never to an in-memory database, which has no file to lock.
func TestSqliteDSNSetsVFSForFileDatabasesOnly(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix-excl VFS is not used on windows")
	}

	for _, tc := range []struct {
		name    string
		path    string
		txlock  string
		wantVFS bool
	}{
		{name: "file write pool", path: filepath.Join(t.TempDir(), "test.db"), txlock: "immediate", wantVFS: true},
		{name: "file read pool", path: filepath.Join(t.TempDir(), "test.db"), txlock: "", wantVFS: true},
		{name: "in-memory", path: ":memory:", txlock: "", wantVFS: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn, err := sqliteDSN(tc.path, tc.txlock)
			if err != nil {
				t.Fatal(err)
			}
			hasVFS := strings.Contains(dsn, "vfs=unix-excl")
			if hasVFS != tc.wantVFS {
				t.Fatalf("dsn = %q, contains vfs=unix-excl = %v, want %v", dsn, hasVFS, tc.wantVFS)
			}
		})
	}
}
