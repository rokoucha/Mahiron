package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"runtime"
	"strings"

	_ "modernc.org/sqlite"
)

// File-backed databases are opened with SQLite's `unix-excl` VFS because on
// NFS, the default VFS's per-transaction fcntl lock is an RPC to the NFS
// server that also makes the Linux NFS client drop that file's page cache,
// and under concurrent readers/writers this turned a couple of Lock RPCs/sec
// into dozens, stalling /api/* requests for tens of seconds. unix-excl keeps
// the WAL index in process-heap memory instead of the -shm file, which is
// why every *sql.DB opened against the same database file in this process
// must use the same VFS — otherwise one connection reads a WAL index the
// other never wrote to, corrupting reads.
//
// DB splits reads and writes across separate connection pools so that a
// blocked writer cannot starve API reads.
//
// SQLite allows only one writer at a time; every write transaction that
// cannot immediately acquire the write lock sleeps for up to busy_timeout
// (5s) before giving up. When writes and reads shared one *sql.DB with
// several pooled connections, a burst of concurrent writers (remote program
// event upserts, EPG sync, EIT merges, ...) could occupy every pooled
// connection while asleep waiting on SQLITE_BUSY. Reads then had to wait in
// database/sql's connection-acquisition queue behind those sleeping writers,
// which is what turned a single slow commit (NFS commits average ~2s, p90
// ~10s) into tens of seconds of blocked /api/* requests.
//
// Write holds exactly one connection (SetMaxOpenConns(1)) and begins every
// transaction with BEGIN IMMEDIATE (via the `_txlock=immediate` DSN option),
// so writers queue up and acquire the write lock one at a time instead of
// racing each other into SQLITE_BUSY sleeps. Read holds a small pool of
// connections dedicated to read-only queries, which WAL lets proceed
// concurrently with whatever the single writer is doing. In-memory databases
// (tests) keep a single shared connection for both, since separate
// connections would each see an independent, empty database.
type DB struct {
	Write *sql.DB
	Read  *sql.DB
}

// Close closes both connection pools. It is safe to call on a DB returned for
// an in-memory database, where Write and Read are the same connection.
func (d *DB) Close() error {
	if d.Write == d.Read {
		return d.Write.Close()
	}
	return errors.Join(d.Write.Close(), d.Read.Close())
}

// Open opens the main database. In WAL mode, synchronous=NORMAL cannot
// corrupt the database on a power cut — it only risks losing the most recent
// commits, which is the guarantee NORMAL keeps and OFF does not. Everything
// the main database holds (services, programs, logos, EPG fetch state) can be
// reacquired from the broadcast stream or a remote source, so that risk is
// acceptable and buys back the fsync-per-commit cost that dominates writes on
// our NFS-backed storage.
//
// OpenCache exists only for call-site compatibility with code that
// distinguishes "cache" databases from the main one; both now run identical
// pragmas.
func Open(path string) (*DB, error) {
	return open(path)
}

func OpenCache(path string) (*DB, error) {
	return open(path)
}

func open(path string) (*DB, error) {
	if isInMemory(path) {
		database, err := openPool(path, "", 1)
		if err != nil {
			return nil, err
		}
		return &DB{Write: database, Read: database}, nil
	}

	writeDB, err := openPool(path, "immediate", 1)
	if err != nil {
		return nil, err
	}

	// journal_mode is persisted to the database file, so setting it once on
	// the write connection is enough for every future connection, including
	// the read pool opened below.
	if _, err := writeDB.Exec("PRAGMA journal_mode = WAL"); err != nil {
		return nil, errors.Join(fmt.Errorf("PRAGMA journal_mode = WAL: %w", err), writeDB.Close())
	}

	readDB, err := openPool(path, "", 8)
	if err != nil {
		return nil, errors.Join(err, writeDB.Close())
	}

	return &DB{Write: writeDB, Read: readDB}, nil
}

func openPool(path string, txlock string, maxConnections int) (*sql.DB, error) {
	dsn, err := sqliteDSN(path, txlock)
	if err != nil {
		return nil, fmt.Errorf("build database DSN: %w", err)
	}
	database, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	database.SetMaxOpenConns(maxConnections)
	database.SetMaxIdleConns(maxConnections)
	return database, nil
}

// sqliteDSN carries the pragmas in the DSN so every connection applies them.
// Unlike journal_mode, synchronous is per connection and is not recorded in the
// database file, so setting it once after opening would not survive a
// connection being replaced.
//
// txlock, when non-empty, sets modernc.org/sqlite's `_txlock` DSN option
// (deferred/immediate/exclusive), which controls how BEGIN starts every
// transaction opened on that connection.
func sqliteDSN(path string, txlock string) (string, error) {
	if path == ":memory:" {
		dsn := "file::memory:?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
		if txlock != "" {
			dsn += "&_txlock=" + txlock
		}
		return dsn, nil
	}
	u, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "synchronous(normal)")
	if vfs := VFSName(path); vfs != "" {
		q.Set("vfs", vfs)
	}
	if txlock != "" {
		q.Set("_txlock", txlock)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// VFSName returns the SQLite VFS name applied to file-backed databases
// opened for path, or "" when no explicit VFS is applied (in-memory
// databases, and Windows, which has no unix-excl VFS). See the package
// comment for why unix-excl is used.
func VFSName(path string) string {
	if isInMemory(path) || runtime.GOOS == "windows" {
		return ""
	}
	return "unix-excl"
}

func OpenInMemory() (*DB, error) {
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

func Migrate(ctx context.Context, database *DB) error {
	mg := NewMigrator(database.Write)
	return mg.Apply(ctx)
}
