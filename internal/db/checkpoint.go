package db

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

// DefaultWALCheckpointInterval is how often StartWALCheckpointer runs a
// checkpoint.
const DefaultWALCheckpointInterval = 10 * time.Minute

// StartWALCheckpointer runs PRAGMA wal_checkpoint(TRUNCATE) on the write
// connection at a fixed interval.
//
// The read pool holds several connections that are open almost continuously
// once API traffic is read-mostly, which prevents SQLite's automatic
// checkpoint (triggered after wal_autocheckpoint pages) from ever completing:
// a checkpoint needs no reader holding a snapshot older than the WAL frames
// being folded back into the database file. Running TRUNCATE periodically on
// the single write connection, which is idle between transactions, keeps the
// WAL from growing unbounded even under near-constant read load.
//
// It is a no-op for an in-memory database, where Write and Read share one
// connection and there is no WAL file to bound. Call the returned stop
// function to end the background loop; it blocks until the loop has exited.
func StartWALCheckpointer(ctx context.Context, database *DB, interval time.Duration) (stop func()) {
	if database == nil || database.Write == database.Read {
		return func() {}
	}
	if interval <= 0 {
		interval = DefaultWALCheckpointInterval
	}
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				if loopCtx.Err() != nil {
					return
				}
				if err := checkpointWAL(loopCtx, database.Write); err != nil {
					slog.Warn("wal checkpoint failed", "err", err)
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func checkpointWAL(ctx context.Context, write *sql.DB) error {
	_, err := write.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}
