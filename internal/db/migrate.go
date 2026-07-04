package db

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"ariga.io/atlas/sql/migrate"
	"ariga.io/atlas/sql/sqlite"
)

//go:embed migrations/*.sql migrations/atlas.sum
var migrationsFS embed.FS

type Migrator struct {
	db *sql.DB
}

func NewMigrator(db *sql.DB) *Migrator {
	return &Migrator{db: db}
}

func (m *Migrator) Apply(ctx context.Context) (err error) {
	dir := migrate.OpenMemDir(fmt.Sprintf("mahiron-%p", m))
	defer func() {
		err = errors.Join(err, dir.Close())
	}()
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		content, err := migrationsFS.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}
		if err := dir.WriteFile(entry.Name(), content); err != nil {
			return fmt.Errorf("load migration %s: %w", entry.Name(), err)
		}
	}
	drv, err := sqlite.Open(m.db)
	if err != nil {
		return fmt.Errorf("open Atlas SQLite driver: %w", err)
	}
	revisions := &revisionStore{db: m.db}
	if err := revisions.init(ctx); err != nil {
		return err
	}
	executor, err := migrate.NewExecutor(drv, dir, revisions)
	if err != nil {
		return fmt.Errorf("create Atlas migration executor: %w", err)
	}
	if err := executor.ExecuteN(ctx, 0); err != nil && !errors.Is(err, migrate.ErrNoPendingFiles) {
		return fmt.Errorf("apply Atlas migrations: %w", err)
	}
	return nil
}

type revisionStore struct {
	db *sql.DB
}

func (s *revisionStore) Ident() *migrate.TableIdent {
	return &migrate.TableIdent{Name: "atlas_schema_revisions"}
}

func (s *revisionStore) init(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS atlas_schema_revisions (
		version TEXT PRIMARY KEY,
		description TEXT NOT NULL,
		type INTEGER NOT NULL,
		applied INTEGER NOT NULL,
		total INTEGER NOT NULL,
		executed_at INTEGER NOT NULL,
		execution_time INTEGER NOT NULL,
		error TEXT NOT NULL,
		error_stmt TEXT NOT NULL,
		hash TEXT NOT NULL,
		partial_hashes TEXT NOT NULL,
		operator_version TEXT NOT NULL
	)`)
	if err != nil {
		return fmt.Errorf("create Atlas revision table: %w", err)
	}
	return nil
}

func (s *revisionStore) ReadRevisions(ctx context.Context) (_ []*migrate.Revision, err error) {
	rows, err := s.db.QueryContext(ctx, revisionSelect+" ORDER BY version")
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()
	var revisions []*migrate.Revision
	for rows.Next() {
		revision, err := scanRevision(rows)
		if err != nil {
			return nil, err
		}
		revisions = append(revisions, revision)
	}
	return revisions, rows.Err()
}

func (s *revisionStore) ReadRevision(ctx context.Context, version string) (*migrate.Revision, error) {
	revision, err := scanRevision(s.db.QueryRowContext(ctx, revisionSelect+" WHERE version = ?", version))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, migrate.ErrRevisionNotExist
	}
	return revision, err
}

func (s *revisionStore) WriteRevision(ctx context.Context, r *migrate.Revision) error {
	partialHashes, err := json.Marshal(r.PartialHashes)
	if err != nil {
		return fmt.Errorf("marshal partial hashes: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO atlas_schema_revisions
		(version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(version) DO UPDATE SET description=excluded.description, type=excluded.type,
		applied=excluded.applied, total=excluded.total, executed_at=excluded.executed_at,
		execution_time=excluded.execution_time, error=excluded.error, error_stmt=excluded.error_stmt,
		hash=excluded.hash, partial_hashes=excluded.partial_hashes, operator_version=excluded.operator_version`,
		r.Version, r.Description, int64(r.Type), r.Applied, r.Total, r.ExecutedAt.UnixNano(),
		int64(r.ExecutionTime), r.Error, r.ErrorStmt, r.Hash, string(partialHashes), r.OperatorVersion)
	return err
}

func (s *revisionStore) DeleteRevision(ctx context.Context, version string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM atlas_schema_revisions WHERE version = ?", version)
	return err
}

const revisionSelect = `SELECT version, description, type, applied, total, executed_at,
	execution_time, error, error_stmt, hash, partial_hashes, operator_version FROM atlas_schema_revisions`

func scanRevision(row interface{ Scan(...any) error }) (*migrate.Revision, error) {
	var (
		r             migrate.Revision
		typ           int64
		executedAt    int64
		executionTime int64
		partialHashes string
	)
	if err := row.Scan(&r.Version, &r.Description, &typ, &r.Applied, &r.Total, &executedAt,
		&executionTime, &r.Error, &r.ErrorStmt, &r.Hash, &partialHashes, &r.OperatorVersion); err != nil {
		return nil, err
	}
	r.Type = migrate.RevisionType(typ)
	r.ExecutedAt = time.Unix(0, executedAt)
	r.ExecutionTime = time.Duration(executionTime)
	if err := json.Unmarshal([]byte(partialHashes), &r.PartialHashes); err != nil {
		return nil, fmt.Errorf("decode revision %s partial hashes: %w", r.Version, err)
	}
	return &r, nil
}
