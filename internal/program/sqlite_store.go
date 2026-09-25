package program

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/db/gen"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/observability"
)

type sqliteStore struct {
	write *sql.DB
	read  *sql.DB
	wq    *gen.Queries
	rq    *gen.Queries
}

const upsertProgramSQL = `INSERT INTO programs (id, event_id, service_id, network_id, stream_id, start_at, duration, is_free,
                      name, description, event)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  event_id=excluded.event_id,
  service_id=excluded.service_id,
  network_id=excluded.network_id,
  stream_id=excluded.stream_id,
  start_at=excluded.start_at,
  duration=excluded.duration,
  is_free=excluded.is_free,
  name=excluded.name,
  description=excluded.description,
  event=excluded.event`

// listProgramsSelectSQL is written out here rather than declared in
// queries/programs.sql because sqlc only generates row-returning queries that
// collect every row into a slice first. /api/programs covers the whole EPG —
// tens of thousands of programs — so the rows are scanned and handed on one at
// a time instead.
const listProgramsSelectSQL = `SELECT id, event_id, service_id, network_id, start_at, duration, is_free,
       name, description, stream_id, event
FROM programs`

// buildListProgramsSQL only emits WHERE clauses for the filters that are
// actually set. The previous `(?N IS NULL OR col = ?N)` form applied to every
// column regardless of whether it was queried, which kept SQLite from using
// idx_programs_service (network_id, service_id) and forced a full scan of the
// programs table on every /api/programs request.
func buildListProgramsSQL(query Query) (string, []any) {
	var conds []string
	var args []any
	if query.ID != nil {
		conds = append(conds, "id = ?")
		args = append(args, *query.ID)
	}
	if query.NetworkID != nil {
		conds = append(conds, "network_id = ?")
		args = append(args, int64(*query.NetworkID))
	}
	if query.ServiceID != nil {
		conds = append(conds, "service_id = ?")
		args = append(args, int64(*query.ServiceID))
	}
	if query.EventID != nil {
		conds = append(conds, "event_id = ?")
		args = append(args, int64(*query.EventID))
	}
	if query.StartAt != nil {
		conds = append(conds, "start_at + duration >= ?")
		args = append(args, *query.StartAt)
	}
	if query.EndAt != nil {
		conds = append(conds, "start_at <= ?")
		args = append(args, *query.EndAt)
	}

	sqlStr := listProgramsSelectSQL
	if len(conds) > 0 {
		sqlStr += "\nWHERE " + strings.Join(conds, "\n  AND ")
	}
	sqlStr += "\nORDER BY start_at, id"
	return sqlStr, args
}

func NewSQLiteStore(database *db.DB) Store {
	return &sqliteStore{
		write: database.Write,
		read:  database.Read,
		wq:    gen.New(database.Write),
		rq:    gen.New(database.Read),
	}
}

func (s *sqliteStore) UpsertAll(ctx context.Context, programs []*Program) (err error) {
	start := time.Now()
	ctx, span := observability.StartSpan(ctx, observability.SpanDBProgramUpsertAll,
		observability.AttrProgramCount.Int(len(programs)),
	)
	defer func() {
		observability.RecordDBOperation(ctx, observability.SpanDBProgramUpsertAll, time.Since(start).Milliseconds(), err)
		observability.EndSpan(span, err)
	}()

	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()

	if err := upsertPrograms(ctx, tx, programs); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *sqliteStore) Get(ctx context.Context, id int64) (*Program, bool, error) {
	row, err := s.rq.GetProgram(ctx, id)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	p, err := fromGenProgram(row)
	if err != nil {
		return nil, false, err
	}
	return p, true, nil
}

func (s *sqliteStore) List(ctx context.Context, query Query) ([]*Program, error) {
	var programs []*Program
	err := s.ListFunc(ctx, query, func(p *Program) error {
		programs = append(programs, p)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return programs, nil
}

func (s *sqliteStore) ListFunc(ctx context.Context, query Query, yield func(*Program) error) error {
	sqlStr, args := buildListProgramsSQL(query)
	rows, err := s.read.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var row gen.Program
		if err := rows.Scan(
			&row.ID,
			&row.EventID,
			&row.ServiceID,
			&row.NetworkID,
			&row.StartAt,
			&row.Duration,
			&row.IsFree,
			&row.Name,
			&row.Description,
			&row.StreamID,
			&row.Event,
		); err != nil {
			return err
		}
		p, err := fromGenProgram(row)
		if err != nil {
			return err
		}
		if err := yield(p); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return rows.Close()
}

func (s *sqliteStore) ListByIDs(ctx context.Context, ids []int64) ([]*Program, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.rq.ListProgramsByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	return fromGenPrograms(rows)
}

func (s *sqliteStore) ListByServiceFrom(ctx context.Context, networkID, serviceID uint16, from int64) ([]*Program, error) {
	rows, err := s.rq.ListProgramsByServiceFrom(ctx, gen.ListProgramsByServiceFromParams{
		NetworkID: int64(networkID),
		ServiceID: int64(serviceID),
		StartAt:   from,
	})
	if err != nil {
		return nil, err
	}
	return fromGenPrograms(rows)
}

func (s *sqliteStore) ListEndedIDsBefore(ctx context.Context, cutoff int64) ([]int64, error) {
	return s.rq.ListEndedProgramIDsBefore(ctx, cutoff)
}

func (s *sqliteStore) DeleteEndedBefore(ctx context.Context, cutoff int64) (err error) {
	start := time.Now()
	ctx, span := observability.StartSpan(ctx, observability.SpanDBProgramDeleteEndedBefore,
		observability.AttrProgramCutoff.Int64(cutoff),
	)
	defer func() {
		observability.RecordDBOperation(ctx, observability.SpanDBProgramDeleteEndedBefore, time.Since(start).Milliseconds(), err)
		observability.EndSpan(span, err)
	}()

	return s.wq.DeleteEndedAtBefore(ctx, cutoff)
}

func (s *sqliteStore) ReplaceServicePrograms(ctx context.Context, networkID, serviceID uint16, from int64, programs []*Program) (err error) {
	start := time.Now()
	ctx, span := observability.StartSpan(ctx, observability.SpanDBProgramReplaceServicePrograms,
		observability.AttrEPGNetworkID.Int(int(networkID)),
		observability.AttrEPGServiceID.Int(int(serviceID)),
		observability.AttrProgramFrom.Int64(from),
		observability.AttrProgramCount.Int(len(programs)),
	)
	defer func() {
		observability.RecordDBOperation(ctx, observability.SpanDBProgramReplaceServicePrograms, time.Since(start).Milliseconds(), err)
		observability.EndSpan(span, err)
	}()

	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin replace: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()
	q := s.wq.WithTx(tx)
	if err := q.DeleteProgramsByServiceFrom(ctx, gen.DeleteProgramsByServiceFromParams{
		NetworkID: int64(networkID),
		ServiceID: int64(serviceID),
		StartAt:   from,
	}); err != nil {
		return fmt.Errorf("delete service snapshot: %w", err)
	}
	if err := upsertPrograms(ctx, tx, programs); err != nil {
		return err
	}
	return tx.Commit()
}

func upsertPrograms(ctx context.Context, tx *sql.Tx, programs []*Program) (err error) {
	stmt, err := tx.PrepareContext(ctx, upsertProgramSQL)
	if err != nil {
		return fmt.Errorf("prepare upsert program: %w", err)
	}
	defer func() {
		err = errors.Join(err, stmt.Close())
	}()
	for _, p := range programs {
		params, err := toUpsertProgramParams(p)
		if err != nil {
			return err
		}
		if err := execUpsertProgram(ctx, stmt, params); err != nil {
			return fmt.Errorf("upsert program %d: %w", p.ID, err)
		}
	}
	return nil
}

func execUpsertProgram(ctx context.Context, stmt *sql.Stmt, arg gen.UpsertProgramParams) error {
	_, err := stmt.ExecContext(ctx,
		arg.ID,
		arg.EventID,
		arg.ServiceID,
		arg.NetworkID,
		arg.StreamID,
		arg.StartAt,
		arg.Duration,
		arg.IsFree,
		arg.Name,
		arg.Description,
		arg.Event,
	)
	return err
}

func (s *sqliteStore) Count(ctx context.Context) (int, error) {
	n, err := s.rq.CountPrograms(ctx)
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

func fromGenPrograms(rows []gen.Program) ([]*Program, error) {
	result := make([]*Program, 0, len(rows))
	for i := range rows {
		p, err := fromGenProgram(rows[i])
		if err != nil {
			return nil, err
		}
		result = append(result, p)
	}
	return result, nil
}

// toUpsertProgramParams stores an undecided start time or duration as 0,
// which no broadcast uses: the columns stay NOT NULL for the time-range
// queries.
func toUpsertProgramParams(p *Program) (gen.UpsertProgramParams, error) {
	var name, desc *string
	if p.Name != "" {
		v := p.Name
		name = &v
	}
	if p.Description != "" {
		v := p.Description
		desc = &v
	}
	var event *string
	if stored := toStoredEvent(&p.Event); !reflect.ValueOf(stored).IsZero() {
		data, err := json.Marshal(stored)
		if err != nil {
			return gen.UpsertProgramParams{}, fmt.Errorf("marshal program %d event: %w", p.ID, err)
		}
		v := string(data)
		event = &v
	}
	isFree := int64(1)
	if p.FreeCA {
		isFree = 0
	}
	return gen.UpsertProgramParams{
		ID:          p.ID,
		EventID:     int64(p.EventID),
		ServiceID:   int64(p.Key.ServiceID),
		NetworkID:   int64(p.Key.NetworkID),
		StreamID:    int64(p.Key.StreamID),
		StartAt:     p.StartAtOrZero(),
		Duration:    int64(p.DurationOrZero()),
		IsFree:      isFree,
		Name:        name,
		Description: desc,
		Event:       event,
	}, nil
}

func fromGenProgram(p gen.Program) (*Program, error) {
	prog := &Program{
		ID: p.ID,
		Event: model.Event{
			Key:     model.ServiceKey{NetworkID: uint16(p.NetworkID), StreamID: uint16(p.StreamID), ServiceID: uint16(p.ServiceID)},
			EventID: uint16(p.EventID),
			FreeCA:  p.IsFree == 0,
		},
	}
	if p.StartAt != 0 {
		v := p.StartAt
		prog.StartAt = &v
	}
	if p.Duration != 0 {
		v := int(p.Duration)
		prog.DurationMS = &v
	}
	if p.Name != nil {
		prog.Name = *p.Name
	}
	if p.Description != nil {
		prog.Description = *p.Description
	}
	if p.Event != nil {
		var stored storedEvent
		if err := json.Unmarshal([]byte(*p.Event), &stored); err != nil {
			return nil, fmt.Errorf("decode program %d event: %w", p.ID, err)
		}
		stored.applyTo(&prog.Event)
	}
	return prog, nil
}

// sameProgram reports whether two programs store the same row, comparing
// what the store writes rather than the Go values, whose nil and empty
// slices differ between decoded rows and incoming events.
func sameProgram(a, b *Program) bool {
	if a == nil || b == nil {
		return a == b
	}
	pa, errA := toUpsertProgramParams(a)
	pb, errB := toUpsertProgramParams(b)
	return errA == nil && errB == nil && reflect.DeepEqual(pa, pb)
}
