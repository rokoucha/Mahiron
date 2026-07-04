package program

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/21S1298001/mahiron/internal/db/gen"
	"github.com/21S1298001/mahiron/internal/observability"
)

type sqliteStore struct {
	db *sql.DB
	q  *gen.Queries
}

const upsertProgramSQL = `INSERT INTO programs (id, event_id, service_id, network_id, start_at, duration, is_free,
                      name, description, genres, video, audios, extended, related_items, series)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  event_id=excluded.event_id,
  service_id=excluded.service_id,
  network_id=excluded.network_id,
  start_at=excluded.start_at,
  duration=excluded.duration,
  is_free=excluded.is_free,
  name=COALESCE(excluded.name, programs.name),
  description=COALESCE(excluded.description, programs.description),
  genres=COALESCE(excluded.genres, programs.genres),
  video=COALESCE(excluded.video, programs.video),
  audios=COALESCE(excluded.audios, programs.audios),
  extended=COALESCE(excluded.extended, programs.extended),
  related_items=COALESCE(excluded.related_items, programs.related_items),
  series=COALESCE(excluded.series, programs.series)`

func NewSQLiteStore(db *sql.DB) ProgramStore {
	return &sqliteStore{
		db: db,
		q:  gen.New(db),
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

	tx, err := s.db.BeginTx(ctx, nil)
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
	row, err := s.q.GetProgram(ctx, id)
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
	params := gen.ListProgramsParams{
		ID:        nilOrInt64(query.ID),
		NetworkID: nilOrInt64(query.NetworkID),
		ServiceID: nilOrInt64(query.ServiceID),
		EventID:   nilOrInt64(query.EventID),
	}
	rows, err := s.q.ListPrograms(ctx, params)
	if err != nil {
		return nil, err
	}
	return fromGenPrograms(rows)
}

func (s *sqliteStore) ListByIDs(ctx context.Context, ids []int64) ([]*Program, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.q.ListProgramsByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	return fromGenPrograms(rows)
}

func (s *sqliteStore) ListByServiceFrom(ctx context.Context, networkID, serviceID uint16, from int64) ([]*Program, error) {
	rows, err := s.q.ListProgramsByServiceFrom(ctx, gen.ListProgramsByServiceFromParams{
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
	return s.q.ListEndedProgramIDsBefore(ctx, cutoff)
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

	return s.q.DeleteEndedAtBefore(ctx, cutoff)
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

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin replace: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()
	q := s.q.WithTx(tx)
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
		arg.StartAt,
		arg.Duration,
		arg.IsFree,
		arg.Name,
		arg.Description,
		arg.Genres,
		arg.Video,
		arg.Audios,
		arg.Extended,
		arg.RelatedItems,
		arg.Series,
	)
	return err
}

func (s *sqliteStore) Count(ctx context.Context) (int, error) {
	n, err := s.q.CountPrograms(ctx)
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

func nilOrInt64[T ~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](p *T) interface{} {
	if p == nil {
		return nil
	}
	return int64(*p)
}

func encodeJSON(v any) (*string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || string(data) == "null" || string(data) == "[]" || string(data) == "{}" {
		return nil, nil
	}
	s := string(data)
	return &s, nil
}

func decodeJSON(s *string, dest any) error {
	if s == nil {
		return nil
	}
	return json.Unmarshal([]byte(*s), dest)
}

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

	genres, err := encodeJSON(p.Genres)
	if err != nil {
		return gen.UpsertProgramParams{}, fmt.Errorf("marshal program %d genres: %w", p.ID, err)
	}
	video, err := encodeJSON(p.Video)
	if err != nil {
		return gen.UpsertProgramParams{}, fmt.Errorf("marshal program %d video: %w", p.ID, err)
	}
	audios, err := encodeJSON(p.Audios)
	if err != nil {
		return gen.UpsertProgramParams{}, fmt.Errorf("marshal program %d audios: %w", p.ID, err)
	}
	extended, err := encodeJSON(p.Extended)
	if err != nil {
		return gen.UpsertProgramParams{}, fmt.Errorf("marshal program %d extended: %w", p.ID, err)
	}
	related, err := encodeJSON(p.RelatedItems)
	if err != nil {
		return gen.UpsertProgramParams{}, fmt.Errorf("marshal program %d related_items: %w", p.ID, err)
	}
	series, err := encodeJSON(p.Series)
	if err != nil {
		return gen.UpsertProgramParams{}, fmt.Errorf("marshal program %d series: %w", p.ID, err)
	}

	isFree := int64(0)
	if p.IsFree {
		isFree = 1
	}

	return gen.UpsertProgramParams{
		ID:           p.ID,
		EventID:      int64(p.EventID),
		ServiceID:    int64(p.ServiceID),
		NetworkID:    int64(p.NetworkID),
		StartAt:      p.StartAt,
		Duration:     int64(p.Duration),
		IsFree:       isFree,
		Name:         name,
		Description:  desc,
		Genres:       genres,
		Video:        video,
		Audios:       audios,
		Extended:     extended,
		RelatedItems: related,
		Series:       series,
	}, nil
}

func fromGenProgram(p gen.Program) (*Program, error) {
	prog := &Program{
		ID:        p.ID,
		EventID:   uint16(p.EventID),
		ServiceID: uint16(p.ServiceID),
		NetworkID: uint16(p.NetworkID),
		StartAt:   p.StartAt,
		Duration:  int(p.Duration),
		IsFree:    p.IsFree != 0,
	}
	if p.Name != nil {
		prog.Name = *p.Name
	}
	if p.Description != nil {
		prog.Description = *p.Description
	}
	if err := decodeJSON(p.Genres, &prog.Genres); err != nil {
		return nil, fmt.Errorf("decode program %d genres: %w", p.ID, err)
	}
	if err := decodeJSON(p.Video, &prog.Video); err != nil {
		return nil, fmt.Errorf("decode program %d video: %w", p.ID, err)
	}
	if err := decodeJSON(p.Audios, &prog.Audios); err != nil {
		return nil, fmt.Errorf("decode program %d audios: %w", p.ID, err)
	}
	if err := decodeJSON(p.Extended, &prog.Extended); err != nil {
		return nil, fmt.Errorf("decode program %d extended: %w", p.ID, err)
	}
	if err := decodeJSON(p.RelatedItems, &prog.RelatedItems); err != nil {
		return nil, fmt.Errorf("decode program %d related_items: %w", p.ID, err)
	}
	if err := decodeJSON(p.Series, &prog.Series); err != nil {
		return nil, fmt.Errorf("decode program %d series: %w", p.ID, err)
	}
	return prog, nil
}
