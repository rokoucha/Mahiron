package program

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/21S1298001/Mahiron5/db/gen"
)

type sqliteStore struct {
	db *sql.DB
	q  *gen.Queries
}

func NewSQLiteStore(db *sql.DB) ProgramStore {
	return &sqliteStore{
		db: db,
		q:  gen.New(db),
	}
}

func (s *sqliteStore) UpsertAll(ctx context.Context, programs []*Program) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	q := s.q.WithTx(tx)
	for _, p := range programs {
		params, err := toUpsertProgramParams(p)
		if err != nil {
			return err
		}
		if err := q.UpsertProgram(ctx, params); err != nil {
			return fmt.Errorf("upsert program %d: %w", p.ID, err)
		}
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

func (s *sqliteStore) DeleteEndedBefore(ctx context.Context, cutoff int64) error {
	return s.q.DeleteEndedAtBefore(ctx, cutoff)
}

func (s *sqliteStore) ReplaceServicePrograms(ctx context.Context, networkID, serviceID uint16, from int64, programs []*Program) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin replace: %w", err)
	}
	defer tx.Rollback()
	q := s.q.WithTx(tx)
	if err := q.DeleteProgramsByServiceFrom(ctx, gen.DeleteProgramsByServiceFromParams{
		NetworkID: int64(networkID),
		ServiceID: int64(serviceID),
		StartAt:   from,
	}); err != nil {
		return fmt.Errorf("delete service snapshot: %w", err)
	}
	for _, p := range programs {
		params, err := toUpsertProgramParams(p)
		if err != nil {
			return err
		}
		if err := q.UpsertProgram(ctx, params); err != nil {
			return fmt.Errorf("upsert program %d: %w", p.ID, err)
		}
	}
	return tx.Commit()
}

func (s *sqliteStore) Count(ctx context.Context) (int, error) {
	n, err := s.q.CountPrograms(ctx)
	if err != nil {
		return 0, err
	}
	return int(n), nil
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
