package program

import "context"

type ProgramStore interface {
	UpsertAll(ctx context.Context, programs []*Program) error
	Get(ctx context.Context, id int64) (*Program, bool, error)
	List(ctx context.Context, query Query) ([]*Program, error)
	// ListFunc calls yield once per matching program, in the same order as
	// List, without materialising the whole result set.
	ListFunc(ctx context.Context, query Query, yield func(*Program) error) error
	ListByIDs(ctx context.Context, ids []int64) ([]*Program, error)
	ListByServiceFrom(ctx context.Context, networkID, serviceID uint16, from int64) ([]*Program, error)
	ListEndedIDsBefore(ctx context.Context, cutoff int64) ([]int64, error)
	DeleteEndedBefore(ctx context.Context, cutoff int64) error
	ReplaceServicePrograms(ctx context.Context, networkID, serviceID uint16, from int64, programs []*Program) error
	Count(ctx context.Context) (int, error)
}
