package schedule

import (
	"context"
	"errors"

	"github.com/21S1298001/mahiron/internal/model"
)

// Source provides either live EIT updates or a remote server's stored programs.
// Construct it with LiveSource or StoredSource; the zero value is invalid.
type Source struct {
	collect func(context.Context, func(model.ScheduleUpdate) error, func(model.PresentFollowing) error) error
	list    func(context.Context, uint16, uint16) ([]model.Event, error)
}

func LiveSource(collect func(context.Context, func(model.ScheduleUpdate) error, func(model.PresentFollowing) error) error) Source {
	return Source{collect: collect}
}

func StoredSource(list func(context.Context, uint16, uint16) ([]model.Event, error)) Source {
	return Source{list: list}
}

func (s Source) IsStored() bool { return s.list != nil }

func (s Source) Collect(ctx context.Context, schedule func(model.ScheduleUpdate) error, pf func(model.PresentFollowing) error) error {
	if s.collect == nil {
		return errors.New("schedule source does not support live collection")
	}
	return s.collect(ctx, schedule, pf)
}

func (s Source) List(ctx context.Context, networkID, serviceID uint16) ([]model.Event, error) {
	if s.list == nil {
		return nil, errors.New("schedule source does not support stored programs")
	}
	return s.list(ctx, networkID, serviceID)
}
