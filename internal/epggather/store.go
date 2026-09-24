package epggather

import (
	"context"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/program"
)

// EventWriter stores the events decoded from EIT.
type EventWriter interface {
	UpsertEvents(context.Context, []model.Event) error
}

// ProgramStore keeps the stored programs: syncing from a remote replaces a
// service's programs, and cleanup removes the ended ones.
type ProgramStore interface {
	DeleteEndedBefore(context.Context, int64) error
	ReplaceServicePrograms(context.Context, uint16, uint16, int64, []*program.Program) error
}
