package epggather

import (
	"context"

	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/ts"
)

type ProgramWriter interface {
	UpsertPrograms(context.Context, []*program.Program) error
}

type ProgramStore interface {
	ProgramWriter
	DeleteEndedBefore(context.Context, int64) error
	ReplaceServicePrograms(context.Context, uint16, uint16, int64, []*program.Program) error
}

type Updater struct {
	store ProgramWriter
}

func NewUpdater(store ProgramWriter) *Updater {
	return &Updater{store: store}
}

func (u *Updater) UpsertEITSection(ctx context.Context, section *EITSection) error {
	return u.store.UpsertPrograms(ctx, section.Programs())
}

func (u *Updater) UpsertEIT(ctx context.Context, eit *ts.EIT) error {
	return u.UpsertEITSection(ctx, EITSectionFromTS(eit))
}
