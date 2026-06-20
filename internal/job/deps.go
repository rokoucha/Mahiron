package job

import (
	"context"
	"io"

	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/internal/service"
)

// This package still depends on config/service/program/tuner/processor domain
// types. Keep the current behavior stable for now; the next decoupling step is
// to replace those cross-package details with job-local DTOs and injectable
// retry policies.

type Registry interface {
	Register(JobDefinition)
	EnqueueDefinition(JobDefinition) (string, error)
}

type EPGServiceStore interface {
	GetServices(context.Context) ([]*service.Service, error)
	SetEPGAttempt(context.Context, uint16, uint16, int64, string) error
	SetEPGSuccess(context.Context, uint16, uint16, int64) error
}

type ServiceScanner interface {
	EPGServiceStore
	ScanServicesWait(context.Context, service.StreamScanner, string, string) ([]uint16, error)
}

type EPGProgramStore interface {
	UpsertEITSection(context.Context, *program.EITSection) error
	UpsertPrograms(context.Context, []*program.Program) error
	DeleteEndedBefore(context.Context, int64) error
}

type EPGStreamManager interface {
	HasSession(string, string) bool
	GetOrCreate(context.Context, string, string) (interface {
		CollectEITS(context.Context, io.Writer) error
		CollectEITPF(context.Context, io.Writer) error
	}, error)
}
