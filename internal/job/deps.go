package job

import (
	"context"
	"time"

	"github.com/21S1298001/mahiron/internal/epg"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/servicescan"
	"github.com/21S1298001/mahiron/ts"
)

// This package keeps job orchestration thin. Feature-specific details should
// live behind usecase packages such as internal/epg.

type Registry interface {
	Register(JobDefinition)
	EnqueueDefinition(JobDefinition) (string, error)
}

type ServiceScanner interface {
	Channels() []servicescan.Channel
	ScanChannel(context.Context, string, string, bool) ([]uint16, error)
}

type LogoCollector interface {
	ObserveLogos(context.Context, string, string, func(*ts.LogoImage) error) error
}

type LogoStore interface {
	MissingLogoTargets(context.Context) ([]service.LogoTarget, error)
	UpsertLogoImage(context.Context, *ts.LogoImage) error
}

type LogoGatherTargetStore interface {
	LogoGatherTargets(context.Context) ([]service.LogoTarget, error)
}

type EPGGatherer interface {
	Groups(context.Context) (map[uint16]*epg.Network, error)
	BuildNetworkInputs(context.Context, uint16) ([]epg.Candidate, []epg.ServiceKey, error)
	GatherNetwork(context.Context, uint16, []epg.Candidate, []epg.ServiceKey) error
	Cleanup(context.Context, time.Time) error
}

type EPGServiceStore interface {
	GetServices(context.Context) ([]*service.Service, error)
	SetEPGAttempt(context.Context, uint16, uint16, int64, string) error
	SetEPGSuccess(context.Context, uint16, uint16, int64) error
}

type EPGProgramStore interface {
	UpsertPrograms(context.Context, []*program.Program) error
	DeleteEndedBefore(context.Context, int64) error
	ReplaceServicePrograms(context.Context, uint16, uint16, int64, []*program.Program) error
}

type EPGStreamManager interface {
	HasSession(string, string) bool
	GetOrCreateWait(context.Context, string, string) (interface {
		CollectEIT(context.Context, func(*ts.EIT) error) error
	}, error)
}
