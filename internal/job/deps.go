package job

import (
	"context"
	"time"

	"github.com/21S1298001/Mahiron5/internal/epg"
	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/internal/service"
	"github.com/21S1298001/Mahiron5/internal/servicescan"
	"github.com/21S1298001/Mahiron5/ts"
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
	CollectLogos(context.Context, string, string, func(*ts.LogoImage) error) error
}

type LogoUpdater interface {
	UpsertLogoImage(context.Context, *ts.LogoImage) error
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
	GetOrCreate(context.Context, string, string) (interface {
		CollectEITS(context.Context, func(*ts.EIT) error) error
		CollectEITPF(context.Context, func(*ts.EIT) error) error
	}, error)
}
