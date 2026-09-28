// Package defs contains the concrete job definitions wired into the generic
// job manager. Feature-specific details should live behind usecase packages
// such as internal/epggather; this package only adapts them to job definitions.
package defs

import (
	"context"
	"time"

	"github.com/21S1298001/mahiron/internal/epggather"
	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/logogather"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/servicescan"
)

type Registry interface {
	Register(job.JobDefinition)
	EnqueueDefinition(job.JobDefinition) (string, error)
}

type ServiceScanner interface {
	Channels() []servicescan.Channel
	ScanChannel(context.Context, string, string, bool) ([]uint16, error)
}

type LogoGatherer interface {
	Targets(context.Context) ([]logogather.Target, error)
	GatherChannel(ctx context.Context, channelType, channelID string, targets []logogather.Target) error
}

type EPGGatherer interface {
	Groups(context.Context) (map[uint16]*epggather.Network, error)
	BuildNetworkInputs(context.Context, uint16) ([]epggather.Candidate, []model.ServiceKey, error)
	GatherNetwork(context.Context, uint16, []epggather.Candidate, []model.ServiceKey) error
}

// ProgramCleaner deletes the programs past the retention period.
type ProgramCleaner interface {
	DeleteExpired(ctx context.Context, now time.Time, retentionDays int) error
}
