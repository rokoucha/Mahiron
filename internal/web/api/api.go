package api

import (
	"context"
	"io"

	"github.com/21S1298001/mahiron/internal/bml"
	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/event"
	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/stream"
	"github.com/21S1298001/mahiron/internal/tuner"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

type Handler struct {
	serviceManager        ServiceManager
	programManager        ProgramManager
	streamManager         StreamManager
	tunerManager          TunerManager
	jobManager            JobManager
	logStore              LogStore
	eventHub              EventHub
	epgStaleAfter         int64
	dataBroadcastDisabled bool
	// bmlStore backs the BML API's retained-module and resource reads without
	// going through the stream manager. bmlSnapshotStore rebuilds provisional
	// snapshots when no live session exists. Both are nil when the
	// data-broadcast cache is disabled.
	bmlStore         bml.ModuleStore
	bmlSnapshotStore bml.SnapshotStore
}

var _ apigen.Handler = (*Handler)(nil)
var _ apigen.RawHandler = (*Handler)(nil)

type HandlerConfig struct {
	ServiceManager        ServiceManager
	ProgramManager        ProgramManager
	StreamManager         StreamManager
	TunerManager          TunerManager
	JobManager            JobManager
	LogStore              LogStore
	EventHub              EventHub
	EpgStaleAfter         int64
	DataBroadcastDisabled bool
	BMLStore              bml.ModuleStore
	BMLSnapshotStore      bml.SnapshotStore
}

type ServiceManager interface {
	EPGSummary(context.Context, int64, int64) (int, int, *int64, error)
	GetChannel(string, string) *config.ChannelConfig
	GetChannels() config.ChannelsConfig
	GetServiceByChannelAndId(context.Context, string, string, string) (*service.Service, error)
	GetServiceById(context.Context, string) (*service.Service, error)
	GetServiceByItemID(context.Context, int64) (*service.Service, error)
	GetLogoByServiceItemID(context.Context, int64) ([]byte, error)
	GetServices(context.Context) ([]*service.Service, error)
	GetServicesByChannel(context.Context, string, string) ([]*service.Service, error)
	GetServicesGroupedByChannel(context.Context) (map[service.ChannelKey][]*service.Service, error)
}

type ProgramManager interface {
	Count(context.Context) (int, error)
	Get(context.Context, int64) (*program.Program, bool, error)
	List(context.Context, program.Query) ([]*program.Program, error)
	ListFunc(context.Context, program.Query, func(*program.Program) error) error
}

type StreamManager interface {
	GetOrCreate(context.Context, string, string) (stream.Session, error)
	GetExisting(string, string) (stream.Session, bool)
	ActiveSessionCount() int
}

type TunerManager interface {
	KillProcess(context.Context, int) error
	Status(int) (tuner.Status, bool)
	Statuses() []tuner.Status
}

type JobManager interface {
	Abort(string) error
	GetActiveJobKeysByPrefix(string) []string
	GetJobSchedules() []job.ScheduleInfo
	GetJobs() []*job.Job
	Rerun(string) error
	RunSchedule(string) error
}

type LogStore interface {
	Snapshot() []byte
	Subscribe() (io.ReadCloser, func())
}

type EventHub interface {
	Log() []event.Event
	Subscribe() (<-chan event.Event, func())
}

func NewHandler(config HandlerConfig) *Handler {
	return &Handler{
		serviceManager:        config.ServiceManager,
		programManager:        config.ProgramManager,
		streamManager:         config.StreamManager,
		tunerManager:          config.TunerManager,
		jobManager:            config.JobManager,
		logStore:              config.LogStore,
		eventHub:              config.EventHub,
		epgStaleAfter:         config.EpgStaleAfter,
		dataBroadcastDisabled: config.DataBroadcastDisabled,
		bmlStore:              config.BMLStore,
		bmlSnapshotStore:      config.BMLSnapshotStore,
	}
}
