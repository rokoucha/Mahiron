package api

import (
	"context"
	"errors"
	"io"
	"net/http"

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
	serviceManager ServiceManager
	programManager ProgramManager
	streamManager  StreamManager
	tunerManager   TunerManager
	jobManager     JobManager
	logStore       LogStore
	eventHub       EventHub
	epgStaleAfter  int64
}

var _ apigen.Handler = (*Handler)(nil)
var _ apigen.RawHandler = (*Handler)(nil)

type HandlerConfig struct {
	ServiceManager ServiceManager
	ProgramManager ProgramManager
	StreamManager  StreamManager
	TunerManager   TunerManager
	JobManager     JobManager
	LogStore       LogStore
	EventHub       EventHub
	EpgStaleAfter  int64
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
}

type ProgramManager interface {
	Count(context.Context) (int, error)
	Get(context.Context, int64) (*program.Program, bool, error)
	List(context.Context, program.Query) ([]*program.Program, error)
}

type StreamManager interface {
	GetOrCreate(context.Context, string, string) (interface {
		ChannelStream(context.Context, bool, io.Writer) error
		ProgramStream(context.Context, *program.Program, bool, io.Writer) error
		ServiceStream(context.Context, uint16, bool, io.Writer) error
		ObserveDataBroadcast(context.Context, uint16, bool, func(stream.DataBroadcastEvent) error) error
		DataBroadcastModule(uint16, byte, uint16) (stream.DataBroadcastModule, bool)
	}, error)
	GetExisting(string, string) (interface {
		ChannelStream(context.Context, bool, io.Writer) error
		ProgramStream(context.Context, *program.Program, bool, io.Writer) error
		ServiceStream(context.Context, uint16, bool, io.Writer) error
		ObserveDataBroadcast(context.Context, uint16, bool, func(stream.DataBroadcastEvent) error) error
		DataBroadcastModule(uint16, byte, uint16) (stream.DataBroadcastModule, bool)
	}, bool)
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
		serviceManager: config.ServiceManager,
		programManager: config.ProgramManager,
		streamManager:  config.StreamManager,
		tunerManager:   config.TunerManager,
		jobManager:     config.JobManager,
		logStore:       config.LogStore,
		eventHub:       config.EventHub,
		epgStaleAfter:  config.EpgStaleAfter,
	}
}

func (h *Handler) AbortJob(ctx context.Context, params apigen.AbortJobParams) (apigen.AbortJobRes, error) {
	return AbortJob(ctx, h, params)
}

func (h *Handler) ChannelsTypeChannelServicesIDStreamHead(ctx context.Context, params apigen.ChannelsTypeChannelServicesIDStreamHeadParams) (apigen.ChannelsTypeChannelServicesIDStreamHeadRes, error) {
	return ChannelsTypeChannelServicesIDStreamHead(ctx, h, params)
}

func (h *Handler) ChannelsTypeChannelStreamHead(ctx context.Context, params apigen.ChannelsTypeChannelStreamHeadParams) (apigen.ChannelsTypeChannelStreamHeadRes, error) {
	return ChannelsTypeChannelStreamHead(ctx, h, params)
}

func (h *Handler) CheckVersion(ctx context.Context) (apigen.CheckVersionRes, error) {
	return CheckVersion(ctx, h)
}

func (h *Handler) GetApiDocumentation(ctx context.Context) (apigen.GetApiDocumentationRes, error) {
	return GetApiDocumentation(ctx, h)
}

func (h *Handler) GetChannel(ctx context.Context, params apigen.GetChannelParams) (apigen.GetChannelRes, error) {
	return GetChannel(ctx, h, params)
}

func (h *Handler) GetChannelStream(ctx context.Context, params apigen.GetChannelStreamParams) (apigen.GetChannelStreamRes, error) {
	return GetChannelStream(ctx, h, params)
}

func (h *Handler) GetChannels(ctx context.Context, params apigen.GetChannelsParams) (apigen.GetChannelsRes, error) {
	return GetChannels(ctx, h, params)
}

func (h *Handler) GetChannelsByType(ctx context.Context, params apigen.GetChannelsByTypeParams) (apigen.GetChannelsByTypeRes, error) {
	return GetChannelsByType(ctx, h, params)
}

func (h *Handler) GetEvents(ctx context.Context) (apigen.GetEventsRes, error) {
	return GetEvents(ctx, h)
}

func (h *Handler) GetEventsStream(ctx context.Context, params apigen.GetEventsStreamParams, w http.ResponseWriter) error {
	return GetEventsStream(ctx, h, params, w)
}

func (h *Handler) GetJobSchedules(ctx context.Context) (apigen.GetJobSchedulesRes, error) {
	return GetJobSchedules(ctx, h)
}

func (h *Handler) GetJobs(ctx context.Context) (apigen.GetJobsRes, error) {
	return GetJobs(ctx, h)
}

func (h *Handler) GetLog(ctx context.Context) (apigen.GetLogRes, error) {
	return GetLog(ctx, h)
}

func (h *Handler) GetLogStream(ctx context.Context) (apigen.GetLogStreamRes, error) {
	return GetLogStream(ctx, h)
}

func (h *Handler) GetLogoImage(ctx context.Context, params apigen.GetLogoImageParams) (apigen.GetLogoImageRes, error) {
	return GetLogoImage(ctx, h, params)
}

func (h *Handler) GetProgram(ctx context.Context, params apigen.GetProgramParams) (apigen.GetProgramRes, error) {
	return GetProgram(ctx, h, params)
}

func (h *Handler) GetProgramStream(ctx context.Context, params apigen.GetProgramStreamParams) (apigen.GetProgramStreamRes, error) {
	return GetProgramStream(ctx, h, params)
}

func (h *Handler) GetPrograms(ctx context.Context, params apigen.GetProgramsParams) (apigen.GetProgramsRes, error) {
	return GetPrograms(ctx, h, params)
}

func (h *Handler) GetService(ctx context.Context, params apigen.GetServiceParams) (apigen.GetServiceRes, error) {
	return GetService(ctx, h, params)
}

func (h *Handler) GetServiceByChannel(ctx context.Context, params apigen.GetServiceByChannelParams) (apigen.GetServiceByChannelRes, error) {
	return GetServiceByChannel(ctx, h, params)
}

func (h *Handler) GetServiceDataBroadcastEvents(ctx context.Context, params apigen.GetServiceDataBroadcastEventsParams, w http.ResponseWriter) error {
	return GetServiceDataBroadcastEvents(ctx, h, params, w)
}

func (h *Handler) GetServiceDataBroadcastModule(ctx context.Context, params apigen.GetServiceDataBroadcastModuleParams, w http.ResponseWriter) error {
	return GetServiceDataBroadcastModule(ctx, h, params, w)
}

func (h *Handler) GetServicePrograms(ctx context.Context, params apigen.GetServiceProgramsParams) (apigen.GetServiceProgramsRes, error) {
	return GetServicePrograms(ctx, h, params)
}

func (h *Handler) GetServiceStream(ctx context.Context, params apigen.GetServiceStreamParams) (apigen.GetServiceStreamRes, error) {
	return GetServiceStream(ctx, h, params)
}

func (h *Handler) GetServiceStreamByChannel(ctx context.Context, params apigen.GetServiceStreamByChannelParams) (apigen.GetServiceStreamByChannelRes, error) {
	return GetServiceStreamByChannel(ctx, h, params)
}

func (h *Handler) GetServices(ctx context.Context, params apigen.GetServicesParams) (apigen.GetServicesRes, error) {
	return GetServices(ctx, h, params)
}

func (h *Handler) GetServicesByChannel(ctx context.Context, params apigen.GetServicesByChannelParams) (apigen.GetServicesByChannelRes, error) {
	return GetServicesByChannel(ctx, h, params)
}

func (h *Handler) GetStatus(ctx context.Context) (apigen.GetStatusRes, error) {
	return GetStatus(ctx, h)
}

func (h *Handler) GetTuner(ctx context.Context, params apigen.GetTunerParams) (apigen.GetTunerRes, error) {
	return GetTuner(ctx, h, params)
}

func (h *Handler) GetTunerProcess(ctx context.Context, params apigen.GetTunerProcessParams) (apigen.GetTunerProcessRes, error) {
	return GetTunerProcess(ctx, h, params)
}

func (h *Handler) GetTuners(ctx context.Context) (apigen.GetTunersRes, error) {
	return GetTuners(ctx, h)
}

func (h *Handler) IptvDiscoverJSONGet(ctx context.Context) (apigen.IptvDiscoverJSONGetRes, error) {
	return IptvDiscoverJSONGet(ctx, h)
}

func (h *Handler) IptvLineupJSONGet(ctx context.Context) (apigen.IptvLineupJSONGetRes, error) {
	return IptvLineupJSONGet(ctx, h)
}

func (h *Handler) IptvLineupStatusJSONGet(ctx context.Context) (apigen.IptvLineupStatusJSONGetRes, error) {
	return IptvLineupStatusJSONGet(ctx, h)
}

func (h *Handler) IptvPlaylistGet(ctx context.Context) (apigen.IptvPlaylistGetRes, error) {
	return IptvPlaylistGet(ctx, h)
}

func (h *Handler) IptvXmltvGet(ctx context.Context) (apigen.IptvXmltvGetRes, error) {
	return IptvXmltvGet(ctx, h)
}

func (h *Handler) KillTunerProcess(ctx context.Context, params apigen.KillTunerProcessParams) (apigen.KillTunerProcessRes, error) {
	if err := h.tunerManager.KillProcess(ctx, params.Index); err != nil {
		if errors.Is(err, tuner.ErrTunerNotFound) {
			return notFound("tuner not found"), nil
		}
		return nil, err
	}
	return &apigen.KillTunerProcessNoContent{}, nil
}

func (h *Handler) ProgramsIDStreamHead(ctx context.Context, params apigen.ProgramsIDStreamHeadParams) (apigen.ProgramsIDStreamHeadRes, error) {
	return ProgramsIDStreamHead(ctx, h, params)
}

func (h *Handler) RerunJob(ctx context.Context, params apigen.RerunJobParams) (apigen.RerunJobRes, error) {
	return RerunJob(ctx, h, params)
}

func (h *Handler) RunJobSchedule(ctx context.Context, params apigen.RunJobScheduleParams) (apigen.RunJobScheduleRes, error) {
	return RunJobSchedule(ctx, h, params)
}

func (h *Handler) ServicesIDStreamHead(ctx context.Context, params apigen.ServicesIDStreamHeadParams) (apigen.ServicesIDStreamHeadRes, error) {
	return ServicesIDStreamHead(ctx, h, params)
}
