package api

import (
	"context"

	"github.com/21S1298001/Mahiron5/service"
	"github.com/21S1298001/Mahiron5/stream"
	"github.com/21S1298001/Mahiron5/tuner"
	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

type Handler struct {
	serviceManager *service.ServiceManager
	streamManager  *stream.StreamManager
	tunerManager   *tuner.TunerManager
}

var _ apigen.Handler = (*Handler)(nil)

type HandlerConfig struct {
	ServiceManager *service.ServiceManager
	StreamManager  *stream.StreamManager
	TunerManager   *tuner.TunerManager
}

func NewHandler(config HandlerConfig) *Handler {
	return &Handler{
		serviceManager: config.ServiceManager,
		streamManager:  config.StreamManager,
		tunerManager:   config.TunerManager,
	}
}

func (h *Handler) AbortJob(ctx context.Context, params apigen.AbortJobParams) (apigen.AbortJobRes, error) {
	panic("implement me")
}

func (h *Handler) ChannelsTypeChannelServicesIDStreamHead(ctx context.Context, params apigen.ChannelsTypeChannelServicesIDStreamHeadParams) (apigen.ChannelsTypeChannelServicesIDStreamHeadRes, error) {
	panic("implement me")
}

func (h *Handler) ChannelsTypeChannelStreamHead(ctx context.Context, params apigen.ChannelsTypeChannelStreamHeadParams) (apigen.ChannelsTypeChannelStreamHeadRes, error) {
	panic("implement me")
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
	panic("implement me")
}

func (h *Handler) GetEventsStream(ctx context.Context, params apigen.GetEventsStreamParams) (apigen.GetEventsStreamRes, error) {
	panic("implement me")
}

func (h *Handler) GetJobSchedules(ctx context.Context) (apigen.GetJobSchedulesRes, error) {
	panic("implement me")
}

func (h *Handler) GetJobs(ctx context.Context) (apigen.GetJobsRes, error) {
	panic("implement me")
}

func (h *Handler) GetLog(ctx context.Context) (apigen.GetLogRes, error) {
	panic("implement me")
}

func (h *Handler) GetLogStream(ctx context.Context) (apigen.GetLogStreamRes, error) {
	panic("implement me")
}

func (h *Handler) GetLogoImage(ctx context.Context, params apigen.GetLogoImageParams) (apigen.GetLogoImageRes, error) {
	panic("implement me")
}

func (h *Handler) GetProgram(ctx context.Context, params apigen.GetProgramParams) (apigen.GetProgramRes, error) {
	panic("implement me")
}

func (h *Handler) GetProgramStream(ctx context.Context, params apigen.GetProgramStreamParams) (apigen.GetProgramStreamRes, error) {
	panic("implement me")
}

func (h *Handler) GetPrograms(ctx context.Context, params apigen.GetProgramsParams) (apigen.GetProgramsRes, error) {
	panic("implement me")
}

func (h *Handler) GetService(ctx context.Context, params apigen.GetServiceParams) (apigen.GetServiceRes, error) {
	return GetService(ctx, h, params)
}

func (h *Handler) GetServiceByChannel(ctx context.Context, params apigen.GetServiceByChannelParams) (apigen.GetServiceByChannelRes, error) {
	return GetServiceByChannel(ctx, h, params)
}

func (h *Handler) GetServicePrograms(ctx context.Context, params apigen.GetServiceProgramsParams) (apigen.GetServiceProgramsRes, error) {
	panic("implement me")
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
	panic("implement me")
}

func (h *Handler) GetTuner(ctx context.Context, params apigen.GetTunerParams) (apigen.GetTunerRes, error) {
	panic("implement me")
}

func (h *Handler) GetTunerProcess(ctx context.Context, params apigen.GetTunerProcessParams) (apigen.GetTunerProcessRes, error) {
	panic("implement me")
}

func (h *Handler) GetTuners(ctx context.Context) (apigen.GetTunersRes, error) {
	panic("implement me")
}

func (h *Handler) IptvDiscoverJSONGet(ctx context.Context) (apigen.IptvDiscoverJSONGetRes, error) {
	panic("implement me")
}

func (h *Handler) IptvLineupJSONGet(ctx context.Context) (apigen.IptvLineupJSONGetRes, error) {
	panic("implement me")
}

func (h *Handler) IptvLineupStatusJSONGet(ctx context.Context) (apigen.IptvLineupStatusJSONGetRes, error) {
	panic("implement me")
}

func (h *Handler) IptvPlaylistGet(ctx context.Context) (apigen.IptvPlaylistGetRes, error) {
	panic("implement me")
}

func (h *Handler) IptvXmltvGet(ctx context.Context) (apigen.IptvXmltvGetRes, error) {
	panic("implement me")
}

func (h *Handler) KillTunerProcess(ctx context.Context, params apigen.KillTunerProcessParams) (apigen.KillTunerProcessRes, error) {
	panic("implement me")
}

func (h *Handler) ProgramsIDStreamHead(ctx context.Context, params apigen.ProgramsIDStreamHeadParams) (apigen.ProgramsIDStreamHeadRes, error) {
	panic("implement me")
}

func (h *Handler) RerunJob(ctx context.Context, params apigen.RerunJobParams) (apigen.RerunJobRes, error) {
	panic("implement me")
}

func (h *Handler) RunJobSchedule(ctx context.Context, params apigen.RunJobScheduleParams) (apigen.RunJobScheduleRes, error) {
	panic("implement me")
}

func (h *Handler) ServicesIDStreamHead(ctx context.Context, params apigen.ServicesIDStreamHeadParams) (apigen.ServicesIDStreamHeadRes, error) {
	panic("implement me")
}
