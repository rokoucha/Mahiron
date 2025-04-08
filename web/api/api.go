package api

import (
	"context"

	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

type Handler struct {
}

var _ apigen.Handler = (*Handler)(nil)

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) CheckVersion(ctx context.Context) (apigen.CheckVersionRes, error) {
	return CheckVersion(ctx, h)
}

func (h *Handler) GetApiDocumentation(ctx context.Context) (string, error) {
	panic("implement me")
}

func (h *Handler) GetChannel(ctx context.Context, params apigen.GetChannelParams) (apigen.GetChannelRes, error) {
	panic("implement me")
}

func (h *Handler) GetChannelStream(ctx context.Context, params apigen.GetChannelStreamParams) (apigen.GetChannelStreamRes, error) {
	panic("implement me")
}

func (h *Handler) GetChannels(ctx context.Context, params apigen.GetChannelsParams) (apigen.GetChannelsRes, error) {
	panic("implement me")
}

func (h *Handler) GetChannelsByType(ctx context.Context, params apigen.GetChannelsByTypeParams) (apigen.GetChannelsByTypeRes, error) {
	panic("implement me")
}

func (h *Handler) GetEvents(ctx context.Context) (apigen.GetEventsRes, error) {
	panic("implement me")
}

func (h *Handler) GetEventsStream(ctx context.Context, params apigen.GetEventsStreamParams) (apigen.GetEventsStreamRes, error) {
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
	panic("implement me")
}

func (h *Handler) GetServiceByChannel(ctx context.Context, params apigen.GetServiceByChannelParams) (apigen.GetServiceByChannelRes, error) {
	panic("implement me")
}

func (h *Handler) GetServicePrograms(ctx context.Context, params apigen.GetServiceProgramsParams) (apigen.GetServiceProgramsRes, error) {
	panic("implement me")
}

func (h *Handler) GetServiceStream(ctx context.Context, params apigen.GetServiceStreamParams) (apigen.GetServiceStreamRes, error) {
	panic("implement me")
}

func (h *Handler) GetServiceStreamByChannel(ctx context.Context, params apigen.GetServiceStreamByChannelParams) (apigen.GetServiceStreamByChannelRes, error) {
	panic("implement me")
}

func (h *Handler) GetServices(ctx context.Context, params apigen.GetServicesParams) (apigen.GetServicesRes, error) {
	panic("implement me")
}

func (h *Handler) GetServicesByChannel(ctx context.Context, params apigen.GetServicesByChannelParams) (apigen.GetServicesByChannelRes, error) {
	panic("implement me")
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

func (h *Handler) KillTunerProcess(ctx context.Context, params apigen.KillTunerProcessParams) (apigen.KillTunerProcessRes, error) {
	panic("implement me")
}
