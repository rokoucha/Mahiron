package api

import (
	"bytes"
	"context"
	"net/http"
	"strconv"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/service"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func GetChannels(ctx context.Context, h *Handler, params apigen.GetChannelsParams) (apigen.GetChannelsRes, error) {
	channels := h.serviceManager.GetChannels()
	items, err := apiChannels(ctx, h, filterChannels(channels, params.Type, params.Channel, params.Name))
	if err != nil {
		return nil, err
	}
	res := apigen.GetChannelsOKApplicationJSON(items)
	return &res, nil
}

func GetChannelsByType(ctx context.Context, h *Handler, params apigen.GetChannelsByTypeParams) (apigen.GetChannelsByTypeRes, error) {
	channels := h.serviceManager.GetChannels()
	filtered := make(config.ChannelsConfig, 0, len(channels))
	for _, channel := range filterChannels(channels, apigen.NewOptString(params.Type), params.Channel, params.Name) {
		filtered = append(filtered, channel)
	}
	items, err := apiChannels(ctx, h, filtered)
	if err != nil {
		return nil, err
	}
	res := apigen.GetChannelsByTypeOKApplicationJSON(items)
	return &res, nil
}

func GetChannel(ctx context.Context, h *Handler, params apigen.GetChannelParams) (apigen.GetChannelRes, error) {
	channel := h.serviceManager.GetChannel(params.Type, params.Channel)
	if channel == nil {
		return notFound("channel not found"), nil
	}
	return apiChannelWithServices(ctx, h, *channel)
}

func GetServices(ctx context.Context, h *Handler, params apigen.GetServicesParams) (apigen.GetServicesRes, error) {
	services, err := h.serviceManager.GetServices(ctx)
	if err != nil {
		return nil, err
	}
	filtered := filterServices(services, params)
	res := apigen.GetServicesOKApplicationJSON(apiServices(h, filtered, true))
	return &res, nil
}

func GetService(ctx context.Context, h *Handler, params apigen.GetServiceParams) (apigen.GetServiceRes, error) {
	service, err := h.serviceManager.GetServiceById(ctx, strconv.FormatInt(params.ID, 10))
	if err != nil {
		return nil, err
	}
	if service == nil {
		return notFound("service not found"), nil
	}
	return apiService(h, service, true), nil
}

func GetServicesByChannel(ctx context.Context, h *Handler, params apigen.GetServicesByChannelParams) (apigen.GetServicesByChannelRes, error) {
	if h.serviceManager.GetChannel(params.Type, params.Channel) == nil {
		return notFound("channel not found"), nil
	}
	services, err := h.serviceManager.GetServicesByChannel(ctx, params.Type, params.Channel)
	if err != nil {
		return nil, err
	}
	res := apigen.GetServicesByChannelOKApplicationJSON(apiServices(h, services, true))
	return &res, nil
}

func GetServiceByChannel(ctx context.Context, h *Handler, params apigen.GetServiceByChannelParams) (apigen.GetServiceByChannelRes, error) {
	if h.serviceManager.GetChannel(params.Type, params.Channel) == nil {
		return notFound("channel not found"), nil
	}
	svc, err := h.serviceManager.GetServiceByChannelAndId(ctx, params.Type, params.Channel, strconv.FormatInt(params.ID, 10))
	if err != nil {
		return nil, err
	}
	if svc == nil {
		return notFound("service not found"), nil
	}
	return apiService(h, svc, true), nil
}

func GetLogoImage(ctx context.Context, h *Handler, params apigen.GetLogoImageParams) (apigen.GetLogoImageRes, error) {
	svc, err := h.serviceManager.GetServiceByItemID(ctx, params.ID)
	if err != nil {
		return nil, err
	}
	if svc == nil {
		return &apigen.GetLogoImageNotFound{}, nil
	}
	data, err := h.serviceManager.GetLogoByServiceItemID(ctx, params.ID)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return &apigen.GetLogoImageServiceUnavailable{}, nil
	}
	return &apigen.GetLogoImageOK{Data: bytes.NewReader(data)}, nil
}

func apiChannels(ctx context.Context, h *Handler, channels config.ChannelsConfig) ([]apigen.Channel, error) {
	result := make([]apigen.Channel, len(channels))
	for i, channel := range channels {
		item, err := apiChannelWithServices(ctx, h, channel)
		if err != nil {
			return nil, err
		}
		result[i] = *item
	}
	return result, nil
}

func filterChannels(channels config.ChannelsConfig, channelType apigen.OptString, channelId apigen.OptString, name apigen.OptString) config.ChannelsConfig {
	filtered := make(config.ChannelsConfig, 0, len(channels))
	for _, channel := range channels {
		if value, ok := channelType.Get(); ok && channel.Type != value {
			continue
		}
		if value, ok := channelId.Get(); ok && channel.Channel != value {
			continue
		}
		if value, ok := name.Get(); ok && channel.Name != value {
			continue
		}
		filtered = append(filtered, channel)
	}
	return filtered
}

func filterServices(services []*service.Service, params apigen.GetServicesParams) []*service.Service {
	filtered := make([]*service.Service, 0, len(services))
	for _, service := range services {
		if value, ok := params.ServiceId.Get(); ok && int(service.ServiceId) != value {
			continue
		}
		if value, ok := params.NetworkId.Get(); ok && int(service.NetworkId) != value {
			continue
		}
		if value, ok := params.Name.Get(); ok && service.Name != value {
			continue
		}
		if value, ok := params.Type.Get(); ok && int(service.Type) != value {
			continue
		}
		if value, ok := params.ChannelType.Get(); ok && service.ChannelType != value {
			continue
		}
		if value, ok := params.ChannelChannel.Get(); ok && service.ChannelId != value {
			continue
		}
		filtered = append(filtered, service)
	}
	return filtered
}

func apiChannelWithServices(ctx context.Context, h *Handler, channel config.ChannelConfig) (*apigen.Channel, error) {
	result := apiChannelWithoutServices(h, channel)
	services, err := h.serviceManager.GetServicesByChannel(ctx, channel.Type, channel.Channel)
	if err != nil {
		return nil, err
	}
	result.Services = apiServices(h, services, false)
	return result, nil
}

func apiChannelWithoutServices(h *Handler, channel config.ChannelConfig) *apigen.Channel {
	result := &apigen.Channel{
		Type:    channel.Type,
		Channel: channel.Channel,
		Name:    apigen.NewOptString(channel.Name),
		Routes:  apiChannelRoutes(channel.RoutesOrDefault()),
	}
	if channel.TsmfRelTs != nil {
		result.TsmfRelTs = apigen.NewOptInt(int(*channel.TsmfRelTs))
	}
	return result
}

func apiChannelRoutes(routes []config.ChannelRouteConfig) []apigen.ChannelRoute {
	result := make([]apigen.ChannelRoute, len(routes))
	for i, route := range routes {
		result[i] = apigen.ChannelRoute{
			ID:      route.Id,
			Type:    route.Type,
			Channel: route.Channel,
		}
		if route.Remote != "" {
			result[i].Remote = apigen.NewOptString(route.Remote)
		}
		if route.Priority != nil {
			result[i].Priority = apigen.NewOptInt(*route.Priority)
		}
		if route.IsDisabled != nil {
			result[i].IsDisabled = apigen.NewOptBool(*route.IsDisabled)
		}
	}
	return result
}

func apiServices(h *Handler, services []*service.Service, includeChannel bool) []apigen.Service {
	result := make([]apigen.Service, len(services))
	for i, service := range services {
		result[i] = *apiService(h, service, includeChannel)
	}
	return result
}

func apiService(h *Handler, service *service.Service, includeChannel bool) *apigen.Service {
	result := &apigen.Service{
		ID:                apigen.ServiceItemId(service.ItemId()),
		ServiceId:         apigen.ServiceId(service.ServiceId),
		NetworkId:         apigen.NetworkId(service.NetworkId),
		TransportStreamId: apigen.NewOptTransportStreamId(apigen.TransportStreamId(service.TransportStreamId)),
		Name:              service.Name,
		Type:              int(service.Type),
		RemoteControlKeyId: apigen.NewOptInt(
			int(service.RemoteControlKeyId),
		),
	}
	applyEPGStatus(result, &service.EPG)
	if includeChannel {
		if channel := h.serviceManager.GetChannel(service.ChannelType, service.ChannelId); channel != nil {
			result.Channel = apigen.NewOptChannel(*apiChannelWithoutServices(h, *channel))
		}
	}
	if service.LogoId != nil {
		result.LogoId = apigen.NewOptInt(int(*service.LogoId))
	}
	result.HasLogoData = apigen.NewOptBool(service.HasLogoData)
	return result
}

func applyEPGStatus(result *apigen.Service, status *service.EPGStatus) {
	if status == nil {
		return
	}
	if status.LastSuccessAt != nil {
		result.EpgReady = apigen.NewOptBool(true)
		result.EpgUpdatedAt = apigen.NewOptUnixtimeMS(apigen.UnixtimeMS(*status.LastSuccessAt))
	} else {
		result.EpgReady = apigen.NewOptBool(false)
	}
	if status.LastAttemptAt != nil {
		result.EpgLastAttemptAt = apigen.NewOptUnixtimeMS(apigen.UnixtimeMS(*status.LastAttemptAt))
	}
	if status.LastError != "" {
		result.EpgLastError = apigen.NewOptString(status.LastError)
	}
}

func notFound(reason string) *apigen.ErrorStatusCode {
	return &apigen.ErrorStatusCode{
		StatusCode: http.StatusNotFound,
		Response: apigen.Error{
			Code:   apigen.NewOptInt(http.StatusNotFound),
			Reason: apigen.NewOptString(reason),
		},
	}
}
