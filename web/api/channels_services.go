package api

import (
	"context"
	"net/http"
	"strconv"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/service"
	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

func GetChannels(ctx context.Context, h *Handler, params apigen.GetChannelsParams) (apigen.GetChannelsRes, error) {
	channels := h.serviceManager.GetChannels()
	res := apigen.GetChannelsOKApplicationJSON(apiChannels(h, filterChannels(channels, params.Type, params.Channel, params.Name)))
	return &res, nil
}

func GetChannelsByType(ctx context.Context, h *Handler, params apigen.GetChannelsByTypeParams) (apigen.GetChannelsByTypeRes, error) {
	channels := h.serviceManager.GetChannels()
	filtered := make(config.ChannelsConfig, 0, len(channels))
	for _, channel := range filterChannels(channels, apigen.NewOptString(params.Type), params.Channel, params.Name) {
		filtered = append(filtered, channel)
	}
	res := apigen.GetChannelsByTypeOKApplicationJSON(apiChannels(h, filtered))
	return &res, nil
}

func GetChannel(ctx context.Context, h *Handler, params apigen.GetChannelParams) (apigen.GetChannelRes, error) {
	channel := h.serviceManager.GetChannel(params.Type, params.Channel)
	if channel == nil {
		return notFound("channel not found"), nil
	}
	return apiChannel(h, *channel, true), nil
}

func GetServices(ctx context.Context, h *Handler, params apigen.GetServicesParams) (apigen.GetServicesRes, error) {
	services := h.serviceManager.GetServices()
	filtered := filterServices(services, params)
	res := apigen.GetServicesOKApplicationJSON(apiServices(h, filtered, true))
	return &res, nil
}

func GetService(ctx context.Context, h *Handler, params apigen.GetServiceParams) (apigen.GetServiceRes, error) {
	service := h.serviceManager.GetServiceById(strconv.FormatInt(params.ID, 10))
	if service == nil {
		return notFound("service not found"), nil
	}
	return apiService(h, service, true), nil
}

func GetServicesByChannel(ctx context.Context, h *Handler, params apigen.GetServicesByChannelParams) (apigen.GetServicesByChannelRes, error) {
	if h.serviceManager.GetChannel(params.Type, params.Channel) == nil {
		return notFound("channel not found"), nil
	}
	res := apigen.GetServicesByChannelOKApplicationJSON(apiServices(h, h.serviceManager.GetServicesByChannel(params.Type, params.Channel), true))
	return &res, nil
}

func GetServiceByChannel(ctx context.Context, h *Handler, params apigen.GetServiceByChannelParams) (apigen.GetServiceByChannelRes, error) {
	if h.serviceManager.GetChannel(params.Type, params.Channel) == nil {
		return notFound("channel not found"), nil
	}
	svc := h.serviceManager.GetServiceByChannelAndId(params.Type, params.Channel, strconv.FormatInt(params.ID, 10))
	if svc == nil {
		return notFound("service not found"), nil
	}
	res := apigen.GetServiceByChannelOKApplicationJSON(apiServices(h, []*service.Service{svc}, true))
	return &res, nil
}

func apiChannels(h *Handler, channels config.ChannelsConfig) []apigen.Channel {
	result := make([]apigen.Channel, len(channels))
	for i, channel := range channels {
		result[i] = *apiChannel(h, channel, true)
	}
	return result
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

func apiChannel(h *Handler, channel config.ChannelConfig, includeServices bool) *apigen.Channel {
	result := &apigen.Channel{
		Type:    channel.Type,
		Channel: channel.Channel,
		Name:    apigen.NewOptString(channel.Name),
		Routes:  apiChannelRoutes(channel.RoutesOrDefault()),
	}
	if channel.TsmfRelTs != nil {
		result.TsmfRelTs = apigen.NewOptInt(int(*channel.TsmfRelTs))
	}
	if includeServices {
		result.Services = apiServices(h, h.serviceManager.GetServicesByChannel(channel.Type, channel.Channel), false)
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
	if includeChannel {
		if channel := h.serviceManager.GetChannel(service.ChannelType, service.ChannelId); channel != nil {
			result.Channel = apigen.NewOptChannel(*apiChannel(h, *channel, false))
		}
	}
	return result
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
