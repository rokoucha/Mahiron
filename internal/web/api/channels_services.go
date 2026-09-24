package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/mirakurun"
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
	filtered = append(filtered, filterChannels(channels, apigen.NewOptString(params.Type), params.Channel, params.Name)...)
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
	etag := logoETag(data)
	if value, ok := params.IfNoneMatch.Get(); ok && etagMatches(value, etag) {
		return &apigen.GetLogoImageNotModified{}, nil
	}
	return &apigen.GetLogoImageOKHeaders{
		ETag:         apigen.NewOptString(etag),
		CacheControl: apigen.NewOptString("public, max-age=86400"),
		Response:     apigen.GetLogoImageOK{Data: bytes.NewReader(data)},
	}, nil
}

// logoETag derives a strong ETag from logo bytes. logo_version changes
// whenever the logo data changes, so the hash naturally changes with it.
func logoETag(data []byte) string {
	sum := sha256.Sum256(data)
	return `"` + hex.EncodeToString(sum[:16]) + `"`
}

func apiChannels(ctx context.Context, h *Handler, channels config.ChannelsConfig) ([]apigen.Channel, error) {
	grouped, err := h.serviceManager.GetServicesGroupedByChannel(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]apigen.Channel, len(channels))
	for i, channel := range channels {
		item := apiChannelWithoutServices(h, channel)
		key := service.ChannelKey{Type: channel.Type, ID: channel.Channel}
		item.Services = apiServices(h, grouped[key], false)
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
	result := mirakurun.ChannelToAPI(channel)
	return &result
}

// resolveServiceChannel returns the channel a service belongs to, or nil when
// the channel is no longer configured.
func resolveServiceChannel(h *Handler, svc *service.Service) *config.ChannelConfig {
	return h.serviceManager.GetChannel(svc.ChannelType, svc.ChannelId)
}

func apiServices(h *Handler, services []*service.Service, includeChannel bool) []apigen.Service {
	return mirakurun.ServicesToAPI(services, func(svc *service.Service) *config.ChannelConfig {
		return resolveServiceChannel(h, svc)
	}, includeChannel)
}

func apiService(h *Handler, service *service.Service, includeChannel bool) *apigen.Service {
	result := mirakurun.ServiceToAPI(service, resolveServiceChannel(h, service), includeChannel)
	return &result
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
