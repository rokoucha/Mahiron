package api

import (
	"context"
	"encoding/json"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/tuner"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
	"github.com/go-faster/jx"
)

func GetTuners(_ context.Context, h *Handler) (apigen.GetTunersRes, error) {
	statuses := h.tunerManager.Statuses()
	result := make(apigen.GetTunersOKApplicationJSON, len(statuses))
	for i := range statuses {
		result[i] = *apiTuner(statuses[i])
	}
	return &result, nil
}

func GetTuner(_ context.Context, h *Handler, params apigen.GetTunerParams) (apigen.GetTunerRes, error) {
	status, ok := h.tunerManager.Status(params.Index)
	if !ok {
		return notFound("tuner not found"), nil
	}
	return apiTuner(status), nil
}

func GetTunerProcess(_ context.Context, h *Handler, params apigen.GetTunerProcessParams) (apigen.GetTunerProcessRes, error) {
	status, ok := h.tunerManager.Status(params.Index)
	if !ok {
		return notFound("tuner not found"), nil
	}
	return &apigen.TunerProcess{Pid: status.PID}, nil
}

func apiTuner(status tuner.Status) *apigen.TunerDevice {
	result := &apigen.TunerDevice{
		Index: status.Index, Name: status.Name, Types: status.Types,
		Command: status.Command, Pid: status.PID, Users: make([]apigen.TunerUser, len(status.Users)),
		IsAvailable: status.IsAvailable,
		IsRemote:    false,
		IsFree:      status.IsFree, IsUsing: status.IsUsing, IsFault: status.IsFault,
	}
	if status.CurrentChannelType != "" {
		result.CurrentChannelType = apigen.NewOptString(status.CurrentChannelType)
		result.CurrentChannel = apigen.NewOptString(status.CurrentChannel)
	}
	if status.TunedChannelType != "" {
		result.TunedChannelType = apigen.NewOptString(status.TunedChannelType)
		result.TunedChannel = apigen.NewOptString(status.TunedChannel)
	}
	for i := range status.Users {
		result.Users[i] = apiTunerUser(status.Users[i])
	}
	return result
}

func apiTunerUser(user tuner.User) apigen.TunerUser {
	result := apigen.TunerUser{
		ID: user.ID, Priority: user.Priority,
		DisableDecoder: apigen.NewOptBool(user.DisableDecoder),
	}
	if user.Agent != "" {
		result.Agent = apigen.NewOptString(user.Agent)
	}
	if user.URL != "" {
		result.URL = apigen.NewOptString(user.URL)
	}
	if user.StreamSetting.Channel != nil {
		setting := apigen.TunerUserStreamSetting{Channel: apiConfiguredChannel(user.StreamSetting.Channel)}
		if user.StreamSetting.NetworkID != nil {
			setting.NetworkId = apigen.NewOptInt(int(*user.StreamSetting.NetworkID))
		}
		if user.StreamSetting.ServiceID != nil {
			setting.ServiceId = apigen.NewOptInt(int(*user.StreamSetting.ServiceID))
		}
		if user.StreamSetting.EventID != nil {
			setting.EventId = apigen.NewOptInt(int(*user.StreamSetting.EventID))
		}
		if user.StreamSetting.NoProvide != nil {
			setting.NoProvide = apigen.NewOptBool(*user.StreamSetting.NoProvide)
		}
		if user.StreamSetting.ParseNIT != nil {
			setting.ParseNIT = apigen.NewOptBool(*user.StreamSetting.ParseNIT)
		}
		if user.StreamSetting.ParseSDT != nil {
			setting.ParseSDT = apigen.NewOptBool(*user.StreamSetting.ParseSDT)
		}
		if user.StreamSetting.ParseEIT != nil {
			setting.ParseEIT = apigen.NewOptBool(*user.StreamSetting.ParseEIT)
		}
		result.StreamSetting = apigen.NewOptTunerUserStreamSetting(setting)
	}
	if len(user.StreamInfo) > 0 {
		info := make(apigen.TunerUserStreamInfo, len(user.StreamInfo))
		for key, item := range user.StreamInfo {
			info[key] = apigen.TunerUserStreamInfoItem{Packet: item.Packet, Drop: item.Drop}
		}
		result.StreamInfo = apigen.NewOptTunerUserStreamInfo(info)
	}
	return result
}

func apiConfiguredChannel(channel *config.ChannelConfig) apigen.ConfigChannelsItem {
	result := apigen.ConfigChannelsItem{
		Name:    channel.Name,
		Type:    channel.Type,
		Channel: channel.Channel,
		Routes:  apiConfiguredChannelRoutes(channel.RoutesOrDefault()),
	}
	if channel.ServiceId != nil {
		result.ServiceId = apigen.NewOptServiceId(apigen.ServiceId(*channel.ServiceId))
	}
	if channel.TsmfRelTs != nil {
		result.TsmfRelTs = apigen.NewOptInt(int(*channel.TsmfRelTs))
	}
	if channel.CommandVars != nil {
		result.CommandVars = apigen.NewOptConfigChannelsItemCommandVars(apiConfigChannelCommandVars(channel.CommandVars))
	}
	if channel.IsDisabled != nil {
		result.IsDisabled = apigen.NewOptBool(*channel.IsDisabled)
	}
	return result
}

func apiConfiguredChannelRoutes(routes []config.ChannelRouteConfig) []apigen.ConfigChannelRoute {
	result := make([]apigen.ConfigChannelRoute, len(routes))
	for i, route := range routes {
		result[i] = apigen.ConfigChannelRoute{
			Type:    route.Type,
			Channel: route.Channel,
		}
		if route.Id != "" {
			result[i].ID = apigen.NewOptString(route.Id)
		}
		if route.Remote != "" {
			result[i].Remote = apigen.NewOptString(route.Remote)
		}
		if route.ServiceId != nil {
			result[i].ServiceId = apigen.NewOptServiceId(apigen.ServiceId(*route.ServiceId))
		}
		if route.TsmfRelTs != nil {
			result[i].TsmfRelTs = apigen.NewOptInt(int(*route.TsmfRelTs))
		}
		if route.CommandVars != nil {
			result[i].CommandVars = apigen.NewOptConfigChannelRouteCommandVars(apiConfigRouteCommandVars(route.CommandVars))
		}
		if route.IsDisabled != nil {
			result[i].IsDisabled = apigen.NewOptBool(*route.IsDisabled)
		}
		if route.Priority != nil {
			result[i].Priority = apigen.NewOptInt(*route.Priority)
		}
	}
	return result
}

func apiConfigChannelCommandVars(values map[string]any) apigen.ConfigChannelsItemCommandVars {
	result := make(apigen.ConfigChannelsItemCommandVars, len(values))
	for key, value := range values {
		result[key] = apiRawJSON(value)
	}
	return result
}

func apiConfigRouteCommandVars(values map[string]any) apigen.ConfigChannelRouteCommandVars {
	result := make(apigen.ConfigChannelRouteCommandVars, len(values))
	for key, value := range values {
		result[key] = apiRawJSON(value)
	}
	return result
}

func apiRawJSON(value any) jx.Raw {
	data, err := json.Marshal(value)
	if err != nil {
		return jx.Raw("null")
	}
	return jx.Raw(data)
}
