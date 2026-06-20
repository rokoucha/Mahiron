package event

import (
	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/internal/service"
	"github.com/21S1298001/Mahiron5/internal/tuner"
)

func serviceEventData(svc *service.Service, channel *config.ChannelConfig) map[string]any {
	data := map[string]any{
		"id":                 svc.ItemId(),
		"serviceId":          svc.ServiceId,
		"networkId":          svc.NetworkId,
		"transportStreamId":  svc.TransportStreamId,
		"name":               svc.Name,
		"type":               int(svc.Type),
		"remoteControlKeyId": int(svc.RemoteControlKeyId),
	}
	if svc.EPG.LastSuccessAt != nil {
		data["epgReady"] = true
		data["epgUpdatedAt"] = *svc.EPG.LastSuccessAt
	} else {
		data["epgReady"] = false
	}
	if svc.EPG.LastAttemptAt != nil {
		data["epgLastAttemptAt"] = *svc.EPG.LastAttemptAt
	}
	if svc.EPG.LastError != "" {
		data["epgLastError"] = svc.EPG.LastError
	}
	if channel != nil {
		channelData := map[string]any{
			"type":    channel.Type,
			"channel": channel.Channel,
			"name":    channel.Name,
		}
		if channel.TsmfRelTs != nil {
			channelData["tsmfRelTs"] = *channel.TsmfRelTs
		}
		data["channel"] = channelData
	}
	return data
}

func tunerEventData(status tuner.Status) map[string]any {
	data := map[string]any{
		"index":       status.Index,
		"name":        status.Name,
		"types":       status.Types,
		"command":     status.Command,
		"pid":         status.PID,
		"users":       tunerUserEventData(status.Users),
		"isAvailable": status.IsAvailable,
		"isFree":      status.IsFree,
		"isUsing":     status.IsUsing,
		"isFault":     status.IsFault,
	}
	if status.CurrentChannelType != "" {
		data["currentChannelType"] = status.CurrentChannelType
		data["currentChannel"] = status.CurrentChannel
	}
	if status.TunedChannelType != "" {
		data["tunedChannelType"] = status.TunedChannelType
		data["tunedChannel"] = status.TunedChannel
	}
	return data
}

func tunerUserEventData(users []tuner.User) []map[string]any {
	result := make([]map[string]any, len(users))
	for i, user := range users {
		data := map[string]any{
			"id":             user.ID,
			"priority":       user.Priority,
			"disableDecoder": user.DisableDecoder,
		}
		if user.Agent != "" {
			data["agent"] = user.Agent
		}
		if user.URL != "" {
			data["url"] = user.URL
		}
		if setting := streamSettingEventData(user.StreamSetting); len(setting) > 0 {
			data["streamSetting"] = setting
		}
		result[i] = data
	}
	return result
}

func streamSettingEventData(setting tuner.StreamSetting) map[string]any {
	data := map[string]any{}
	if setting.Channel != nil {
		data["channel"] = map[string]any{
			"name":    setting.Channel.Name,
			"type":    setting.Channel.Type,
			"channel": setting.Channel.Channel,
		}
	}
	if setting.NetworkID != nil {
		data["networkId"] = *setting.NetworkID
	}
	if setting.ServiceID != nil {
		data["serviceId"] = *setting.ServiceID
	}
	if setting.EventID != nil {
		data["eventId"] = *setting.EventID
	}
	if setting.NoProvide != nil {
		data["noProvide"] = *setting.NoProvide
	}
	if setting.ParseNIT != nil {
		data["parseNIT"] = *setting.ParseNIT
	}
	if setting.ParseSDT != nil {
		data["parseSDT"] = *setting.ParseSDT
	}
	if setting.ParseEIT != nil {
		data["parseEIT"] = *setting.ParseEIT
	}
	return data
}

func programEventData(p *program.Program) map[string]any {
	data := map[string]any{
		"id":        p.ID,
		"eventId":   p.EventID,
		"serviceId": p.ServiceID,
		"networkId": p.NetworkID,
		"startAt":   p.StartAt,
		"duration":  p.Duration,
		"isFree":    p.IsFree,
	}
	if p.Name != "" {
		data["name"] = p.Name
	}
	if p.Description != "" {
		data["description"] = p.Description
	}
	if len(p.Genres) > 0 {
		data["genres"] = programGenresEventData(p.Genres)
	}
	if p.Video != nil {
		data["video"] = map[string]any{
			"streamContent": p.Video.StreamContent,
			"componentType": p.Video.ComponentType,
		}
	}
	if len(p.Audios) > 0 {
		data["audios"] = programAudiosEventData(p.Audios)
	}
	if len(p.Extended) > 0 {
		data["extended"] = p.Extended
	}
	if len(p.RelatedItems) > 0 {
		data["relatedItems"] = programRelatedItemsEventData(p.RelatedItems)
	}
	if p.Series != nil {
		series := map[string]any{
			"id":          p.Series.ID,
			"repeat":      p.Series.Repeat,
			"pattern":     p.Series.Pattern,
			"episode":     p.Series.Episode,
			"lastEpisode": p.Series.LastEpisode,
		}
		if p.Series.ExpiresAt != nil {
			series["expiresAt"] = *p.Series.ExpiresAt
		}
		if p.Series.Name != "" {
			series["name"] = p.Series.Name
		}
		data["series"] = series
	}
	return data
}

func programRemoveEventData(id int64) map[string]int64 {
	return map[string]int64{"id": id}
}

func programGenresEventData(genres []program.Genre) []map[string]any {
	result := make([]map[string]any, len(genres))
	for i, genre := range genres {
		result[i] = map[string]any{
			"lv1": genre.Lv1,
			"lv2": genre.Lv2,
			"un1": genre.Un1,
			"un2": genre.Un2,
		}
	}
	return result
}

func programAudiosEventData(audios []program.Audio) []map[string]any {
	result := make([]map[string]any, len(audios))
	for i, audio := range audios {
		data := map[string]any{
			"componentType": audio.ComponentType,
		}
		if audio.ComponentTag != nil {
			data["componentTag"] = *audio.ComponentTag
		}
		if audio.IsMain != nil {
			data["isMain"] = *audio.IsMain
		}
		if audio.SamplingRate != nil {
			data["samplingRate"] = *audio.SamplingRate
		}
		if len(audio.Langs) > 0 {
			data["langs"] = audio.Langs
		}
		result[i] = data
	}
	return result
}

func programRelatedItemsEventData(items []program.RelatedItem) []map[string]any {
	result := make([]map[string]any, len(items))
	for i, item := range items {
		data := map[string]any{}
		if item.Type != "" {
			data["type"] = item.Type
		}
		if item.NetworkID != nil {
			data["networkId"] = *item.NetworkID
		}
		if item.TransportStreamID != nil {
			data["transportStreamId"] = *item.TransportStreamID
		}
		if item.ServiceID != 0 {
			data["serviceId"] = item.ServiceID
		}
		if item.EventID != 0 {
			data["eventId"] = item.EventID
		}
		result[i] = data
	}
	return result
}
