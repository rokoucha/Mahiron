package api

import (
	"context"
	"strconv"

	"github.com/21S1298001/mahiron/internal/program"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func GetPrograms(ctx context.Context, h *Handler, params apigen.GetProgramsParams) (apigen.GetProgramsRes, error) {
	programs, err := h.programManager.List(ctx, programQuery(params))
	if err != nil {
		return nil, err
	}
	res := apigen.GetProgramsOKApplicationJSON(apiPrograms(programs))
	return &res, nil
}

func GetProgram(ctx context.Context, h *Handler, params apigen.GetProgramParams) (apigen.GetProgramRes, error) {
	p, ok, err := h.programManager.Get(ctx, params.ID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return notFound("program not found"), nil
	}
	return apiProgram(p), nil
}

func GetServicePrograms(ctx context.Context, h *Handler, params apigen.GetServiceProgramsParams) (apigen.GetServiceProgramsRes, error) {
	service, err := h.serviceManager.GetServiceById(ctx, strconv.FormatInt(params.ID, 10))
	if err != nil {
		return nil, err
	}
	if service == nil {
		return notFound("service not found"), nil
	}
	networkID := service.NetworkId
	serviceID := service.ServiceId
	programs, err := h.programManager.List(ctx, program.Query{
		NetworkID: &networkID,
		ServiceID: &serviceID,
	})
	if err != nil {
		return nil, err
	}
	res := apigen.GetServiceProgramsOKApplicationJSON(apiPrograms(programs))
	return &res, nil
}

func programQuery(params apigen.GetProgramsParams) program.Query {
	var query program.Query
	if value, ok := params.NetworkId.Get(); ok {
		v := uint16(value)
		query.NetworkID = &v
	}
	if value, ok := params.ServiceId.Get(); ok {
		v := uint16(value)
		query.ServiceID = &v
	}
	if value, ok := params.EventId.Get(); ok {
		v := uint16(value)
		query.EventID = &v
	}
	return query
}

func apiPrograms(programs []*program.Program) []apigen.Program {
	result := make([]apigen.Program, len(programs))
	for i, p := range programs {
		result[i] = *apiProgram(p)
	}
	return result
}

func apiProgram(p *program.Program) *apigen.Program {
	result := &apigen.Program{
		ID:           apigen.ProgramId(p.ID),
		EventId:      apigen.EventId(p.EventID),
		ServiceId:    apigen.ServiceId(p.ServiceID),
		NetworkId:    apigen.NetworkId(p.NetworkID),
		StartAt:      apigen.UnixtimeMS(p.StartAt),
		Duration:     p.Duration,
		IsFree:       p.IsFree,
		Genres:       apiProgramGenres(p.Genres),
		Audios:       apiProgramAudios(p.Audios),
		RelatedItems: apiRelatedItems(p.RelatedItems),
	}
	if p.Name != "" {
		result.Name = apigen.NewOptString(p.Name)
	}
	if p.Description != "" {
		result.Description = apigen.NewOptString(p.Description)
	}
	if p.Video != nil {
		result.Video = apigen.NewOptProgramVideo(apiProgramVideo(p.Video))
	}
	if len(p.Extended) > 0 {
		result.Extended = apigen.NewOptProgramExtended(apigen.ProgramExtended(p.Extended))
	}
	if p.Series != nil {
		result.Series = apigen.NewOptProgramSeries(apiProgramSeries(p.Series))
	}
	return result
}

func apiProgramVideo(video *program.Video) apigen.ProgramVideo {
	out := apigen.ProgramVideo{
		StreamContent: apigen.NewOptInt(video.StreamContent),
		ComponentType: apigen.NewOptInt(video.ComponentType),
	}
	if videoType, ok := apiProgramVideoType(video.StreamContent); ok {
		out.Type = apigen.NewOptProgramVideoType(videoType)
	}
	if resolution, ok := apiProgramVideoResolution(video.ComponentType); ok {
		out.Resolution = apigen.NewOptProgramVideoResolution(resolution)
	}
	return out
}

func apiProgramVideoType(streamContent int) (apigen.ProgramVideoType, bool) {
	switch streamContent {
	case 0x1:
		return apigen.ProgramVideoTypeMpeg2, true
	case 0x5:
		return apigen.ProgramVideoTypeH264, true
	case 0x9:
		return apigen.ProgramVideoTypeH265, true
	default:
		return "", false
	}
}

func apiProgramVideoResolution(componentType int) (apigen.ProgramVideoResolution, bool) {
	switch {
	case componentType >= 0x01 && componentType <= 0x04:
		return apigen.ProgramVideoResolution480i, true
	case componentType == 0x83:
		return apigen.ProgramVideoResolution4320p, true
	case componentType >= 0x91 && componentType <= 0x94:
		return apigen.ProgramVideoResolution2160p, true
	case componentType >= 0xA1 && componentType <= 0xA4:
		return apigen.ProgramVideoResolution480p, true
	case componentType >= 0xB1 && componentType <= 0xB4:
		return apigen.ProgramVideoResolution1080i, true
	case componentType >= 0xC1 && componentType <= 0xC4:
		return apigen.ProgramVideoResolution720p, true
	case componentType >= 0xD1 && componentType <= 0xD4:
		return apigen.ProgramVideoResolution240p, true
	case componentType >= 0xE1 && componentType <= 0xE4:
		return apigen.ProgramVideoResolution1080p, true
	default:
		return "", false
	}
}

func apiRelatedItems(items []program.RelatedItem) []apigen.RelatedItem {
	if len(items) == 0 {
		return []apigen.RelatedItem{}
	}
	result := make([]apigen.RelatedItem, len(items))
	for i, item := range items {
		result[i] = apiRelatedItem(item)
	}
	return result
}

func apiRelatedItem(item program.RelatedItem) apigen.RelatedItem {
	out := apigen.RelatedItem{}
	if item.Type != "" {
		t := apigen.RelatedItemType(item.Type)
		out.Type = apigen.NewOptRelatedItemType(t)
	}
	if item.NetworkID != nil {
		out.NetworkId = apigen.NewOptInt(int(*item.NetworkID))
	}
	out.ServiceId = apigen.NewOptInt(int(item.ServiceID))
	out.EventId = apigen.NewOptInt(int(item.EventID))
	return out
}

func apiProgramSeries(s *program.Series) apigen.ProgramSeries {
	out := apigen.ProgramSeries{
		ID:          apigen.NewOptInt(s.ID),
		Repeat:      apigen.NewOptInt(s.Repeat),
		Episode:     apigen.NewOptProgramEpisodeNumber(apigen.ProgramEpisodeNumber(s.Episode)),
		LastEpisode: apigen.NewOptProgramEpisodeNumber(apigen.ProgramEpisodeNumber(s.LastEpisode)),
	}
	if s.Pattern >= 0 {
		out.Pattern = apigen.NewOptProgramPattern(apigen.ProgramPattern(s.Pattern))
	}
	if s.ExpiresAt != nil {
		out.ExpiresAt = apigen.NewOptUnixtimeMS(apigen.UnixtimeMS(*s.ExpiresAt))
	}
	if s.Name != "" {
		out.Name = apigen.NewOptString(s.Name)
	}
	return out
}

func apiProgramGenres(genres []program.Genre) []apigen.ProgramGenre {
	result := make([]apigen.ProgramGenre, len(genres))
	for i, genre := range genres {
		result[i] = apigen.ProgramGenre{
			Lv1: apigen.NewOptInt(genre.Lv1),
			Lv2: apigen.NewOptInt(genre.Lv2),
			Un1: apigen.NewOptInt(genre.Un1),
			Un2: apigen.NewOptInt(genre.Un2),
		}
	}
	return result
}

func apiProgramAudios(audios []program.Audio) []apigen.ProgramAudiosItem {
	result := make([]apigen.ProgramAudiosItem, len(audios))
	for i, audio := range audios {
		item := apigen.ProgramAudiosItem{
			ComponentType: apigen.NewOptInt(audio.ComponentType),
			Langs:         apiProgramAudioLangs(audio.Langs),
		}
		if audio.ComponentTag != nil {
			item.ComponentTag = apigen.NewOptInt(*audio.ComponentTag)
		}
		if audio.IsMain != nil {
			item.IsMain = apigen.NewOptBool(*audio.IsMain)
		}
		if audio.SamplingRate != nil {
			item.SamplingRate = apigen.NewOptProgramAudioSamplingRate(apigen.ProgramAudioSamplingRate(*audio.SamplingRate))
		}
		result[i] = item
	}
	return result
}

func apiProgramAudioLangs(langs []string) []apigen.ProgramAudiosItemLangsItem {
	result := make([]apigen.ProgramAudiosItemLangsItem, 0, len(langs))
	for _, lang := range langs {
		switch lang {
		case "jpn", "eng", "deu", "fra", "ita", "rus", "zho", "kor", "spa":
			result = append(result, apigen.ProgramAudiosItemLangsItem(lang))
		case "etc":
			result = append(result, apigen.ProgramAudiosItemLangsItemEtc)
		}
	}
	return result
}
