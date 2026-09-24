package mirakurun

import (
	"github.com/21S1298001/mahiron/internal/program"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

// ProgramToAPI converts a program to its Mirakurun-compatible API shape.
// Fields with no information stay absent so that clients can tell "unknown"
// from a zero value: genres is omitted entirely when empty (EPGStation reads
// genres[0].lv1 whenever the key exists), and video type/resolution appear
// only for known values.
func ProgramToAPI(p *program.Program) apigen.Program {
	result := apigen.Program{
		ID:           apigen.ProgramId(p.ID),
		EventId:      apigen.EventId(p.EventID),
		ServiceId:    apigen.ServiceId(p.ServiceID),
		NetworkId:    apigen.NetworkId(p.NetworkID),
		StartAt:      apigen.UnixtimeMS(p.StartAt),
		Duration:     p.Duration,
		IsFree:       p.IsFree,
		Genres:       programGenresToAPI(p.Genres),
		Audios:       programAudiosToAPI(p.Audios),
		RelatedItems: relatedItemsToAPI(p.RelatedItems),
	}
	if p.Name != "" {
		result.Name = apigen.NewOptString(p.Name)
	}
	if p.Description != "" {
		result.Description = apigen.NewOptString(p.Description)
	}
	if p.Video != nil {
		result.Video = apigen.NewOptProgramVideo(programVideoToAPI(p.Video))
	}
	result.Extended = extendedToAPI(p.Extended)
	if p.Series != nil {
		result.Series = apigen.NewOptProgramSeries(programSeriesToAPI(p.Series))
	}
	return result
}

// ProgramsToAPI converts programs to their Mirakurun-compatible API shapes.
func ProgramsToAPI(programs []*program.Program) []apigen.Program {
	result := make([]apigen.Program, len(programs))
	for i, p := range programs {
		result[i] = ProgramToAPI(p)
	}
	return result
}

func programVideoToAPI(video *program.Video) apigen.ProgramVideo {
	out := apigen.ProgramVideo{
		StreamContent: apigen.NewOptInt(video.StreamContent),
		ComponentType: apigen.NewOptInt(video.ComponentType),
	}
	if videoType, ok := VideoType(video.StreamContent); ok {
		out.Type = apigen.NewOptProgramVideoType(apigen.ProgramVideoType(videoType))
	}
	if resolution, ok := VideoResolution(video.ComponentType); ok {
		out.Resolution = apigen.NewOptProgramVideoResolution(apigen.ProgramVideoResolution(resolution))
	}
	return out
}

func programSeriesToAPI(s *program.Series) apigen.ProgramSeries {
	out := apigen.ProgramSeries{
		ID:          apigen.NewOptInt(s.ID),
		Repeat:      apigen.NewOptInt(s.Repeat),
		Episode:     apigen.NewOptProgramEpisodeNumber(apigen.ProgramEpisodeNumber(s.Episode)),
		LastEpisode: apigen.NewOptProgramEpisodeNumber(apigen.ProgramEpisodeNumber(s.LastEpisode)),
	}
	// A negative pattern means "no series pattern"; the key stays absent.
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

// programGenresToAPI returns nil for programs without genre information so
// that the "genres" key is omitted entirely, matching Mirakurun.
func programGenresToAPI(genres []program.Genre) []apigen.ProgramGenre {
	if len(genres) == 0 {
		return nil
	}
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

func programAudiosToAPI(audios []program.Audio) []apigen.ProgramAudiosItem {
	result := make([]apigen.ProgramAudiosItem, len(audios))
	for i, audio := range audios {
		item := apigen.ProgramAudiosItem{
			ComponentType: apigen.NewOptInt(audio.ComponentType),
			Langs:         programAudioLangsToAPI(audio.Langs),
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

func programAudioLangsToAPI(langs []string) []apigen.ProgramAudiosItemLangsItem {
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

func relatedItemsToAPI(items []program.RelatedItem) []apigen.RelatedItem {
	if len(items) == 0 {
		return []apigen.RelatedItem{}
	}
	result := make([]apigen.RelatedItem, len(items))
	for i, item := range items {
		result[i] = relatedItemToAPI(item)
	}
	return result
}

func relatedItemToAPI(item program.RelatedItem) apigen.RelatedItem {
	out := apigen.RelatedItem{}
	if item.Type != "" {
		t := apigen.RelatedItemType(item.Type)
		out.Type = apigen.NewOptRelatedItemType(t)
	}
	if item.NetworkID != nil {
		out.NetworkId = apigen.NewOptInt(int(*item.NetworkID))
	}
	if item.TransportStreamID != nil {
		out.TransportStreamId = apigen.NewOptInt(int(*item.TransportStreamID))
	}
	out.ServiceId = apigen.NewOptInt(int(item.ServiceID))
	out.EventId = apigen.NewOptInt(int(item.EventID))
	return out
}

// VideoType maps a stream_content value to the Mirakurun video type string.
func VideoType(streamContent int) (string, bool) {
	switch streamContent {
	case 0x1:
		return "mpeg2", true
	case 0x5:
		return "h.264", true
	case 0x9:
		return "h.265", true
	default:
		return "", false
	}
}

// VideoResolution maps a component_type value to the Mirakurun resolution
// string.
func VideoResolution(componentType int) (string, bool) {
	switch {
	case componentType >= 0x01 && componentType <= 0x04:
		return "480i", true
	case componentType == 0x83:
		return "4320p", true
	case componentType >= 0x91 && componentType <= 0x94:
		return "2160p", true
	case componentType >= 0xA1 && componentType <= 0xA4:
		return "480p", true
	case componentType >= 0xB1 && componentType <= 0xB4:
		return "1080i", true
	case componentType >= 0xC1 && componentType <= 0xC4:
		return "720p", true
	case componentType >= 0xD1 && componentType <= 0xD4:
		return "240p", true
	case componentType >= 0xE1 && componentType <= 0xE4:
		return "1080p", true
	default:
		return "", false
	}
}

// ProgramFromAPI converts a Mirakurun-compatible program, as served by our
// own API or received from a remote server, back to a program. Absent
// optional values become zero values, and an absent series pattern becomes
// -1 ("no pattern"), mirroring what ProgramToAPI omits.
func ProgramFromAPI(p *apigen.Program) *program.Program {
	prog := &program.Program{
		ID:           int64(p.ID),
		EventID:      uint16(p.EventId),
		ServiceID:    uint16(p.ServiceId),
		NetworkID:    uint16(p.NetworkId),
		StartAt:      int64(p.StartAt),
		Duration:     p.Duration,
		IsFree:       p.IsFree,
		Genres:       programGenresFromAPI(p.Genres),
		Audios:       programAudiosFromAPI(p.Audios),
		Extended:     extendedFromAPI(p.Extended),
		RelatedItems: relatedItemsFromAPI(p.RelatedItems),
	}
	if name, ok := p.Name.Get(); ok {
		prog.Name = name
	}
	if description, ok := p.Description.Get(); ok {
		prog.Description = description
	}
	if video, ok := p.Video.Get(); ok {
		streamContent, _ := video.StreamContent.Get()
		componentType, _ := video.ComponentType.Get()
		prog.Video = &program.Video{
			StreamContent: streamContent,
			ComponentType: componentType,
		}
	}
	if series, ok := p.Series.Get(); ok {
		prog.Series = programSeriesFromAPI(&series)
	}
	return prog
}

func programGenresFromAPI(items []apigen.ProgramGenre) []program.Genre {
	if len(items) == 0 {
		return nil
	}
	result := make([]program.Genre, len(items))
	for i, item := range items {
		lv1, _ := item.Lv1.Get()
		lv2, _ := item.Lv2.Get()
		un1, _ := item.Un1.Get()
		un2, _ := item.Un2.Get()
		result[i] = program.Genre{Lv1: lv1, Lv2: lv2, Un1: un1, Un2: un2}
	}
	return result
}

func programAudiosFromAPI(items []apigen.ProgramAudiosItem) []program.Audio {
	if len(items) == 0 {
		return nil
	}
	result := make([]program.Audio, len(items))
	for i, item := range items {
		componentType, _ := item.ComponentType.Get()
		audio := program.Audio{ComponentType: componentType}
		if tag, ok := item.ComponentTag.Get(); ok {
			audio.ComponentTag = &tag
		}
		if isMain, ok := item.IsMain.Get(); ok {
			audio.IsMain = &isMain
		}
		if rate, ok := item.SamplingRate.Get(); ok {
			v := int(rate)
			audio.SamplingRate = &v
		}
		for _, lang := range item.Langs {
			audio.Langs = append(audio.Langs, string(lang))
		}
		result[i] = audio
	}
	return result
}

func relatedItemsFromAPI(items []apigen.RelatedItem) []program.RelatedItem {
	if len(items) == 0 {
		return nil
	}
	result := make([]program.RelatedItem, len(items))
	for i, item := range items {
		related := program.RelatedItem{}
		if typ, ok := item.Type.Get(); ok {
			related.Type = program.RelatedItemType(typ)
		}
		if networkID, ok := item.NetworkId.Get(); ok {
			v := uint16(networkID)
			related.NetworkID = &v
		}
		if tsID, ok := item.TransportStreamId.Get(); ok {
			v := uint16(tsID)
			related.TransportStreamID = &v
		}
		if serviceID, ok := item.ServiceId.Get(); ok {
			related.ServiceID = uint16(serviceID)
		}
		if eventID, ok := item.EventId.Get(); ok {
			related.EventID = uint16(eventID)
		}
		result[i] = related
	}
	return result
}

func programSeriesFromAPI(s *apigen.ProgramSeries) *program.Series {
	id, _ := s.ID.Get()
	repeat, _ := s.Repeat.Get()
	episode, _ := s.Episode.Get()
	lastEpisode, _ := s.LastEpisode.Get()
	series := &program.Series{
		ID:          id,
		Repeat:      repeat,
		Pattern:     -1,
		Episode:     int(episode),
		LastEpisode: int(lastEpisode),
	}
	if pattern, ok := s.Pattern.Get(); ok {
		series.Pattern = int(pattern)
	}
	if expiresAt, ok := s.ExpiresAt.Get(); ok {
		v := int64(expiresAt)
		series.ExpiresAt = &v
	}
	if name, ok := s.Name.Get(); ok {
		series.Name = name
	}
	return series
}
