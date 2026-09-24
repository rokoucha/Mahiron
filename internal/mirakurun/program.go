package mirakurun

import (
	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/model"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

// ProgramToAPI converts a broadcast event to its Mirakurun-compatible
// program. Fields with no information stay absent so that clients can tell
// "unknown" from a zero value: genres is omitted entirely when empty
// (EPGStation reads genres[0].lv1 whenever the key exists), and video
// type/resolution appear only for known values. An undecided start time or
// duration is written as 0.
func ProgramToAPI(e *model.Event) apigen.Program {
	result := apigen.Program{
		ID:           apigen.ProgramId(model.ProgramID(e.Key, e.EventID)),
		EventId:      apigen.EventId(e.EventID),
		ServiceId:    apigen.ServiceId(e.Key.ServiceID),
		NetworkId:    apigen.NetworkId(e.Key.NetworkID),
		IsFree:       !e.FreeCA,
		Genres:       genresToAPI(e.Genres),
		Audios:       audiosToAPI(e.Audios),
		Extended:     extendedToAPI(e.Extended),
		RelatedItems: relatedItemsToAPI(e.Related),
	}
	if e.StartAt != nil {
		result.StartAt = apigen.UnixtimeMS(*e.StartAt)
	}
	if e.DurationMS != nil {
		result.Duration = *e.DurationMS
	}
	if e.Name != "" {
		result.Name = apigen.NewOptString(e.Name)
	}
	if e.Description != "" {
		result.Description = apigen.NewOptString(e.Description)
	}
	// Mirakurun describes one video; the first component is the main one.
	if len(e.Videos) > 0 {
		result.Video = apigen.NewOptProgramVideo(videoToAPI(e.Videos[0]))
	}
	if e.Series != nil {
		result.Series = apigen.NewOptProgramSeries(seriesToAPI(e.Series))
	}
	return result
}

// videoTypes maps codecs to the Mirakurun video type strings.
var videoTypes = map[model.VideoCodec]apigen.ProgramVideoType{
	model.VideoCodecMPEG2: "mpeg2",
	model.VideoCodecH264:  "h.264",
	model.VideoCodecH265:  "h.265",
}

// videoToAPI writes the STD-B10 stream_content and component_type values
// Mirakurun exposes. A codec or a resolution outside the STD-B10 tables
// becomes 0 and has no type or resolution.
func videoToAPI(video model.VideoComponent) apigen.ProgramVideo {
	streamContent, _ := isdb.TSStreamContentForVideoCodec(string(video.Codec))
	componentType, _ := isdb.VideoComponentTypeToRaw(string(video.Resolution), string(video.Aspect))
	out := apigen.ProgramVideo{
		StreamContent: apigen.NewOptInt(int(streamContent)),
		ComponentType: apigen.NewOptInt(int(componentType)),
	}
	if videoType, ok := videoTypes[video.Codec]; ok {
		out.Type = apigen.NewOptProgramVideoType(videoType)
	}
	if video.Resolution != model.VideoResolutionUnknown {
		out.Resolution = apigen.NewOptProgramVideoResolution(apigen.ProgramVideoResolution(video.Resolution))
	}
	return out
}

func seriesToAPI(s *model.Series) apigen.ProgramSeries {
	out := apigen.ProgramSeries{
		ID:          apigen.NewOptInt(s.ID),
		Repeat:      apigen.NewOptInt(s.Repeat),
		Episode:     apigen.NewOptProgramEpisodeNumber(apigen.ProgramEpisodeNumber(s.Episode)),
		LastEpisode: apigen.NewOptProgramEpisodeNumber(apigen.ProgramEpisodeNumber(s.LastEpisode)),
	}
	if s.Pattern != nil {
		out.Pattern = apigen.NewOptProgramPattern(apigen.ProgramPattern(*s.Pattern))
	}
	if s.ExpiresAt != nil {
		out.ExpiresAt = apigen.NewOptUnixtimeMS(apigen.UnixtimeMS(*s.ExpiresAt))
	}
	if s.Name != "" {
		out.Name = apigen.NewOptString(s.Name)
	}
	return out
}

// genresToAPI returns nil for programs without genre information so that
// the "genres" key is omitted entirely, matching Mirakurun.
func genresToAPI(genres []model.Genre) []apigen.ProgramGenre {
	if len(genres) == 0 {
		return nil
	}
	result := make([]apigen.ProgramGenre, len(genres))
	for i, genre := range genres {
		result[i] = apigen.ProgramGenre{
			Lv1: apigen.NewOptInt(int(genre.Lv1)),
			Lv2: apigen.NewOptInt(int(genre.Lv2)),
			Un1: apigen.NewOptInt(int(genre.Un1)),
			Un2: apigen.NewOptInt(int(genre.Un2)),
		}
	}
	return result
}

func audiosToAPI(audios []model.AudioComponent) []apigen.ProgramAudiosItem {
	result := make([]apigen.ProgramAudiosItem, len(audios))
	for i, audio := range audios {
		item := apigen.ProgramAudiosItem{
			ComponentType: apigen.NewOptInt(int(audio.ComponentType)),
			ComponentTag:  apigen.NewOptInt(int(audio.Tag)),
			IsMain:        apigen.NewOptBool(audio.Main),
			Langs:         audioLangsToAPI(audio.Languages),
		}
		if audio.SamplingHz != 0 {
			item.SamplingRate = apigen.NewOptProgramAudioSamplingRate(apigen.ProgramAudioSamplingRate(audio.SamplingHz))
		}
		result[i] = item
	}
	return result
}

func audioLangsToAPI(langs []string) []apigen.ProgramAudiosItemLangsItem {
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

// relatedItemTypes are the group types Mirakurun names. The other-network
// relay and movement (4 and 5) have no Mirakurun name.
var relatedItemTypes = map[model.EventGroupType]apigen.RelatedItemType{
	model.EventGroupShared:   apigen.RelatedItemTypeShared,
	model.EventGroupRelay:    apigen.RelatedItemTypeRelay,
	model.EventGroupMovement: apigen.RelatedItemTypeMovement,
}

func relatedItemsToAPI(items []model.RelatedEvent) []apigen.RelatedItem {
	result := make([]apigen.RelatedItem, len(items))
	for i, item := range items {
		out := apigen.RelatedItem{
			ServiceId: apigen.NewOptInt(int(item.ServiceID)),
			EventId:   apigen.NewOptInt(int(item.EventID)),
		}
		if typ, ok := relatedItemTypes[item.GroupType]; ok {
			out.Type = apigen.NewOptRelatedItemType(typ)
		}
		if item.NetworkID != 0 {
			out.NetworkId = apigen.NewOptInt(int(item.NetworkID))
		}
		if item.StreamID != 0 {
			out.TransportStreamId = apigen.NewOptInt(int(item.StreamID))
		}
		result[i] = out
	}
	return result
}

// EventFromAPI converts a Mirakurun-compatible program received from a
// remote server to a broadcast event. The remote's STD-B10 video values
// become meanings; values outside the tables are lost, as for broadcast
// ones. Absent optional values become zero values, and Mirakurun carries no
// stream ID.
func EventFromAPI(p *apigen.Program) model.Event {
	e := model.Event{
		Key:      model.ServiceKey{NetworkID: uint16(p.NetworkId), ServiceID: uint16(p.ServiceId)},
		EventID:  uint16(p.EventId),
		FreeCA:   !p.IsFree,
		Extended: extendedFromAPI(p.Extended),
	}
	if p.StartAt != 0 {
		startAt := int64(p.StartAt)
		e.StartAt = &startAt
	}
	if p.Duration != 0 {
		duration := p.Duration
		e.DurationMS = &duration
	}
	e.Name, _ = p.Name.Get()
	e.Description, _ = p.Description.Get()
	for _, item := range p.Genres {
		lv1, _ := item.Lv1.Get()
		lv2, _ := item.Lv2.Get()
		un1, _ := item.Un1.Get()
		un2, _ := item.Un2.Get()
		e.Genres = append(e.Genres, model.Genre{Lv1: uint8(lv1), Lv2: uint8(lv2), Un1: uint8(un1), Un2: uint8(un2)})
	}
	if video, ok := p.Video.Get(); ok {
		e.Videos = []model.VideoComponent{videoFromAPI(video)}
	}
	for _, item := range p.Audios {
		componentType, _ := item.ComponentType.Get()
		tag, _ := item.ComponentTag.Get()
		main, _ := item.IsMain.Get()
		samplingRate, _ := item.SamplingRate.Get()
		audio := model.AudioComponent{
			Tag:           uint16(tag),
			ComponentType: uint8(componentType),
			Main:          main,
			SamplingHz:    int(samplingRate),
			Languages:     []string{},
		}
		for _, lang := range item.Langs {
			audio.Languages = append(audio.Languages, string(lang))
		}
		e.Audios = append(e.Audios, audio)
	}
	for _, item := range p.RelatedItems {
		related := model.RelatedEvent{}
		if typ, ok := item.Type.Get(); ok {
			for groupType, name := range relatedItemTypes {
				if name == typ {
					related.GroupType = groupType
				}
			}
		}
		networkID, _ := item.NetworkId.Get()
		streamID, _ := item.TransportStreamId.Get()
		serviceID, _ := item.ServiceId.Get()
		eventID, _ := item.EventId.Get()
		related.NetworkID, related.StreamID = uint16(networkID), uint16(streamID)
		related.ServiceID, related.EventID = uint16(serviceID), uint16(eventID)
		e.Related = append(e.Related, related)
	}
	if series, ok := p.Series.Get(); ok {
		e.Series = seriesFromAPI(&series)
	}
	return e
}

func videoFromAPI(video apigen.ProgramVideo) model.VideoComponent {
	out := model.VideoComponent{}
	if streamContent, ok := video.StreamContent.Get(); ok {
		codec, _ := isdb.VideoCodecForTSStreamContent(byte(streamContent))
		out.Codec = model.VideoCodec(codec)
	}
	if componentType, ok := video.ComponentType.Get(); ok {
		if parsed, ok := isdb.ParseVideoComponentType(byte(componentType)); ok {
			out.Resolution = model.VideoResolution(parsed.Resolution)
			out.Aspect = model.VideoAspect(parsed.Aspect)
			out.Progressive = parsed.Progressive
		}
	}
	return out
}

func seriesFromAPI(s *apigen.ProgramSeries) *model.Series {
	id, _ := s.ID.Get()
	repeat, _ := s.Repeat.Get()
	episode, _ := s.Episode.Get()
	lastEpisode, _ := s.LastEpisode.Get()
	series := &model.Series{
		ID:          id,
		Repeat:      repeat,
		Episode:     int(episode),
		LastEpisode: int(lastEpisode),
	}
	if pattern, ok := s.Pattern.Get(); ok {
		v := int(pattern)
		series.Pattern = &v
	}
	if expiresAt, ok := s.ExpiresAt.Get(); ok {
		v := int64(expiresAt)
		series.ExpiresAt = &v
	}
	series.Name, _ = s.Name.Get()
	return series
}
