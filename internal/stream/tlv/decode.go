package tlv

import (
	"time"

	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/stream/schedule"
	"github.com/21S1298001/mahiron/mmt"
)

// scheduleSection converts one MH-EIT section for schedule.Collector.
func scheduleSection(eit *mmt.MHEIT) schedule.Section {
	return schedule.Section{
		TableID: eit.TableID,
		Header: isdb.SectionHeader{
			TableIDExtension:   eit.ServiceID,
			Version:            eit.VersionNumber,
			SectionNumber:      eit.SectionNumber,
			LastSectionNumber:  eit.LastSectionNumber,
			CurrentNext:        eit.CurrentNextIndicator,
			LastTableID:        eit.LastTableID,
			SegmentLastSection: eit.SegmentLastSectionNumber,
		},
		Service: model.ServiceKey{NetworkID: eit.OriginalNetworkID, StreamID: eit.TLVStreamID, ServiceID: eit.ServiceID},
		Events:  eventsFromMHEIT(eit),
	}
}

// eventsFromMHEIT converts one MH-EIT section to model events. The video
// codec is not in MH-EIT; the session fills it in from the MPT afterward.
func eventsFromMHEIT(eit *mmt.MHEIT) []model.Event {
	key := model.ServiceKey{NetworkID: eit.OriginalNetworkID, StreamID: eit.TLVStreamID, ServiceID: eit.ServiceID}
	events := make([]model.Event, 0, len(eit.Events))
	for _, entry := range eit.Events {
		event := model.Event{
			Key:           key,
			EventID:       entry.EventID,
			RunningStatus: entry.RunningStatus,
			FreeCA:        entry.FreeCAMode,
		}
		if !entry.StartTime.IsZero() {
			v := entry.StartTime.UnixMilli()
			event.StartAt = &v
		}
		if entry.Duration > 0 {
			v := int(entry.Duration / time.Millisecond)
			event.DurationMS = &v
		}
		applyDescriptors(&event, entry.Descriptors)
		events = append(events, event)
	}
	return events
}

func applyDescriptors(event *model.Event, descriptors []mmt.Descriptor) {
	var extended []*mmt.MHExtendedEventDescriptor
	for _, d := range descriptors {
		switch d.Tag {
		case mmt.DescriptorTagMHShortEvent:
			if short, err := mmt.ParseMHShortEventDescriptor(d); err == nil {
				event.Name = short.EventName
				event.Description = short.Text
				event.Language = short.Language
			}
		case mmt.DescriptorTagMHExtendedEvent:
			if part, err := mmt.ParseMHExtendedEventDescriptor(d); err == nil {
				extended = append(extended, part)
			}
		case mmt.DescriptorTagVideoComponent:
			if video, err := mmt.ParseVideoComponentDescriptor(d); err == nil {
				event.Videos = append(event.Videos, videoComponent(video))
			}
		case mmt.DescriptorTagMHAudioComponent:
			if audio, err := mmt.ParseMHAudioComponentDescriptor(d); err == nil {
				event.Audios = append(event.Audios, audioComponent(audio))
			}
		case mmt.DescriptorTagMHContent:
			if genres, err := mmt.ParseMHContentDescriptor(d); err == nil {
				for _, genre := range genres {
					event.Genres = append(event.Genres, model.Genre{Lv1: genre.Level1, Lv2: genre.Level2, Un1: genre.User1, Un2: genre.User2})
				}
			}
		case mmt.DescriptorTagMHSeries:
			if series, err := mmt.ParseMHSeriesDescriptor(d); err == nil {
				event.Series = seriesFromDescriptor(series)
			}
		case mmt.DescriptorTagMHEventGroup:
			if group, err := mmt.ParseMHEventGroupDescriptor(d); err == nil {
				event.Related = append(event.Related, relatedEvents(group)...)
			}
		case mmt.DescriptorTagMHParentalRating:
			if ratings, err := mmt.ParseMHParentalRatingDescriptor(d); err == nil {
				for _, rating := range ratings {
					event.Parental = append(event.Parental, model.ParentalRating{Country: rating.CountryCode, Age: rating.Rating})
				}
			}
		}
	}
	event.Extended = extendedBlocks(extended)
}

// extendedBlocks joins the extended event descriptors of each language in
// broadcast order. A text without items becomes one unnamed item, as the TS
// decoder does.
func extendedBlocks(descriptors []*mmt.MHExtendedEventDescriptor) []model.ExtendedBlock {
	var languages []string
	byLanguage := map[string][]*mmt.MHExtendedEventDescriptor{}
	for _, d := range descriptors {
		if _, ok := byLanguage[d.Language]; !ok {
			languages = append(languages, d.Language)
		}
		byLanguage[d.Language] = append(byLanguage[d.Language], d)
	}
	var blocks []model.ExtendedBlock
	for _, language := range languages {
		joined := mmt.JoinExtendedEvent(byLanguage[language])
		block := model.ExtendedBlock{Language: language, Body: joined.Text}
		for _, item := range joined.Items {
			block.Items = append(block.Items, model.ExtendedItem{Name: item.Description, Text: item.Item})
		}
		if block.Body != "" && len(block.Items) == 0 {
			block.Items = append(block.Items, model.ExtendedItem{Text: block.Body})
		}
		blocks = append(blocks, block)
	}
	return blocks
}

// videoComponent converts the video component descriptor (ARIB STD-B60,
// Tables 7-48 to 7-51).
func videoComponent(d *mmt.VideoComponentDescriptor) model.VideoComponent {
	video := model.VideoComponent{
		Tag:         d.ComponentTag,
		Resolution:  videoResolution(d.Resolution, d.Progressive),
		Progressive: d.Progressive,
		Language:    d.Language,
		Text:        d.Text,
	}
	switch d.AspectRatio {
	case 1:
		video.Aspect = model.VideoAspect4x3
	case 2:
		video.Aspect = model.VideoAspect16x9PanVector
	case 3:
		video.Aspect = model.VideoAspect16x9NoPanVector
	case 4:
		video.Aspect = model.VideoAspect16x9Over
	}
	if numer, denom, ok := frameRate(d.FrameRate); ok {
		video.FrameRateNumer, video.FrameRateDenom, video.HasFrameRate = numer, denom, true
	}
	switch d.TransferCharacteristics {
	case 1, 2, 3: // BT.709, IEC 61966-2-4 and BT.2020 are all SDR
		video.Transfer = model.TransferSDR
	case 4:
		video.Transfer = model.TransferPQ
	case 5:
		video.Transfer = model.TransferHLG
	}
	return video
}

func videoResolution(code byte, progressive bool) model.VideoResolution {
	switch code {
	case 2:
		if progressive {
			return model.VideoResolution240p
		}
	case 3:
		if progressive {
			return model.VideoResolution480p
		}
		return model.VideoResolution480i
	case 4:
		if progressive {
			return model.VideoResolution720p
		}
	case 5:
		if progressive {
			return model.VideoResolution1080p
		}
		return model.VideoResolution1080i
	case 6:
		if progressive {
			return model.VideoResolution2160p
		}
	case 7:
		if progressive {
			return model.VideoResolution4320p
		}
	}
	return model.VideoResolutionUnknown
}

func frameRate(code byte) (numer, denom int, ok bool) {
	switch code {
	case 1:
		return 15, 1, true
	case 2:
		return 24000, 1001, true
	case 3:
		return 24, 1, true
	case 4:
		return 25, 1, true
	case 5:
		return 30000, 1001, true
	case 6:
		return 30, 1, true
	case 7:
		return 50, 1, true
	case 8:
		return 60000, 1001, true
	case 9:
		return 60, 1, true
	case 10:
		return 100, 1, true
	case 11:
		return 120000, 1001, true
	case 12:
		return 120, 1, true
	}
	return 0, 0, false
}

// audioComponent converts the MH-audio component descriptor. Its
// stream_content values differ from ISDB-T/S (ARIB STD-B60, Table 7-57).
func audioComponent(d *mmt.MHAudioComponentDescriptor) model.AudioComponent {
	quality := d.QualityIndicator
	simulcast := d.SimulcastGroupTag
	audio := model.AudioComponent{
		Tag:            d.ComponentTag,
		ComponentType:  d.ComponentType,
		StreamType:     d.StreamType,
		SimulcastGroup: &simulcast,
		Main:           d.MainComponent,
		Quality:        &quality,
		SamplingHz:     d.SamplingRateHz(),
		Languages:      []string{d.Language},
		Text:           d.Text,
	}
	switch d.StreamContent {
	case 0x3:
		audio.Codec = model.AudioCodecAAC
	case 0x4:
		audio.Codec = model.AudioCodecALS
	}
	if d.ESMultiLingual {
		audio.Languages = append(audio.Languages, d.Language2)
	}
	return audio
}

func seriesFromDescriptor(d *mmt.MHSeriesDescriptor) *model.Series {
	pattern := int(d.ProgramPattern)
	series := &model.Series{
		ID:          int(d.SeriesID),
		Repeat:      int(d.RepeatLabel),
		Pattern:     &pattern,
		Episode:     int(d.EpisodeNumber),
		LastEpisode: int(d.LastEpisodeNumber),
		Name:        d.SeriesName,
	}
	if d.ExpireDateValid {
		expires := time.Date(1858, time.November, 17+int(d.ExpireDate), 0, 0, 0, 0, time.FixedZone("JST", 9*60*60)).UnixMilli()
		series.ExpiresAt = &expires
	}
	return series
}

// relatedEvents converts an MH-event group. The events of the group's own
// network carry no network or stream ID, as in ISDB-T/S.
func relatedEvents(d *mmt.MHEventGroupDescriptor) []model.RelatedEvent {
	groupType := model.EventGroupType(d.GroupType)
	var related []model.RelatedEvent
	for _, e := range d.Events {
		related = append(related, model.RelatedEvent{GroupType: groupType, ServiceID: e.ServiceID, EventID: e.EventID})
	}
	for _, e := range d.OtherNetworkEvents {
		related = append(related, model.RelatedEvent{GroupType: groupType, NetworkID: e.OriginalNetworkID, StreamID: e.TLVStreamID, ServiceID: e.ServiceID, EventID: e.EventID})
	}
	return related
}

// videoCodecForAssetType maps an MPT asset_type to the codec (ARIB STD-B60,
// Table 7-8). TLV does not imply HEVC: other types stay unknown.
func videoCodecForAssetType(assetType string) (model.VideoCodec, bool) {
	switch assetType {
	case "hev1", "hvc1":
		return model.VideoCodecH265, true
	case "avc1", "avc3":
		return model.VideoCodecH264, true
	}
	return model.VideoCodecUnknown, false
}
