package channel

import (
	"sort"
	"time"

	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/stream/schedule"
	"github.com/21S1298001/mahiron/ts"
)

// ScheduleSection converts one TS EIT section for schedule.Collector.
func ScheduleSection(eit *ts.EIT) schedule.Section {
	return schedule.Section{
		TableID: eit.TableID,
		Header: isdb.SectionHeader{
			TableIDExtension:   eit.ServiceID,
			Version:            eit.VersionNumber,
			SectionNumber:      eit.SectionNumber,
			LastSectionNumber:  eit.LastSectionNumber,
			CurrentNext:        true,
			LastTableID:        eit.LastTableID,
			SegmentLastSection: eit.SegmentLastSectionNumber,
		},
		Service: model.ServiceKey{NetworkID: eit.OriginalNetworkID, StreamID: eit.TransportStreamID, ServiceID: eit.ServiceID},
		Events:  EventsFromEIT(eit),
	}
}

// EventsFromEIT converts one TS EIT section to model events. Descriptors
// are decoded directly into meaning values; the intermediate string-keyed
// descriptor map is gone. A zero start time or a zero duration means
// "undecided" on the wire and stays nil instead of collapsing to zero.
func EventsFromEIT(eit *ts.EIT) []model.Event {
	if eit == nil {
		return nil
	}
	key := model.ServiceKey{
		NetworkID: eit.OriginalNetworkID,
		StreamID:  eit.TransportStreamID,
		ServiceID: eit.ServiceID,
	}
	out := make([]model.Event, 0, len(eit.Events))
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
		out = append(out, event)
	}
	return out
}

func applyDescriptors(event *model.Event, descriptors []ts.Descriptor) {
	extended := make(map[string]*extendedGroup)
	var extendedOrder []string
	for _, desc := range descriptors {
		switch desc.Tag() {
		case ts.DescriptorTagShortEvent:
			if name, text, lang, ok := parseShortEvent(desc); ok {
				event.Name = name
				event.Description = text
				event.Language = lang
			}
		case ts.DescriptorTagExtendedEvent:
			part, ok := parseExtendedEventPart(desc)
			if !ok {
				continue
			}
			group, ok := extended[part.lang]
			if !ok {
				group = &extendedGroup{}
				extended[part.lang] = group
				extendedOrder = append(extendedOrder, part.lang)
			}
			group.parts = append(group.parts, part)
		case ts.DescriptorTagComponent:
			if video, ok := parseComponentDescriptor(desc); ok {
				event.Videos = append(event.Videos, video)
			}
		case ts.DescriptorTagContent:
			event.Genres = append(event.Genres, parseContentDescriptor(desc)...)
		case ts.DescriptorTagAudioComponent:
			if audio, ok := parseAudioComponentDescriptor(desc); ok {
				event.Audios = append(event.Audios, audio)
			}
		case ts.DescriptorTagSeries:
			if series, ok := parseSeriesDescriptor(desc); ok {
				event.Series = &series
			}
		case ts.DescriptorTagEventGroup:
			event.Related = append(event.Related, parseEventGroupDescriptor(desc)...)
		case ts.DescriptorTagParentalRating:
			for _, rating := range ts.ParseParentalRatings(desc) {
				event.Parental = append(event.Parental, model.ParentalRating{Country: rating.Country, Age: rating.Rating})
			}
		}
	}
	for _, lang := range extendedOrder {
		event.Extended = append(event.Extended, mergeExtendedParts(lang, extended[lang].parts))
	}
}

func parseShortEvent(desc ts.Descriptor) (name, text, lang string, ok bool) {
	data := desc.Data()
	if len(data) < 5 {
		return "", "", "", false
	}
	lang = string(data[:3])
	nameLen := int(data[3])
	nameStart := 4
	nameEnd := nameStart + nameLen
	if nameEnd >= len(data) {
		return "", "", "", false
	}
	textLen := int(data[nameEnd])
	textStart := nameEnd + 1
	textEnd := textStart + textLen
	if textEnd > len(data) {
		return "", "", "", false
	}
	name, err := ts.DecodeARIBString(data[nameStart:nameEnd])
	if err != nil {
		return "", "", "", false
	}
	text, err = ts.DecodeARIBString(data[textStart:textEnd])
	if err != nil {
		return "", "", "", false
	}
	return name, text, lang, true
}

type extendedPart struct {
	number  int
	lang    string
	textRaw []byte
	items   []extendedItemPart
}

type extendedItemPart struct {
	descriptionRaw []byte
	textRaw        []byte
}

type extendedGroup struct {
	parts []extendedPart
}

func parseExtendedEventPart(desc ts.Descriptor) (extendedPart, bool) {
	data := desc.Data()
	if len(data) < 6 {
		return extendedPart{}, false
	}
	part := extendedPart{
		number: int(data[0] >> 4),
		lang:   string(data[1:4]),
	}
	itemsLen := int(data[4])
	off := 5
	itemsEnd := off + itemsLen
	if itemsEnd > len(data) {
		return extendedPart{}, false
	}
	for off < itemsEnd {
		descLen := int(data[off])
		off++
		if off+descLen > itemsEnd {
			return extendedPart{}, false
		}
		description := append([]byte(nil), data[off:off+descLen]...)
		off += descLen
		if off >= itemsEnd {
			return extendedPart{}, false
		}
		itemLen := int(data[off])
		off++
		if off+itemLen > itemsEnd {
			return extendedPart{}, false
		}
		text := append([]byte(nil), data[off:off+itemLen]...)
		off += itemLen
		part.items = append(part.items, extendedItemPart{descriptionRaw: description, textRaw: text})
	}
	if off >= len(data) {
		return extendedPart{}, false
	}
	textLen := int(data[off])
	off++
	if off+textLen > len(data) {
		return extendedPart{}, false
	}
	part.textRaw = append([]byte(nil), data[off:off+textLen]...)
	return part, true
}

func mergeExtendedParts(lang string, parts []extendedPart) model.ExtendedBlock {
	sort.SliceStable(parts, func(i, j int) bool { return parts[i].number < parts[j].number })
	var textRaw []byte
	var rawItems []extendedItemPart
	for _, part := range parts {
		textRaw = append(textRaw, part.textRaw...)
		for _, item := range part.items {
			if len(item.descriptionRaw) == 0 && len(rawItems) > 0 {
				rawItems[len(rawItems)-1].textRaw = append(rawItems[len(rawItems)-1].textRaw, item.textRaw...)
				continue
			}
			rawItems = append(rawItems, item)
		}
	}
	body, err := ts.DecodeARIBString(textRaw)
	if err != nil {
		body = ""
	}
	block := model.ExtendedBlock{Language: lang, Body: body}
	for _, item := range rawItems {
		description, err := ts.DecodeARIBString(item.descriptionRaw)
		if err != nil {
			description = ""
		}
		text, err := ts.DecodeARIBString(item.textRaw)
		if err != nil {
			text = ""
		}
		block.Items = append(block.Items, model.ExtendedItem{Name: description, Text: text})
	}
	if body != "" && len(block.Items) == 0 {
		block.Items = append(block.Items, model.ExtendedItem{Text: body})
	}
	return block
}

func parseContentDescriptor(desc ts.Descriptor) []model.Genre {
	data := desc.Data()
	if len(data)%2 != 0 {
		return nil
	}
	out := make([]model.Genre, 0, len(data)/2)
	for i := 0; i+1 < len(data); i += 2 {
		out = append(out, model.Genre{
			Lv1: data[i] >> 4,
			Lv2: data[i] & 0x0f,
			Un1: data[i+1] >> 4,
			Un2: data[i+1] & 0x0f,
		})
	}
	return out
}

func parseComponentDescriptor(desc ts.Descriptor) (model.VideoComponent, bool) {
	data := desc.Data()
	if len(data) < 6 {
		return model.VideoComponent{}, false
	}
	text, err := ts.DecodeARIBString(data[6:])
	if err != nil {
		return model.VideoComponent{}, false
	}
	video := model.VideoComponent{
		Tag:      uint16(data[2]),
		Language: string(data[3:6]),
		Text:     text,
	}
	if codec, ok := isdb.VideoCodecForTSStreamContent(data[0] & 0x0f); ok {
		video.Codec = model.VideoCodec(codec)
	}
	if parsed, ok := isdb.ParseVideoComponentType(data[1]); ok {
		video.Resolution = model.VideoResolution(parsed.Resolution)
		video.Aspect = model.VideoAspect(parsed.Aspect)
		video.Progressive = parsed.Progressive
	}
	return video, true
}

func audioSamplingHz(code byte) int {
	switch code {
	case 1:
		return 16000
	case 2:
		return 22050
	case 3:
		return 24000
	case 5:
		return 32000
	case 6:
		return 44100
	case 7:
		return 48000
	default:
		return 0
	}
}

func parseAudioComponentDescriptor(desc ts.Descriptor) (model.AudioComponent, bool) {
	data := desc.Data()
	if len(data) < 9 {
		return model.AudioComponent{}, false
	}
	multilingual := data[5]&0x80 != 0
	audio := model.AudioComponent{
		Tag:           uint16(data[2]),
		ComponentType: data[1],
		StreamType:    data[3],
		Main:          data[5]&0x40 != 0,
		SamplingHz:    audioSamplingHz((data[5] >> 1) & 0x07),
		Languages:     []string{string(data[6:9])},
	}
	if codec, ok := isdb.AudioCodecForTSStreamContent(data[0] & 0x0f); ok {
		audio.Codec = model.AudioCodec(codec)
	}
	quality := (data[5] >> 4) & 0x03
	audio.Quality = &quality
	simulcast := data[4]
	audio.SimulcastGroup = &simulcast
	off := 9
	if multilingual {
		if len(data) < 12 {
			return model.AudioComponent{}, false
		}
		audio.Languages = append(audio.Languages, string(data[9:12]))
		off = 12
	}
	text, err := ts.DecodeARIBString(data[off:])
	if err != nil {
		return model.AudioComponent{}, false
	}
	audio.Text = text
	return audio, true
}

func parseSeriesDescriptor(desc ts.Descriptor) (model.Series, bool) {
	data := desc.Data()
	if len(data) < 8 {
		return model.Series{}, false
	}
	name, err := ts.DecodeARIBString(data[8:])
	if err != nil {
		return model.Series{}, false
	}
	series := model.Series{
		ID:          int(uint16(data[0])<<8 | uint16(data[1])),
		Repeat:      int(data[2] >> 4),
		Episode:     int(uint16(data[5])<<4 | uint16(data[6]>>4)),
		LastEpisode: int(uint16(data[6]&0x0f)<<8 | uint16(data[7])),
		Name:        name,
	}
	pattern := int((data[2] >> 1) & 0x07)
	series.Pattern = &pattern
	if data[2]&0x01 != 0 {
		if expire, ok := parseMJDDate(data[3:5]); ok {
			v := expire.UnixMilli()
			series.ExpiresAt = &v
		} else {
			return model.Series{}, false
		}
	}
	return series, true
}

func parseEventGroupDescriptor(desc ts.Descriptor) []model.RelatedEvent {
	data := desc.Data()
	if len(data) < 1 {
		return nil
	}
	groupType := model.EventGroupType(data[0] >> 4)
	eventCount := int(data[0] & 0x0f)
	off := 1
	var out []model.RelatedEvent
	for i := 0; i < eventCount; i++ {
		if off+4 > len(data) {
			return nil
		}
		out = append(out, model.RelatedEvent{
			GroupType: groupType,
			ServiceID: uint16(data[off])<<8 | uint16(data[off+1]),
			EventID:   uint16(data[off+2])<<8 | uint16(data[off+3]),
		})
		off += 4
	}
	if groupType == model.EventGroupRelayOtherNetwork || groupType == model.EventGroupMovementOtherNetwork {
		for off+8 <= len(data) {
			out = append(out, model.RelatedEvent{
				GroupType: groupType,
				NetworkID: uint16(data[off])<<8 | uint16(data[off+1]),
				StreamID:  uint16(data[off+2])<<8 | uint16(data[off+3]),
				ServiceID: uint16(data[off+4])<<8 | uint16(data[off+5]),
				EventID:   uint16(data[off+6])<<8 | uint16(data[off+7]),
			})
			off += 8
		}
		if off != len(data) {
			return nil
		}
	}
	return out
}

func parseMJDDate(b []byte) (time.Time, bool) {
	if len(b) != 2 {
		return time.Time{}, false
	}
	mjd := int(uint16(b[0])<<8 | uint16(b[1]))
	yp := int((float64(mjd) - 15078.2) / 365.25)
	mp := int((float64(mjd) - 14956.1 - float64(int(float64(yp)*365.25))) / 30.6001)
	day := mjd - 14956 - int(float64(yp)*365.25) - int(float64(mp)*30.6001)
	k := 0
	if mp == 14 || mp == 15 {
		k = 1
	}
	year := yp + k + 1900
	month := time.Month(mp - 1 - k*12)
	return time.Date(year, month, day, 0, 0, 0, 0, time.FixedZone("JST", 9*60*60)), true
}

// LogoFromImage converts one TS logo image to the model, normalizing the
// PNG bytes the way the service manager used to. Deleted logos carry no
// data.
func LogoFromImage(image *ts.LogoImage) (model.Logo, error) {
	logo := model.Logo{
		NetworkID:      image.OriginalNetworkID,
		LogoID:         image.LogoID,
		Version:        image.LogoVersion,
		DownloadDataID: image.DownloadDataID,
		LogoType:       image.LogoType,
		Deleted:        image.IsDeleted,
	}
	if image.IsDeleted {
		return logo, nil
	}
	data, err := ts.NormalizeARIBLogoPNG(image.Data)
	if err != nil {
		return model.Logo{}, err
	}
	logo.Data = data
	return logo, nil
}
