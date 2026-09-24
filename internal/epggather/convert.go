package epggather

import (
	"sort"
	"time"

	"github.com/21S1298001/mahiron/ts"
)

func EITSectionFromTS(eit *ts.EIT) *EITSection {
	if eit == nil {
		return nil
	}
	out := &EITSection{
		OriginalNetworkID:        eit.OriginalNetworkID,
		TransportStreamID:        eit.TransportStreamID,
		ServiceID:                eit.ServiceID,
		TableID:                  eit.TableID,
		LastTableID:              eit.LastTableID,
		SectionNumber:            eit.SectionNumber,
		LastSectionNumber:        eit.LastSectionNumber,
		SegmentLastSectionNumber: eit.SegmentLastSectionNumber,
		VersionNumber:            eit.VersionNumber,
		Events:                   make([]EITEvent, 0, len(eit.Events)),
	}
	for _, event := range eit.Events {
		out.Events = append(out.Events, EITEvent{
			EventID:     event.EventID,
			StartTime:   unixMilli(event.StartTime),
			Duration:    int(event.Duration / time.Millisecond),
			Scrambled:   event.FreeCAMode,
			Descriptors: descriptorsFromTS(event.Descriptors),
		})
	}
	return out
}

func unixMilli(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func descriptorsFromTS(descriptors []ts.Descriptor) []EITDescriptor {
	out := make([]EITDescriptor, 0, len(descriptors))
	extended := map[string]*extendedEventGroup{}
	for _, desc := range descriptors {
		if desc.Tag() == ts.DescriptorTagExtendedEvent {
			part, ok := parseExtendedEventDescriptorPart(desc)
			if !ok {
				continue
			}
			group, ok := extended[part.lang]
			if !ok {
				out = append(out, EITDescriptor{})
				group = &extendedEventGroup{index: len(out) - 1}
				extended[part.lang] = group
			}
			part.order = len(group.parts)
			group.parts = append(group.parts, part)
			continue
		}
		item, ok := descriptorFromTS(desc)
		if ok {
			out = append(out, item)
		}
	}
	for _, group := range extended {
		out[group.index] = mergeExtendedEventParts(group.parts)
	}
	return out
}

func descriptorFromTS(desc ts.Descriptor) (EITDescriptor, bool) {
	switch desc.Tag() {
	case ts.DescriptorTagShortEvent:
		return parseShortEventDescriptor(desc)
	case ts.DescriptorTagExtendedEvent:
		return parseExtendedEventDescriptor(desc)
	case ts.DescriptorTagComponent:
		return parseComponentDescriptor(desc)
	case ts.DescriptorTagContent:
		return parseContentDescriptor(desc)
	case ts.DescriptorTagAudioComponent:
		return parseAudioComponentDescriptor(desc)
	case ts.DescriptorTagSeries:
		return parseSeriesDescriptor(desc)
	case ts.DescriptorTagEventGroup:
		return parseEventGroupDescriptor(desc)
	default:
		return EITDescriptor{}, false
	}
}

func parseShortEventDescriptor(desc ts.Descriptor) (EITDescriptor, bool) {
	data := desc.Data()
	if len(data) < 5 {
		return EITDescriptor{}, false
	}
	lang := string(data[:3])
	nameLen := int(data[3])
	nameStart := 4
	nameEnd := nameStart + nameLen
	if nameEnd >= len(data) {
		return EITDescriptor{}, false
	}
	textLen := int(data[nameEnd])
	textStart := nameEnd + 1
	textEnd := textStart + textLen
	if textEnd > len(data) {
		return EITDescriptor{}, false
	}
	name, err := ts.DecodeARIBString(data[nameStart:nameEnd])
	if err != nil {
		return EITDescriptor{}, false
	}
	text, err := ts.DecodeARIBString(data[textStart:textEnd])
	if err != nil {
		return EITDescriptor{}, false
	}
	return EITDescriptor{Type: "ShortEvent", Lang: lang, EventName: name, Text: text}, true
}

func parseExtendedEventDescriptor(desc ts.Descriptor) (EITDescriptor, bool) {
	part, ok := parseExtendedEventDescriptorPart(desc)
	if !ok {
		return EITDescriptor{}, false
	}
	return mergeExtendedEventParts([]extendedEventDescriptorPart{part}), true
}

type extendedEventDescriptorPart struct {
	descriptorNumber     int
	lastDescriptorNumber int
	lang                 string
	textRaw              []byte
	items                []extendedEventItemPart
	order                int
}

type extendedEventItemPart struct {
	descriptionRaw []byte
	textRaw        []byte
}

type extendedEventGroup struct {
	index int
	parts []extendedEventDescriptorPart
}

func parseExtendedEventDescriptorPart(desc ts.Descriptor) (extendedEventDescriptorPart, bool) {
	data := desc.Data()
	if len(data) < 6 {
		return extendedEventDescriptorPart{}, false
	}
	descriptorNumber := int(data[0] >> 4)
	lastDescriptorNumber := int(data[0] & 0x0f)
	lang := string(data[1:4])
	itemsLen := int(data[4])
	off := 5
	itemsEnd := off + itemsLen
	if itemsEnd > len(data) {
		return extendedEventDescriptorPart{}, false
	}
	var items []extendedEventItemPart
	for off < itemsEnd {
		descLen := int(data[off])
		off++
		if off+descLen > itemsEnd {
			return extendedEventDescriptorPart{}, false
		}
		itemDescription := append([]byte(nil), data[off:off+descLen]...)
		off += descLen
		if off >= itemsEnd {
			return extendedEventDescriptorPart{}, false
		}
		itemLen := int(data[off])
		off++
		if off+itemLen > itemsEnd {
			return extendedEventDescriptorPart{}, false
		}
		itemText := append([]byte(nil), data[off:off+itemLen]...)
		off += itemLen
		items = append(items, extendedEventItemPart{descriptionRaw: itemDescription, textRaw: itemText})
	}
	if off >= len(data) {
		return extendedEventDescriptorPart{}, false
	}
	textLen := int(data[off])
	off++
	if off+textLen > len(data) {
		return extendedEventDescriptorPart{}, false
	}
	text := append([]byte(nil), data[off:off+textLen]...)
	return extendedEventDescriptorPart{
		descriptorNumber:     descriptorNumber,
		lastDescriptorNumber: lastDescriptorNumber,
		lang:                 lang,
		textRaw:              text,
		items:                items,
	}, true
}

func mergeExtendedEventParts(parts []extendedEventDescriptorPart) EITDescriptor {
	sort.SliceStable(parts, func(i, j int) bool {
		if parts[i].descriptorNumber != parts[j].descriptorNumber {
			return parts[i].descriptorNumber < parts[j].descriptorNumber
		}
		return parts[i].order < parts[j].order
	})
	var lang string
	var textRaw []byte
	var rawItems []extendedEventItemPart
	for i, part := range parts {
		if i == 0 {
			lang = part.lang
		}
		textRaw = append(textRaw, part.textRaw...)
		for _, item := range part.items {
			if len(item.descriptionRaw) == 0 && len(rawItems) > 0 {
				rawItems[len(rawItems)-1].textRaw = append(rawItems[len(rawItems)-1].textRaw, item.textRaw...)
				continue
			}
			rawItems = append(rawItems, item)
		}
	}
	text, err := ts.DecodeARIBString(textRaw)
	if err != nil {
		text = ""
	}
	items := make([][]string, 0, len(rawItems))
	for _, item := range rawItems {
		description, err := ts.DecodeARIBString(item.descriptionRaw)
		if err != nil {
			description = ""
		}
		itemText, err := ts.DecodeARIBString(item.textRaw)
		if err != nil {
			itemText = ""
		}
		items = append(items, []string{description, itemText})
	}
	if text != "" && len(items) == 0 {
		items = append(items, []string{"", text})
	}
	return EITDescriptor{Type: "ExtendedEvent", Lang: lang, Text: text, Items: items}
}

func parseContentDescriptor(desc ts.Descriptor) (EITDescriptor, bool) {
	data := desc.Data()
	if len(data)%2 != 0 {
		return EITDescriptor{}, false
	}
	nibbles := make([][]int, 0, len(data)/2)
	for i := 0; i+1 < len(data); i += 2 {
		nibbles = append(nibbles, []int{
			int(data[i] >> 4),
			int(data[i] & 0x0f),
			int(data[i+1] >> 4),
			int(data[i+1] & 0x0f),
		})
	}
	return EITDescriptor{Type: "Content", Nibbles: nibbles}, true
}

func parseComponentDescriptor(desc ts.Descriptor) (EITDescriptor, bool) {
	data := desc.Data()
	if len(data) < 6 {
		return EITDescriptor{}, false
	}
	text, err := ts.DecodeARIBString(data[6:])
	if err != nil {
		return EITDescriptor{}, false
	}
	return EITDescriptor{
		Type:          "Component",
		StreamContent: intPtr(int(data[0] & 0x0f)),
		ComponentType: intPtr(int(data[1])),
		ComponentTag:  intPtr(int(data[2])),
		Lang:          string(data[3:6]),
		Text:          text,
	}, true
}

func parseAudioComponentDescriptor(desc ts.Descriptor) (EITDescriptor, bool) {
	data := desc.Data()
	if len(data) < 9 {
		return EITDescriptor{}, false
	}
	multilingual := data[5]&0x80 != 0
	main := data[5]&0x40 != 0
	off := 9
	item := EITDescriptor{
		Type:          "AudioComponent",
		StreamContent: intPtr(int(data[0] & 0x0f)),
		ComponentType: intPtr(int(data[1])),
		ComponentTag:  intPtr(int(data[2])),
		MainComponent: boolPtr(main),
		SamplingRate:  intPtr(int((data[5] >> 1) & 0x07)),
		Lang:          string(data[6:9]),
	}
	if multilingual {
		if len(data) < 12 {
			return EITDescriptor{}, false
		}
		item.Lang2 = string(data[9:12])
		off = 12
	}
	text, err := ts.DecodeARIBString(data[off:])
	if err != nil {
		return EITDescriptor{}, false
	}
	item.Text = text
	return item, true
}

func parseSeriesDescriptor(desc ts.Descriptor) (EITDescriptor, bool) {
	data := desc.Data()
	if len(data) < 8 {
		return EITDescriptor{}, false
	}
	expireValid := data[2]&0x01 != 0
	episode := int(uint16(data[5])<<4 | uint16(data[6]>>4))
	lastEpisode := int(uint16(data[6]&0x0f)<<8 | uint16(data[7]))
	seriesName, err := ts.DecodeARIBString(data[8:])
	if err != nil {
		return EITDescriptor{}, false
	}
	item := EITDescriptor{
		Type:              "Series",
		SeriesID:          intPtr(int(uint16(data[0])<<8 | uint16(data[1]))),
		RepeatLabel:       intPtr(int(data[2] >> 4)),
		ProgramPattern:    intPtr(int((data[2] >> 1) & 0x07)),
		EpisodeNumber:     intPtr(episode),
		LastEpisodeNumber: intPtr(lastEpisode),
		SeriesName:        seriesName,
	}
	if expireValid {
		t, ok := parseMJDDate(data[3:5])
		if !ok {
			return EITDescriptor{}, false
		}
		v := t.UnixMilli()
		item.ExpireDate = &v
	}
	return item, true
}

func parseEventGroupDescriptor(desc ts.Descriptor) (EITDescriptor, bool) {
	data := desc.Data()
	if len(data) < 1 {
		return EITDescriptor{}, false
	}
	groupType := int(data[0] >> 4)
	eventCount := int(data[0] & 0x0f)
	off := 1
	events := make([]RelatedEvent, 0, eventCount)
	for i := 0; i < eventCount; i++ {
		if off+4 > len(data) {
			return EITDescriptor{}, false
		}
		events = append(events, RelatedEvent{
			ServiceID: uint16(data[off])<<8 | uint16(data[off+1]),
			EventID:   uint16(data[off+2])<<8 | uint16(data[off+3]),
		})
		off += 4
	}
	if groupType == 4 || groupType == 5 {
		for off+8 <= len(data) {
			onid := uint16(data[off])<<8 | uint16(data[off+1])
			tsid := uint16(data[off+2])<<8 | uint16(data[off+3])
			events = append(events, RelatedEvent{
				OriginalNetworkID: uint16Ptr(onid),
				TransportStreamID: uint16Ptr(tsid),
				ServiceID:         uint16(data[off+4])<<8 | uint16(data[off+5]),
				EventID:           uint16(data[off+6])<<8 | uint16(data[off+7]),
			})
			off += 8
		}
		if off != len(data) {
			return EITDescriptor{}, false
		}
	}
	return EITDescriptor{Type: "EventGroup", GroupType: intPtr(groupType), Events: events}, true
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

func intPtr(v int) *int { return &v }

func boolPtr(v bool) *bool { return &v }

func uint16Ptr(v uint16) *uint16 { return &v }
