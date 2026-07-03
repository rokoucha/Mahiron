package ts

// EIT section and descriptor vectors in this file exercise the service
// information contract defined by ARIB STD-B10.

import (
	"errors"
	"testing"
	"time"
)

func TestParseEITParsesHeaderEventsAndDescriptors(t *testing.T) {
	section := buildEIT(t, TableIDEITPF0, 1024, 0x1234, 0x7fe0, 2, 3, 4, []eitEventSpec{{
		eventID:     0x2345,
		start:       time.Date(2026, 6, 21, 10, 15, 30, 0, jst),
		duration:    90*time.Minute + 5*time.Second,
		scrambled:   true,
		descriptors: shortEventDescriptor("jpn", aribAlnum("NEWS"), aribAlnum("WEATHER")),
	}})

	eit, err := ParseEIT(section)
	if err != nil {
		t.Fatal(err)
	}
	if eit.ServiceID != 1024 || eit.TransportStreamID != 0x1234 || eit.OriginalNetworkID != 0x7fe0 {
		t.Fatalf("EIT ids = sid:%d tsid:%#x onid:%#x", eit.ServiceID, eit.TransportStreamID, eit.OriginalNetworkID)
	}
	if eit.SectionNumber != 2 || eit.LastSectionNumber != 3 || eit.SegmentLastSectionNumber != 4 {
		t.Fatalf("section numbers = %#v", eit)
	}
	if len(eit.Events) != 1 {
		t.Fatalf("events = %d, want 1", len(eit.Events))
	}
	event := eit.Events[0]
	if event.EventID != 0x2345 || !event.StartTime.Equal(time.Date(2026, 6, 21, 10, 15, 30, 0, jst)) {
		t.Fatalf("event = %#v", event)
	}
	if event.Duration != 90*time.Minute+5*time.Second || !event.FreeCAMode || event.RunningStatus != 4 {
		t.Fatalf("event flags/duration = %#v", event)
	}
	if len(event.Descriptors) != 1 || event.Descriptors[0].Tag() != DescriptorTagShortEvent {
		t.Fatalf("descriptors = %#v", event.Descriptors)
	}
}

func TestParseEITRejectsInvalidSectionsAndUndefinedTimes(t *testing.T) {
	section := buildEIT(t, TableIDEITPF0, 1024, 1, 2, 0, 0, 0, []eitEventSpec{{eventID: 1, undefinedStart: true, undefinedDuration: true}})
	eit, err := ParseEIT(section)
	if err != nil {
		t.Fatal(err)
	}
	if !eit.Events[0].StartTime.IsZero() || eit.Events[0].Duration != 0 {
		t.Fatalf("undefined event = %#v", eit.Events[0])
	}

	brokenCRC := append(Section(nil), section...)
	brokenCRC[len(brokenCRC)-1] ^= 0xff
	if _, err := ParseEIT(brokenCRC); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("broken CRC error = %v, want ErrInvalidSection", err)
	}

	brokenBCD := buildEIT(t, TableIDEITPF0, 1024, 1, 2, 0, 0, 0, []eitEventSpec{{eventID: 1, rawStart: []byte{0xef, 0x00, 0x2a, 0x00, 0x00}}})
	if _, err := ParseEIT(brokenBCD); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("broken BCD error = %v, want ErrInvalidSection", err)
	}
}

type eitEventSpec struct {
	eventID           uint16
	start             time.Time
	rawStart          []byte
	undefinedStart    bool
	duration          time.Duration
	undefinedDuration bool
	scrambled         bool
	descriptors       []byte
}

func buildEIT(t *testing.T, tableID byte, serviceID, tsid, onid uint16, sectionNumber, lastSectionNumber, segmentLastSectionNumber byte, events []eitEventSpec) Section {
	t.Helper()
	eventLen := 0
	for _, event := range events {
		eventLen += 12 + len(event.descriptors)
	}
	sectionLength := 11 + eventLen + 4
	s := make([]byte, 3+sectionLength)
	s[0] = tableID
	s[1] = 0xb0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[3] = byte(serviceID >> 8)
	s[4] = byte(serviceID)
	s[5] = 0xc1
	s[6] = sectionNumber
	s[7] = lastSectionNumber
	s[8] = byte(tsid >> 8)
	s[9] = byte(tsid)
	s[10] = byte(onid >> 8)
	s[11] = byte(onid)
	s[12] = segmentLastSectionNumber
	s[13] = tableID
	off := 14
	for _, event := range events {
		s[off] = byte(event.eventID >> 8)
		s[off+1] = byte(event.eventID)
		start := event.rawStart
		if event.undefinedStart {
			start = []byte{0xff, 0xff, 0xff, 0xff, 0xff}
		} else if start == nil {
			start = encodeMJDTime(event.start)
		}
		copy(s[off+2:off+7], start)
		var duration []byte
		if event.undefinedDuration {
			duration = []byte{0xff, 0xff, 0xff}
		} else {
			duration = encodeBCDDuration(event.duration)
		}
		copy(s[off+7:off+10], duration)
		s[off+10] = 0x80 | byte(len(event.descriptors)>>8)
		if event.scrambled {
			s[off+10] |= 0x10
		}
		s[off+11] = byte(len(event.descriptors))
		copy(s[off+12:], event.descriptors)
		off += 12 + len(event.descriptors)
	}
	writeCRC(s)
	return Section(s)
}

func encodeMJDTime(t time.Time) []byte {
	t = t.In(jst)
	mjd := mjdFromDate(t)
	return []byte{byte(mjd >> 8), byte(mjd), encodeBCD(t.Hour()), encodeBCD(t.Minute()), encodeBCD(t.Second())}
}

func mjdFromDate(t time.Time) int {
	y := t.Year() - 1900
	m := int(t.Month())
	d := t.Day()
	l := 0
	if m == 1 || m == 2 {
		l = 1
	}
	return 14956 + d + int(float64(y-l)*365.25) + int(float64(m+1+l*12)*30.6001)
}

func encodeBCDDuration(d time.Duration) []byte {
	total := int(d / time.Second)
	hour := total / 3600
	minute := (total / 60) % 60
	second := total % 60
	return []byte{encodeBCD(hour), encodeBCD(minute), encodeBCD(second)}
}

func encodeBCD(v int) byte {
	return byte((v/10)<<4 | (v % 10))
}

func aribAlnum(s string) []byte {
	return append([]byte{0x0e}, []byte(s)...)
}

func shortEventDescriptor(lang string, name, text []byte) Descriptor {
	data := []byte(lang)
	data = append(data, byte(len(name)))
	data = append(data, name...)
	data = append(data, byte(len(text)))
	data = append(data, text...)
	return descriptor(DescriptorTagShortEvent, data)
}

func contentDescriptor(n1, n2, u1, u2 byte) Descriptor {
	return descriptor(DescriptorTagContent, []byte{n1<<4 | n2, u1<<4 | u2})
}

func componentDescriptor(streamContent, componentType, componentTag byte, lang string, text []byte) Descriptor {
	data := []byte{0xf0 | streamContent, componentType, componentTag}
	data = append(data, []byte(lang)...)
	data = append(data, text...)
	return descriptor(DescriptorTagComponent, data)
}

func audioComponentDescriptor(streamContent, componentType, componentTag, streamType, simulcastGroupTag byte, main bool, qualityIndicator, samplingRate byte, lang, lang2 string, text []byte) Descriptor {
	flags := byte((qualityIndicator&0x03)<<4 | (samplingRate&0x07)<<1)
	if lang2 != "" {
		flags |= 0x80
	}
	if main {
		flags |= 0x40
	}
	data := []byte{0xf0 | streamContent, componentType, componentTag, streamType, simulcastGroupTag, flags}
	data = append(data, []byte(lang)...)
	if lang2 != "" {
		data = append(data, []byte(lang2)...)
	}
	data = append(data, text...)
	return descriptor(DescriptorTagAudioComponent, data)
}

func extendedEventDescriptor(lang string, items [][2][]byte, text []byte) Descriptor {
	return extendedEventDescriptorWithNumbers(0, 0, lang, items, text)
}

func extendedEventDescriptorWithNumbers(descriptorNumber, lastDescriptorNumber int, lang string, items [][2][]byte, text []byte) Descriptor {
	data := []byte{byte(descriptorNumber&0x0f)<<4 | byte(lastDescriptorNumber&0x0f)}
	data = append(data, []byte(lang)...)
	var itemsData []byte
	for _, item := range items {
		itemsData = append(itemsData, byte(len(item[0])))
		itemsData = append(itemsData, item[0]...)
		itemsData = append(itemsData, byte(len(item[1])))
		itemsData = append(itemsData, item[1]...)
	}
	data = append(data, byte(len(itemsData)))
	data = append(data, itemsData...)
	data = append(data, byte(len(text)))
	data = append(data, text...)
	return descriptor(DescriptorTagExtendedEvent, data)
}

type relatedEventSpec struct {
	onid      uint16
	tsid      uint16
	serviceID uint16
	eventID   uint16
}

func eventGroupDescriptor(groupType byte, local, external []relatedEventSpec) Descriptor {
	data := []byte{groupType<<4 | byte(len(local))}
	for _, event := range local {
		data = append(data, byte(event.serviceID>>8), byte(event.serviceID), byte(event.eventID>>8), byte(event.eventID))
	}
	for _, event := range external {
		data = append(data, byte(event.onid>>8), byte(event.onid), byte(event.tsid>>8), byte(event.tsid), byte(event.serviceID>>8), byte(event.serviceID), byte(event.eventID>>8), byte(event.eventID))
	}
	return descriptor(DescriptorTagEventGroup, data)
}

func seriesDescriptor(seriesID uint16, repeatLabel, pattern byte, expire time.Time, episode, lastEpisode int, name []byte) Descriptor {
	data := []byte{byte(seriesID >> 8), byte(seriesID), repeatLabel<<4 | (pattern&0x07)<<1 | 0x01}
	mjd := mjdFromDate(expire)
	data = append(data, byte(mjd>>8), byte(mjd))
	data = append(data, byte(episode>>4), byte((episode&0x0f)<<4|((lastEpisode>>8)&0x0f)), byte(lastEpisode))
	data = append(data, name...)
	return descriptor(DescriptorTagSeries, data)
}

func descriptor(tag byte, data []byte) Descriptor {
	out := []byte{tag, byte(len(data))}
	out = append(out, data...)
	return Descriptor(out)
}
