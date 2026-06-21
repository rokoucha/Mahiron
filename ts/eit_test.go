package ts

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/internal/epg"
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

func TestEITJSONDescriptorCompatibility(t *testing.T) {
	descriptors := []Descriptor{
		shortEventDescriptor("jpn", aribAlnum("NEWS"), aribAlnum("WEATHER")),
		contentDescriptor(0, 1, 15, 15),
		componentDescriptor(1, 0xb3, 0x10, "jpn", aribAlnum("VIDEO")),
		audioComponentDescriptor(3, 1, 0x20, 0x11, 0x22, true, 2, 7, "jpn", "eng", aribAlnum("AUDIO")),
		extendedEventDescriptor("jpn", [][2][]byte{{aribAlnum("DETAIL"), aribAlnum("BODY")}}, nil),
		eventGroupDescriptor(4, []relatedEventSpec{{serviceID: 1024, eventID: 10}}, []relatedEventSpec{{onid: 1, tsid: 2, serviceID: 3, eventID: 4}}),
		seriesDescriptor(11, 1, 2, time.Date(2026, 7, 1, 0, 0, 0, 0, jst), 12, 13, aribAlnum("SERIES")),
	}
	got := descriptorsToJSON(descriptors)
	if len(got) != len(descriptors) {
		t.Fatalf("descriptor count = %d, want %d: %#v", len(got), len(descriptors), got)
	}
	byType := map[string]eitDescriptorJSON{}
	for _, item := range got {
		byType[item.Type] = item
	}
	if byType["ShortEvent"].EventName != "ＮＥＷＳ" || byType["ShortEvent"].Text != "ＷＥＡＴＨＥＲ" {
		t.Fatalf("ShortEvent = %#v", byType["ShortEvent"])
	}
	if n := byType["Content"].Nibbles; len(n) != 1 || n[0][0] != 0 || n[0][1] != 1 || n[0][2] != 15 || n[0][3] != 15 {
		t.Fatalf("Content = %#v", byType["Content"])
	}
	if byType["Component"].StreamContent == nil || *byType["Component"].StreamContent != 1 || *byType["Component"].ComponentType != 0xb3 || byType["Component"].LanguageCode == nil || *byType["Component"].LanguageCode != 0x6a706e {
		t.Fatalf("Component = %#v", byType["Component"])
	}
	if byType["AudioComponent"].Lang != "jpn" || byType["AudioComponent"].Lang2 != "eng" || byType["AudioComponent"].MainComponent == nil || !*byType["AudioComponent"].MainComponent {
		t.Fatalf("AudioComponent = %#v", byType["AudioComponent"])
	}
	if byType["AudioComponent"].StreamType == nil || *byType["AudioComponent"].StreamType != 0x11 || byType["AudioComponent"].SimulcastGroupTag == nil || *byType["AudioComponent"].SimulcastGroupTag != 0x22 {
		t.Fatalf("AudioComponent = %#v", byType["AudioComponent"])
	}
	if byType["AudioComponent"].ESMultiLingual == nil || !*byType["AudioComponent"].ESMultiLingual || byType["AudioComponent"].MainComponentFlag == nil || !*byType["AudioComponent"].MainComponentFlag {
		t.Fatalf("AudioComponent = %#v", byType["AudioComponent"])
	}
	if byType["AudioComponent"].QualityIndicator == nil || *byType["AudioComponent"].QualityIndicator != 2 || byType["AudioComponent"].LanguageCode == nil || *byType["AudioComponent"].LanguageCode != 0x6a706e || byType["AudioComponent"].LanguageCode2 == nil || *byType["AudioComponent"].LanguageCode2 != 0x656e67 {
		t.Fatalf("AudioComponent = %#v", byType["AudioComponent"])
	}
	if items := byType["ExtendedEvent"].Items; len(items) != 1 || items[0][0] != "ＤＥＴＡＩＬ" || items[0][1] != "ＢＯＤＹ" {
		t.Fatalf("ExtendedEvent = %#v", byType["ExtendedEvent"])
	}
	if len(byType["EventGroup"].Events) != 2 || byType["EventGroup"].Events[1].OriginalNetworkID == nil {
		t.Fatalf("EventGroup = %#v", byType["EventGroup"])
	}
	if byType["Series"].SeriesID == nil || *byType["Series"].SeriesID != 11 || byType["Series"].EpisodeNumber == nil || *byType["Series"].EpisodeNumber != 12 {
		t.Fatalf("Series = %#v", byType["Series"])
	}
}

func TestEITJSONMergesExtendedEventDescriptorsByLanguage(t *testing.T) {
	descriptors := []Descriptor{
		shortEventDescriptor("jpn", aribAlnum("NEWS"), aribAlnum("WEATHER")),
		extendedEventDescriptorWithNumbers(1, 2, "jpn", [][2][]byte{{aribAlnum("B"), aribAlnum("TWO")}}, nil),
		contentDescriptor(0, 1, 15, 15),
		extendedEventDescriptorWithNumbers(0, 0, "eng", [][2][]byte{{aribAlnum("E"), aribAlnum("ENG")}}, nil),
		extendedEventDescriptorWithNumbers(0, 2, "jpn", [][2][]byte{{aribAlnum("A"), aribAlnum("ONE")}}, nil),
		extendedEventDescriptorWithNumbers(2, 2, "jpn", [][2][]byte{{nil, aribAlnum("CONT")}, {aribAlnum("C"), aribAlnum("THREE")}}, nil),
	}

	got := descriptorsToJSON(descriptors)
	if len(got) != 4 {
		t.Fatalf("descriptor count = %d, want 4: %#v", len(got), got)
	}
	if got[0].Type != "ShortEvent" || got[1].Type != "ExtendedEvent" || got[1].Lang != "jpn" || got[2].Type != "Content" || got[3].Type != "ExtendedEvent" || got[3].Lang != "eng" {
		t.Fatalf("descriptor order = %#v", got)
	}
	items := got[1].Items
	if len(items) != 3 || items[0][0] != "Ａ" || items[0][1] != "ＯＮＥ" || items[1][0] != "Ｂ" || items[1][1] != "ＴＷＯＣＯＮＴ" || items[2][0] != "Ｃ" || items[2][1] != "ＴＨＲＥＥ" {
		t.Fatalf("merged jpn ExtendedEvent items = %#v", got[1])
	}
	if engItems := got[3].Items; len(engItems) != 1 || engItems[0][0] != "Ｅ" || engItems[0][1] != "ＥＮＧ" {
		t.Fatalf("eng ExtendedEvent = %#v", got[3])
	}
}

func TestEITJSONMergesTextOnlyExtendedEventForPrograms(t *testing.T) {
	descriptors := []Descriptor{
		extendedEventDescriptorWithNumbers(2, 2, "jpn", nil, aribAlnum("THREE")),
		extendedEventDescriptorWithNumbers(0, 2, "jpn", nil, aribAlnum("ONE")),
		extendedEventDescriptorWithNumbers(1, 2, "jpn", nil, aribAlnum("TWO")),
	}

	got := descriptorsToJSON(descriptors)
	if len(got) != 1 {
		t.Fatalf("descriptor count = %d, want 1: %#v", len(got), got)
	}
	if got[0].Text != "ＯＮＥＴＷＯＴＨＲＥＥ" {
		t.Fatalf("merged text = %q", got[0].Text)
	}
	if items := got[0].Items; len(items) != 1 || items[0][0] != "" || items[0][1] != "ＯＮＥＴＷＯＴＨＲＥＥ" {
		t.Fatalf("text-only compatibility items = %#v", got[0].Items)
	}

	data, err := json.Marshal(eitSectionJSON{
		OriginalNetworkID: 1,
		ServiceID:         2,
		Events: []eitEventJSON{{
			EventID:     3,
			Descriptors: got,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	section, err := epg.DecodeSectionJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	programs := section.Programs()
	if len(programs) != 1 || programs[0].Extended[""] != "ＯＮＥＴＷＯＴＨＲＥＥ" {
		t.Fatalf("decoded programs = %#v", programs)
	}
}

func TestEITCollectorLocalFixtureMergesSplitExtendedEvent(t *testing.T) {
	const inputPath = "testdata/local/test-gr-27.ts"
	if !fileExists(inputPath) {
		t.Skip("local TS fixture not found")
	}

	rawEvent := localFixtureEITEvent(t, inputPath, TableIDEITSStart+9, 0, 1024, 1711)
	var rawExtendedParts int
	for _, desc := range rawEvent.Descriptors {
		if desc.Tag() == DescriptorTagExtendedEvent {
			rawExtendedParts++
		}
	}
	if rawExtendedParts < 2 {
		t.Fatalf("raw ExtendedEvent parts = %d, want split descriptors", rawExtendedParts)
	}

	input, err := os.Open(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()

	var out bytes.Buffer
	if err := NewEITCollector().CollectEITS(context.Background(), input, &out); err != nil {
		t.Fatal(err)
	}
	event := localFixtureJSONEvent(t, readEITSectionsJSONL(t, bytes.NewReader(out.Bytes())), TableIDEITSStart+9, 0, 1024, 1711)
	var extended []eitDescriptorJSON
	for _, desc := range event.Descriptors {
		if desc.Type == "ExtendedEvent" {
			extended = append(extended, desc)
		}
	}
	if len(extended) != 1 {
		t.Fatalf("merged ExtendedEvent count = %d, want 1: %#v", len(extended), event.Descriptors)
	}
	items := extended[0].Items
	if len(items) != 2 || items[0][0] != "番組内容" || items[1][0] != "出演者" {
		t.Fatalf("merged ExtendedEvent items = %#v", extended[0])
	}
	if len(items[0][1]) < 300 || len(items[1][1]) == 0 {
		t.Fatalf("merged ExtendedEvent text = %#v", extended[0])
	}
}

func TestEITCollectorWritesJSONLAndFiltersTables(t *testing.T) {
	pf := buildEIT(t, TableIDEITPF0, 1024, 1, 2, 0, 0, 0, []eitEventSpec{{eventID: 1, start: time.Date(2026, 6, 21, 1, 2, 3, 0, jst), duration: time.Minute}})
	schedule := buildEIT(t, TableIDEITSStart, 1024, 1, 2, 0, 0, 0, []eitEventSpec{{eventID: 2, start: time.Date(2026, 6, 21, 2, 3, 4, 0, jst), duration: 2 * time.Minute}})
	input := append(sectionPackets(PIDEIT, pf, 0), sectionPackets(PIDEIT, schedule, 1)...)

	var pfOut bytes.Buffer
	if err := NewEITCollector().CollectEITPF(context.Background(), bytes.NewReader(input), &pfOut); err != nil {
		t.Fatal(err)
	}
	var pfSection eitSectionJSON
	if err := json.Unmarshal(bytes.TrimSpace(pfOut.Bytes()), &pfSection); err != nil {
		t.Fatal(err)
	}
	if pfSection.TableID != TableIDEITPF0 || len(pfSection.Events) != 1 || pfSection.Events[0].EventID != 1 {
		t.Fatalf("PF JSON = %#v", pfSection)
	}
	decoded, err := epg.DecodeSectionJSON(bytes.TrimSpace(pfOut.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	programs := decoded.Programs()
	if len(programs) != 1 || programs[0].EventID != 1 || programs[0].ServiceID != 1024 || programs[0].NetworkID != 2 {
		t.Fatalf("decoded programs = %#v", programs)
	}

	var scheduleOut bytes.Buffer
	if err := NewEITCollector().CollectEITS(context.Background(), bytes.NewReader(input), &scheduleOut); err != nil {
		t.Fatal(err)
	}
	var scheduleSection eitSectionJSON
	if err := json.Unmarshal(bytes.TrimSpace(scheduleOut.Bytes()), &scheduleSection); err != nil {
		t.Fatal(err)
	}
	if scheduleSection.TableID != TableIDEITSStart || len(scheduleSection.Events) != 1 || scheduleSection.Events[0].EventID != 2 {
		t.Fatalf("schedule JSON = %#v", scheduleSection)
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

func TestEITCollectorHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := NewEITCollector().CollectEITPF(ctx, bytes.NewReader(nil), io.Discard)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CollectEITPF error = %v, want context.Canceled", err)
	}
}

func TestEITCollectorLocalFixturesMatchMirakcAribEITSKeys(t *testing.T) {
	cases := []struct {
		name       string
		inputPath  string
		mirakcPath string
	}{
		{
			name:       "gr-27",
			inputPath:  "testdata/local/test-gr-27.ts",
			mirakcPath: "testdata/local/mirakc-arib-collect-eits-gr-27.json",
		},
		{
			name:       "bs-15",
			inputPath:  "testdata/local/test-bs-15.ts",
			mirakcPath: "testdata/local/mirakc-arib-collect-eits-bs-15.json",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !fileExists(tc.inputPath) || !fileExists(tc.mirakcPath) {
				t.Skip("local TS fixture or mirakc-arib EITS output fixture not found")
			}

			input, err := os.Open(tc.inputPath)
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()

			var out bytes.Buffer
			if err := NewEITCollector().CollectEITS(context.Background(), input, &out); err != nil {
				t.Fatal(err)
			}
			got := readEITSectionsJSONL(t, bytes.NewReader(out.Bytes()))

			wantFile, err := os.Open(tc.mirakcPath)
			if err != nil {
				t.Fatal(err)
			}
			defer wantFile.Close()
			want := readEITSectionsJSONL(t, wantFile)

			if len(got) == 0 {
				t.Fatal("CollectEITS produced no sections")
			}
			gotByKey := map[eitFixtureSectionKey]eitSectionJSON{}
			gotTypes := map[string]int{}
			for _, section := range got {
				gotByKey[eitFixtureKey(section)] = section
				for _, event := range section.Events {
					for _, desc := range event.Descriptors {
						gotTypes[desc.Type]++
					}
				}
			}
			for _, wantSection := range want {
				gotSection, ok := gotByKey[eitFixtureKey(wantSection)]
				if !ok {
					t.Fatalf("missing EITS section key %#v", eitFixtureKey(wantSection))
				}
				if len(gotSection.Events) != len(wantSection.Events) {
					t.Fatalf("section %#v events = %d, want %d", eitFixtureKey(wantSection), len(gotSection.Events), len(wantSection.Events))
				}
				for i, wantEvent := range wantSection.Events {
					gotEvent := gotSection.Events[i]
					if gotEvent.EventID != wantEvent.EventID || gotEvent.StartTime != wantEvent.StartTime || gotEvent.Duration != wantEvent.Duration || gotEvent.Scrambled != wantEvent.Scrambled {
						t.Fatalf("section %#v event[%d] = %#v, want %#v", eitFixtureKey(wantSection), i, gotEvent, wantEvent)
					}
					compareMirakcDescriptorCompatibility(t, eitFixtureKey(wantSection), i, gotEvent, wantEvent)
				}
			}
			for _, typ := range []string{"ShortEvent", "Component", "AudioComponent", "Content", "EventGroup", "ExtendedEvent"} {
				if gotTypes[typ] == 0 {
					t.Fatalf("descriptor type %s was not decoded; got counts %#v", typ, gotTypes)
				}
			}
		})
	}
}

type eitFixtureSectionKey struct {
	OriginalNetworkID uint16
	TransportStreamID uint16
	ServiceID         uint16
	TableID           uint8
	SectionNumber     uint8
	VersionNumber     uint8
}

func eitFixtureKey(section eitSectionJSON) eitFixtureSectionKey {
	return eitFixtureSectionKey{
		OriginalNetworkID: section.OriginalNetworkID,
		TransportStreamID: section.TransportStreamID,
		ServiceID:         section.ServiceID,
		TableID:           section.TableID,
		SectionNumber:     section.SectionNumber,
		VersionNumber:     section.VersionNumber,
	}
}

func compareMirakcDescriptorCompatibility(t *testing.T, key eitFixtureSectionKey, eventIndex int, gotEvent, wantEvent eitEventJSON) {
	t.Helper()
	for _, typ := range []string{"ShortEvent", "ExtendedEvent", "Component", "AudioComponent", "Series"} {
		got := descriptorsByType(gotEvent.Descriptors, typ)
		want := descriptorsByType(wantEvent.Descriptors, typ)
		if len(got) != len(want) {
			t.Fatalf("section %#v event[%d] %s descriptors = %d, want %d", key, eventIndex, typ, len(got), len(want))
		}
		for i := range want {
			compareMirakcDescriptorFields(t, key, eventIndex, typ, i, got[i], want[i])
		}
	}
}

func descriptorsByType(descriptors []eitDescriptorJSON, typ string) []eitDescriptorJSON {
	var out []eitDescriptorJSON
	for _, desc := range descriptors {
		if desc.Type == typ {
			out = append(out, desc)
		}
	}
	return out
}

func compareMirakcDescriptorFields(t *testing.T, key eitFixtureSectionKey, eventIndex int, typ string, descriptorIndex int, got, want eitDescriptorJSON) {
	t.Helper()
	if !intPtrEqual(got.StreamContent, want.StreamContent) ||
		!intPtrEqual(got.ComponentType, want.ComponentType) ||
		!intPtrEqual(got.ComponentTag, want.ComponentTag) ||
		!intPtrEqual(got.LanguageCode, want.LanguageCode) {
		t.Fatalf("section %#v event[%d] %s[%d] = %#v, want %#v", key, eventIndex, typ, descriptorIndex, got, want)
	}
	if typ != "AudioComponent" {
		return
	}
	if !optionalWantIntPtrEqual(got.StreamType, want.StreamType) ||
		!intPtrEqual(got.SimulcastGroupTag, want.SimulcastGroupTag) ||
		!boolPtrEqual(got.ESMultiLingual, want.ESMultiLingual) ||
		!boolPtrEqual(got.MainComponentFlag, want.MainComponentFlag) ||
		!intPtrEqual(got.QualityIndicator, want.QualityIndicator) ||
		!intPtrEqual(got.SamplingRate, want.SamplingRate) ||
		!intPtrEqual(got.LanguageCode2, want.LanguageCode2) {
		t.Fatalf("section %#v event[%d] %s[%d] = %#v, want %#v", key, eventIndex, typ, descriptorIndex, got, want)
	}
}

func intPtrEqual(a, b *int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func optionalWantIntPtrEqual(got, want *int) bool {
	if want == nil {
		return true
	}
	return intPtrEqual(got, want)
}

func boolPtrEqual(a, b *bool) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func readEITSectionsJSONL(t *testing.T, r io.Reader) []eitSectionJSON {
	t.Helper()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var sections []eitSectionJSON
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var section eitSectionJSON
		if err := json.Unmarshal(line, &section); err != nil {
			t.Fatal(err)
		}
		sections = append(sections, section)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return sections
}

func localFixtureEITEvent(t *testing.T, path string, tableID, sectionNumber byte, serviceID, eventID uint16) EITEvent {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	reader := NewPacketReader(file)
	assembler := NewSectionAssembler(PIDEIT)
	for {
		packet, err := reader.Next()
		if errors.Is(err, io.EOF) {
			t.Fatalf("EIT event not found in %s: table=%d section=%d service=%d event=%d", path, tableID, sectionNumber, serviceID, eventID)
		}
		if err != nil {
			t.Fatal(err)
		}
		if packet.PID() != PIDEIT || packet.TransportErrorIndicator() || packet.IsNull() || !packet.ValidPayloadOffset() {
			continue
		}
		sections, err := assembler.FeedAll(packet)
		if err != nil {
			t.Fatal(err)
		}
		for _, section := range sections {
			if section.TableID() != tableID {
				continue
			}
			eit, err := ParseEIT(section)
			if err != nil {
				continue
			}
			if eit.SectionNumber != sectionNumber || eit.ServiceID != serviceID {
				continue
			}
			for _, event := range eit.Events {
				if event.EventID == eventID {
					return event
				}
			}
		}
	}
}

func localFixtureJSONEvent(t *testing.T, sections []eitSectionJSON, tableID, sectionNumber byte, serviceID, eventID uint16) eitEventJSON {
	t.Helper()
	for _, section := range sections {
		if section.TableID != tableID || section.SectionNumber != sectionNumber || section.ServiceID != serviceID {
			continue
		}
		for _, event := range section.Events {
			if event.EventID == eventID {
				return event
			}
		}
	}
	t.Fatalf("EIT JSON event not found: table=%d section=%d service=%d event=%d", tableID, sectionNumber, serviceID, eventID)
	return eitEventJSON{}
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
		duration := []byte{0, 0, 0}
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
