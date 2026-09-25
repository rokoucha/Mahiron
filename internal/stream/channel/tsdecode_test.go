package channel

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/ts"
)

// aribASCII encodes s for ARIB descriptor text using the alphanumeric set.
func aribASCII(s string) []byte {
	out := []byte{0x0e}
	return append(out, s...)
}

func descriptor(tag byte, data ...byte) ts.Descriptor {
	return append(ts.Descriptor{tag, byte(len(data))}, data...)
}

func shortEventDesc(lang, name, text string) ts.Descriptor {
	n, t := aribASCII(name), aribASCII(text)
	data := append([]byte(lang), byte(len(n)))
	data = append(data, n...)
	data = append(data, byte(len(t)))
	data = append(data, t...)
	return descriptor(ts.DescriptorTagShortEvent, data...)
}

func extendedEventDesc(number, last int, lang string, items [][2]string, body string) ts.Descriptor {
	data := []byte{byte(number<<4 | last)}
	data = append(data, lang...)
	itemsStart := len(data)
	data = append(data, 0) // items length placeholder
	for _, item := range items {
		d, t := aribASCII(item[0]), aribASCII(item[1])
		data = append(data, byte(len(d)))
		data = append(data, d...)
		data = append(data, byte(len(t)))
		data = append(data, t...)
	}
	data[itemsStart] = byte(len(data) - itemsStart - 1)
	b := aribASCII(body)
	data = append(data, byte(len(b)))
	data = append(data, b...)
	return descriptor(ts.DescriptorTagExtendedEvent, data...)
}

func TestEventsFromShortEvent(t *testing.T) {
	start := time.Date(2026, 9, 23, 21, 0, 0, 0, time.FixedZone("JST", 9*60*60))
	eit := &ts.EIT{
		OriginalNetworkID: 4,
		TransportStreamID: 0x4010,
		ServiceID:         104,
		Events: []ts.EITEvent{{
			EventID:       123,
			StartTime:     start,
			Duration:      30 * time.Minute,
			RunningStatus: 4,
			FreeCAMode:    true,
			Descriptors:   []ts.Descriptor{shortEventDesc("jpn", "ABC", "DEF")},
		}},
	}
	events := EventsFromEIT(eit)
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	event := events[0]
	if event.Key != (model.ServiceKey{NetworkID: 4, StreamID: 0x4010, ServiceID: 104}) {
		t.Fatalf("key = %+v", event.Key)
	}
	if event.Name != "ＡＢＣ" || event.Description != "ＤＥＦ" || event.Language != "jpn" {
		t.Fatalf("short event = %+v", event)
	}
	if event.StartAt == nil || *event.StartAt != start.UnixMilli() {
		t.Fatalf("start = %+v, want %d", event.StartAt, start.UnixMilli())
	}
	if event.DurationMS == nil || *event.DurationMS != 30*60*1000 {
		t.Fatalf("duration = %+v", event.DurationMS)
	}
	if event.RunningStatus != 4 || !event.FreeCA {
		t.Fatalf("status/free = %d/%v", event.RunningStatus, event.FreeCA)
	}
}

func TestEventsFromUndecidedTimesStayNil(t *testing.T) {
	eit := &ts.EIT{Events: []ts.EITEvent{{EventID: 1}}}
	events := EventsFromEIT(eit)
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if events[0].StartAt != nil || events[0].DurationMS != nil {
		t.Fatalf("undecided times = %+v/%+v, want nil", events[0].StartAt, events[0].DurationMS)
	}
	if EventsFromEIT(nil) != nil {
		t.Fatal("nil EIT should yield nil")
	}
}

func TestEventsFromExtendedEventMerge(t *testing.T) {
	first := extendedEventDesc(0, 1, "jpn", [][2]string{{"A", "1"}}, "")
	// The continuation item carries a zero-length description on the wire,
	// so it extends the previous item's text instead of starting a new one.
	b2, t2 := aribASCII("B"), aribASCII("2")
	t3 := aribASCII("3")
	secondData := []byte{0x11, 'j', 'p', 'n'}
	itemsStart := len(secondData)
	secondData = append(secondData, 0) // items length placeholder (data[4])
	secondData = append(secondData, byte(len(b2)))
	secondData = append(secondData, b2...)
	secondData = append(secondData, byte(len(t2)))
	secondData = append(secondData, t2...)
	secondData = append(secondData, 0) // zero-length description: continuation
	secondData = append(secondData, byte(len(t3)))
	secondData = append(secondData, t3...)
	secondData = append(secondData, 0) // empty body
	secondData[itemsStart] = byte(len(secondData) - itemsStart - 2)
	second := descriptor(ts.DescriptorTagExtendedEvent, secondData...)
	eit := &ts.EIT{Events: []ts.EITEvent{{EventID: 1, Descriptors: []ts.Descriptor{first, second}}}}
	events := EventsFromEIT(eit)
	if len(events) != 1 || len(events[0].Extended) != 1 {
		t.Fatalf("extended = %+v", events)
	}
	block := events[0].Extended[0]
	if block.Language != "jpn" || len(block.Items) != 2 {
		t.Fatalf("block = %+v", block)
	}
	if block.Items[0].Name != "Ａ" || block.Items[0].Text != "１" {
		t.Fatalf("item 0 = %+v", block.Items[0])
	}
	// The empty-description item continues the previous item's text.
	if block.Items[1].Name != "Ｂ" || block.Items[1].Text != "２３" {
		t.Fatalf("item 1 = %+v", block.Items[1])
	}
}

func TestEventsFromComponentDescriptor(t *testing.T) {
	text := aribASCII("X")
	data := append([]byte{0x05, 0xB3, 0x07, 'e', 'n', 'g'}, text...)
	eit := &ts.EIT{Events: []ts.EITEvent{{EventID: 1, Descriptors: []ts.Descriptor{descriptor(ts.DescriptorTagComponent, data...)}}}}
	videos := EventsFromEIT(eit)[0].Videos
	if len(videos) != 1 {
		t.Fatalf("videos = %+v", videos)
	}
	video := videos[0]
	if video.Codec != model.VideoCodecH264 || video.Resolution != model.VideoResolution1080i ||
		video.Aspect != model.VideoAspect16x9NoPanVector || video.Progressive || video.Tag != 7 {
		t.Fatalf("video = %+v", video)
	}
	if video.Language != "eng" || video.Text != "Ｘ" {
		t.Fatalf("video lang/text = %q/%q", video.Language, video.Text)
	}

	uhd := append([]byte{0x09, 0xE4, 0x00, 'j', 'p', 'n'}, text...)
	eit.Events[0].Descriptors = []ts.Descriptor{descriptor(ts.DescriptorTagComponent, uhd...)}
	video = EventsFromEIT(eit)[0].Videos[0]
	if video.Codec != model.VideoCodecH265 || video.Resolution != model.VideoResolution1080p || !video.Progressive {
		t.Fatalf("uhd video = %+v", video)
	}
}

func TestEventsFromAudioComponentDescriptor(t *testing.T) {
	text := aribASCII("Y")
	// stream_content AAC, type 3, tag 0x10, stream_type 0x0F, simulcast 2,
	// multilingual + main + quality 1 + 48000Hz, langs jpn+eng.
	flags := byte(0x80 | 0x40 | 0x10 | 0x0E)
	data := append([]byte{0x02, 0x03, 0x10, 0x0F, 0x02, flags, 'j', 'p', 'n', 'e', 'n', 'g'}, text...)
	eit := &ts.EIT{Events: []ts.EITEvent{{EventID: 1, Descriptors: []ts.Descriptor{descriptor(ts.DescriptorTagAudioComponent, data...)}}}}
	audios := EventsFromEIT(eit)[0].Audios
	if len(audios) != 1 {
		t.Fatalf("audios = %+v", audios)
	}
	audio := audios[0]
	if audio.Codec != model.AudioCodecAAC || audio.ComponentType != 3 || audio.Tag != 0x10 ||
		audio.StreamType != 0x0F || !audio.Main || audio.SamplingHz != 48000 {
		t.Fatalf("audio = %+v", audio)
	}
	if audio.SimulcastGroup == nil || *audio.SimulcastGroup != 2 {
		t.Fatalf("simulcast = %+v", audio.SimulcastGroup)
	}
	if audio.Quality == nil || *audio.Quality != 1 {
		t.Fatalf("quality = %+v", audio.Quality)
	}
	if len(audio.Languages) != 2 || audio.Languages[0] != "jpn" || audio.Languages[1] != "eng" {
		t.Fatalf("langs = %+v", audio.Languages)
	}
}

func TestEventsFromSeriesDescriptor(t *testing.T) {
	name := aribASCII("S")
	// series_id 0x1234, repeat 2, pattern 5, expire valid, MJD 51544
	// (2000-01-01), episode 12, last 13.
	data := append([]byte{0x12, 0x34, 0x2B, 0xC9, 0x58, 0x00, 0xC0, 0x0D}, name...)
	eit := &ts.EIT{Events: []ts.EITEvent{{EventID: 1, Descriptors: []ts.Descriptor{descriptor(ts.DescriptorTagSeries, data...)}}}}
	series := EventsFromEIT(eit)[0].Series
	if series == nil {
		t.Fatal("no series decoded")
	}
	wantExpire := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.FixedZone("JST", 9*60*60)).UnixMilli()
	if series.ID != 0x1234 || series.Repeat != 2 || series.Episode != 12 || series.LastEpisode != 13 ||
		series.Name != "Ｓ" || series.Pattern == nil || *series.Pattern != 5 ||
		series.ExpiresAt == nil || *series.ExpiresAt != wantExpire {
		t.Fatalf("series = %+v", series)
	}
}

func TestEventsFromEventGroupDescriptor(t *testing.T) {
	// group_type 2 with two same-stream events.
	data := []byte{0x22, 0x00, 0x68, 0x00, 0x01, 0x00, 0x69, 0x00, 0x02}
	eit := &ts.EIT{Events: []ts.EITEvent{{EventID: 1, Descriptors: []ts.Descriptor{descriptor(ts.DescriptorTagEventGroup, data...)}}}}
	related := EventsFromEIT(eit)[0].Related
	if len(related) != 2 || related[0].GroupType != model.EventGroupRelay ||
		related[0].ServiceID != 0x68 || related[1].EventID != 2 {
		t.Fatalf("related = %+v", related)
	}

	// group_type 4 with one same-stream and one other-network event.
	data = []byte{0x41, 0x00, 0x68, 0x00, 0x01, 0x00, 0x04, 0x40, 0x10, 0x00, 0x69, 0x00, 0x02}
	eit.Events[0].Descriptors = []ts.Descriptor{descriptor(ts.DescriptorTagEventGroup, data...)}
	related = EventsFromEIT(eit)[0].Related
	if len(related) != 2 || related[1].GroupType != model.EventGroupRelayOtherNetwork ||
		related[1].NetworkID != 4 || related[1].StreamID != 0x4010 {
		t.Fatalf("other-network related = %+v", related)
	}
}

func TestEventsFromContentAndParentalDescriptors(t *testing.T) {
	eit := &ts.EIT{Events: []ts.EITEvent{{EventID: 1, Descriptors: []ts.Descriptor{
		descriptor(ts.DescriptorTagContent, 0x12, 0x34),
		descriptor(ts.DescriptorTagParentalRating, 'j', 'p', 'n', 0x0D),
	}}}}
	event := EventsFromEIT(eit)[0]
	if len(event.Genres) != 1 || event.Genres[0] != (model.Genre{Lv1: 1, Lv2: 2, Un1: 3, Un2: 4}) {
		t.Fatalf("genres = %+v", event.Genres)
	}
	if len(event.Parental) != 1 || event.Parental[0] != (model.ParentalRating{Country: "jpn", Age: 0x0D}) {
		t.Fatalf("parental = %+v", event.Parental)
	}
}

func TestLogoFromImageDeleted(t *testing.T) {
	logo, err := LogoFromImage(&ts.LogoImage{OriginalNetworkID: 4, LogoID: 1, IsDeleted: true})
	if err != nil {
		t.Fatal(err)
	}
	if !logo.Deleted || logo.Data != nil {
		t.Fatalf("deleted logo = %+v", logo)
	}
	if _, err := LogoFromImage(&ts.LogoImage{Data: []byte("bogus")}); err == nil {
		t.Fatal("bogus PNG data should fail normalization")
	}
}

// TestLogoFromImageCompletesPalette checks that a 2K logo, which relies on
// the receiver's common fixed palette, gets its PLTE and tRNS.
func TestLogoFromImageCompletesPalette(t *testing.T) {
	raw := paletteOnlyPNG()
	logo, err := LogoFromImage(&ts.LogoImage{OriginalNetworkID: 4, LogoID: 12, LogoType: 5, Data: raw})
	if err != nil {
		t.Fatal(err)
	}
	if logo.NetworkID != 4 || logo.LogoID != 12 || logo.LogoType != 5 {
		t.Fatalf("logo = %+v", logo)
	}
	for _, chunk := range []string{"PLTE", "tRNS"} {
		if !bytes.Contains(logo.Data, []byte(chunk)) {
			t.Fatalf("logo data lacks %s", chunk)
		}
	}
}

// paletteOnlyPNG builds a 1x1 palette-index PNG without PLTE.
func paletteOnlyPNG() []byte {
	chunk := func(dst []byte, typ string, data []byte) []byte {
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(data)))
		dst = append(dst, typ...)
		dst = append(dst, data...)
		return binary.BigEndian.AppendUint32(dst, crc32.ChecksumIEEE(append([]byte(typ), data...)))
	}
	out := []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a}
	ihdr := binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, 1), 1)
	out = chunk(out, "IHDR", append(ihdr, 8, 3, 0, 0, 0))
	out = chunk(out, "IDAT", []byte{0x78, 0x9c, 0x63, 0x60, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01})
	return chunk(out, "IEND", nil)
}
