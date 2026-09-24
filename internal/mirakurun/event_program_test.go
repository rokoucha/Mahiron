package mirakurun

import (
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/stream/channel"
	"github.com/21S1298001/mahiron/ts"
)

func tsDescriptor(tag byte, data ...byte) ts.Descriptor {
	return append(ts.Descriptor{tag, byte(len(data))}, data...)
}

// aribText encodes ASCII text as an ARIB string in the alphanumeric set.
func aribText(s string) []byte { return append([]byte{0x0e}, s...) }

// TestProgramFromEventRoundTripsTSDescriptors decodes TS EIT descriptors into
// the meaning-level model and converts them back into the STD-B10 values the
// program store keeps. The decoded model must lose nothing the program
// needs.
func TestProgramFromEventRoundTripsTSDescriptors(t *testing.T) {
	name := aribText("Name")
	short := append([]byte("jpn"), byte(len(name)))
	short = append(append(short, name...), 0)
	audioFlags := byte(0x80 | 0x40 | 0x10 | 0x0E) // multilingual, main, quality 1, 48 kHz
	series := append([]byte{0x12, 0x34, 0x2B, 0xC9, 0x58, 0x00, 0xC0, 0x0D}, aribText("S")...)
	jst := time.FixedZone("JST", 9*60*60)
	eit := &ts.EIT{
		TableID: 0x50, OriginalNetworkID: 4, TransportStreamID: 0x4010, ServiceID: 101,
		Events: []ts.EITEvent{{
			EventID:    9,
			StartTime:  time.Date(2026, 9, 24, 21, 0, 0, 0, jst),
			Duration:   30 * time.Minute,
			FreeCAMode: true,
			Descriptors: []ts.Descriptor{
				tsDescriptor(ts.DescriptorTagShortEvent, short...),
				tsDescriptor(ts.DescriptorTagComponent, append([]byte{0x05, 0xB3, 0x00, 'j', 'p', 'n'}, aribText("V")...)...),
				tsDescriptor(ts.DescriptorTagContent, 0x12, 0x34),
				tsDescriptor(ts.DescriptorTagAudioComponent, append([]byte{0x02, 0x03, 0x10, 0x0F, 0x02, audioFlags, 'j', 'p', 'n', 'e', 'n', 'g'}, aribText("A")...)...),
				tsDescriptor(ts.DescriptorTagSeries, series...),
				// group_type 4: one same-stream and one other-network event.
				tsDescriptor(ts.DescriptorTagEventGroup, 0x41, 0x00, 0x68, 0x00, 0x01, 0x00, 0x04, 0x40, 0x10, 0x00, 0x69, 0x00, 0x02),
			},
		}},
	}

	p := ProgramFromEvent(channel.EventsFromEIT(eit)[0])

	start := time.Date(2026, 9, 24, 21, 0, 0, 0, jst).UnixMilli()
	if p.ID != program.ProgramID(4, 101, 9) || p.StartAt != start || p.Duration != 30*60*1000 || p.IsFree || p.Name != "Ｎａｍｅ" {
		t.Fatalf("program = %+v", p)
	}
	if p.Video == nil || p.Video.StreamContent != 0x05 || p.Video.ComponentType != 0xB3 {
		t.Fatalf("video = %+v, want stream_content 0x05 and component_type 0xB3", p.Video)
	}
	if len(p.Genres) != 1 || p.Genres[0] != (program.Genre{Lv1: 1, Lv2: 2, Un1: 3, Un2: 4}) {
		t.Fatalf("genres = %+v", p.Genres)
	}
	if len(p.Audios) != 1 {
		t.Fatalf("audios = %+v", p.Audios)
	}
	audio := p.Audios[0]
	if audio.ComponentType != 3 || *audio.ComponentTag != 0x10 || !*audio.IsMain || *audio.SamplingRate != 48000 ||
		len(audio.Langs) != 2 || audio.Langs[1] != "eng" {
		t.Fatalf("audio = %+v", audio)
	}
	if p.Series == nil || p.Series.ID != 0x1234 || p.Series.Repeat != 2 || p.Series.Pattern != 5 ||
		p.Series.Episode != 12 || p.Series.LastEpisode != 13 || p.Series.ExpiresAt == nil {
		t.Fatalf("series = %+v", p.Series)
	}
	if len(p.RelatedItems) != 2 || p.RelatedItems[0].NetworkID != nil ||
		p.RelatedItems[1].NetworkID == nil || *p.RelatedItems[1].NetworkID != 4 || *p.RelatedItems[1].TransportStreamID != 0x4010 {
		t.Fatalf("related items = %+v", p.RelatedItems)
	}
}
