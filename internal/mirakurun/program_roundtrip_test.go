package mirakurun

import (
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/stream/channel"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
	"github.com/21S1298001/mahiron/ts"
)

func tsDescriptor(tag byte, data ...byte) ts.Descriptor {
	return append(ts.Descriptor{tag, byte(len(data))}, data...)
}

// aribText encodes ASCII text as an ARIB string in the alphanumeric set.
func aribText(s string) []byte { return append([]byte{0x0e}, s...) }

// TestProgramToAPIRoundTripsTSDescriptors decodes TS EIT descriptors into
// the meaning-level model and converts it into the Mirakurun program, whose
// video carries the STD-B10 values again. The decoded model must lose
// nothing the API needs.
func TestProgramToAPIRoundTripsTSDescriptors(t *testing.T) {
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

	p := ProgramToAPI(&channel.EventsFromEIT(eit)[0])

	start := time.Date(2026, 9, 24, 21, 0, 0, 0, jst).UnixMilli()
	if p.ID != apigen.ProgramId(model.ProgramID(model.ServiceKey{NetworkID: 4, ServiceID: 101}, 9)) ||
		int64(p.StartAt) != start || p.Duration != 30*60*1000 || p.IsFree || p.Name.Value != "Ｎａｍｅ" {
		t.Fatalf("program = %+v", p)
	}
	video := p.Video.Value
	if video.StreamContent.Value != 0x05 || video.ComponentType.Value != 0xB3 ||
		video.Type.Value != "h.264" || video.Resolution.Value != "1080i" {
		t.Fatalf("video = %+v, want stream_content 0x05 and component_type 0xB3", video)
	}
	if len(p.Genres) != 1 || p.Genres[0].Lv1.Value != 1 || p.Genres[0].Un2.Value != 4 {
		t.Fatalf("genres = %+v", p.Genres)
	}
	if len(p.Audios) != 1 {
		t.Fatalf("audios = %+v", p.Audios)
	}
	audio := p.Audios[0]
	if audio.ComponentType.Value != 3 || audio.ComponentTag.Value != 0x10 || !audio.IsMain.Value ||
		audio.SamplingRate.Value != 48000 || len(audio.Langs) != 2 || audio.Langs[1] != "eng" {
		t.Fatalf("audio = %+v", audio)
	}
	seriesAPI := p.Series.Value
	if seriesAPI.ID.Value != 0x1234 || seriesAPI.Repeat.Value != 2 || seriesAPI.Pattern.Value != 5 ||
		seriesAPI.Episode.Value != 12 || seriesAPI.LastEpisode.Value != 13 || !seriesAPI.ExpiresAt.Set {
		t.Fatalf("series = %+v", seriesAPI)
	}
	if len(p.RelatedItems) != 2 || p.RelatedItems[0].NetworkId.Set ||
		p.RelatedItems[1].NetworkId.Value != 4 || p.RelatedItems[1].TransportStreamId.Value != 0x4010 {
		t.Fatalf("related items = %+v", p.RelatedItems)
	}
}
