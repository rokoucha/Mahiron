package mirakurun

import (
	"context"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/ts"
)

// ProgramFromEvent converts a decoded broadcast event into the
// Mirakurun-shaped program.Program that the program store keeps. It is a
// bridge until program.Program embeds model.Event (docs/isdb-s3.md phase
// 3-6), and it reproduces what the removed EITDescriptor path stored:
//
//   - The video is the last component, with STD-B10 stream_content and
//     component_type rebuilt from the meaning values. A codec or a
//     resolution outside the STD-B10 tables becomes 0.
//   - Undecided start times and durations become 0.
//   - Extended items of all languages merge into one map.
//   - Related events of group types 4 and 5 carry network and stream IDs
//     only for the other-network entries, and those group types have no
//     Mirakurun type name.
func ProgramFromEvent(e model.Event) *program.Program {
	p := &program.Program{
		ID:          model.ProgramID(e.Key, e.EventID),
		EventID:     e.EventID,
		ServiceID:   e.Key.ServiceID,
		NetworkID:   e.Key.NetworkID,
		IsFree:      !e.FreeCA,
		Name:        e.Name,
		Description: e.Description,
	}
	if e.StartAt != nil {
		p.StartAt = *e.StartAt
	}
	if e.DurationMS != nil {
		p.Duration = *e.DurationMS
	}
	for _, genre := range e.Genres {
		p.Genres = append(p.Genres, program.Genre{Lv1: int(genre.Lv1), Lv2: int(genre.Lv2), Un1: int(genre.Un1), Un2: int(genre.Un2)})
	}
	if len(e.Videos) > 0 {
		video := e.Videos[len(e.Videos)-1]
		componentType, _ := ts.VideoComponentTypeToRaw(string(video.Resolution), string(video.Aspect))
		p.Video = &program.Video{StreamContent: tsVideoStreamContent(video.Codec), ComponentType: int(componentType)}
	}
	for _, audio := range e.Audios {
		tag := int(audio.Tag)
		main := audio.Main
		sampling := audio.SamplingHz
		p.Audios = append(p.Audios, program.Audio{
			ComponentType: int(audio.ComponentType),
			ComponentTag:  &tag,
			IsMain:        &main,
			SamplingRate:  &sampling,
			Langs:         append([]string{}, audio.Languages...),
		})
	}
	for _, block := range e.Extended {
		if p.Extended == nil {
			p.Extended = make(map[string]string, len(block.Items))
		}
		for _, item := range block.Items {
			p.Extended[item.Name] = item.Text
		}
	}
	for _, related := range e.Related {
		item := program.RelatedItem{
			Type:      relatedItemType(related.GroupType),
			ServiceID: related.ServiceID,
			EventID:   related.EventID,
		}
		if related.NetworkID != 0 || related.StreamID != 0 {
			networkID, streamID := related.NetworkID, related.StreamID
			item.NetworkID, item.TransportStreamID = &networkID, &streamID
		}
		p.RelatedItems = append(p.RelatedItems, item)
	}
	if e.Series != nil {
		series := &program.Series{
			ID:          e.Series.ID,
			Repeat:      e.Series.Repeat,
			Pattern:     -1,
			Episode:     e.Series.Episode,
			LastEpisode: e.Series.LastEpisode,
			Name:        e.Series.Name,
		}
		if e.Series.Pattern != nil {
			series.Pattern = *e.Series.Pattern
		}
		if e.Series.ExpiresAt != nil {
			expiresAt := *e.Series.ExpiresAt
			series.ExpiresAt = &expiresAt
		}
		p.Series = series
	}
	return p
}

func tsVideoStreamContent(codec model.VideoCodec) int {
	switch codec {
	case model.VideoCodecMPEG2:
		return 0x01
	case model.VideoCodecH264:
		return 0x05
	case model.VideoCodecH265:
		return 0x09
	default:
		return 0
	}
}

func relatedItemType(groupType model.EventGroupType) program.RelatedItemType {
	switch groupType {
	case model.EventGroupShared:
		return program.RelatedItemTypeShared
	case model.EventGroupRelay:
		return program.RelatedItemTypeRelay
	case model.EventGroupMovement:
		return program.RelatedItemTypeMovement
	default:
		return ""
	}
}

// ProgramWriter stores Mirakurun-shaped programs, such as program.Manager.
type ProgramWriter interface {
	UpsertPrograms(context.Context, []*program.Program) error
}

// ProgramEventWriter stores decoded broadcast events through a program
// writer, so that sessions and EPG gathering hand over model.Event while
// the program store still keeps program.Program.
type ProgramEventWriter struct {
	programs ProgramWriter
}

func NewProgramEventWriter(programs ProgramWriter) *ProgramEventWriter {
	return &ProgramEventWriter{programs: programs}
}

func (w *ProgramEventWriter) UpsertEvents(ctx context.Context, events []model.Event) error {
	if len(events) == 0 {
		return nil
	}
	programs := make([]*program.Program, len(events))
	for i := range events {
		programs[i] = ProgramFromEvent(events[i])
	}
	return w.programs.UpsertPrograms(ctx, programs)
}
