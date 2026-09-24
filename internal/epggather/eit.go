package epggather

import "github.com/21S1298001/mahiron/internal/program"

type EITSection struct {
	OriginalNetworkID        uint16
	TransportStreamID        uint16
	ServiceID                uint16
	TableID                  uint8
	LastTableID              uint8
	SectionNumber            uint8
	LastSectionNumber        uint8
	SegmentLastSectionNumber uint8
	VersionNumber            uint8
	Events                   []EITEvent
}

type EITEvent struct {
	EventID     uint16
	StartTime   int64
	Duration    int
	Scrambled   bool
	Descriptors []EITDescriptor
}

type EITDescriptor struct {
	Type              string
	EventName         string
	Text              string
	StreamContent     *int
	ComponentType     *int
	ComponentTag      *int
	MainComponent     *bool
	SamplingRate      *int
	Lang              string
	Lang2             string
	Nibbles           [][]int
	Items             [][]string
	GroupType         *int
	Events            []RelatedEvent
	SeriesID          *int
	RepeatLabel       *int
	ProgramPattern    *int
	ExpireDate        *int64
	EpisodeNumber     *int
	LastEpisodeNumber *int
	SeriesName        string
}

type RelatedEvent struct {
	OriginalNetworkID *uint16
	TransportStreamID *uint16
	ServiceID         uint16
	EventID           uint16
}

func (s *EITSection) Programs() []*program.Program {
	programs := make([]*program.Program, 0, len(s.Events))
	for _, event := range s.Events {
		item := &program.Program{
			ID:        program.ProgramID(s.OriginalNetworkID, s.ServiceID, event.EventID),
			EventID:   event.EventID,
			ServiceID: s.ServiceID,
			NetworkID: s.OriginalNetworkID,
			StartAt:   event.StartTime,
			Duration:  event.Duration,
			IsFree:    !event.Scrambled,
		}
		for _, descriptor := range event.Descriptors {
			applyDescriptor(item, descriptor)
		}
		programs = append(programs, item)
	}
	return programs
}

func applyDescriptor(item *program.Program, descriptor EITDescriptor) {
	switch descriptor.Type {
	case "ShortEvent":
		item.Name = descriptor.EventName
		item.Description = descriptor.Text
	case "Content":
		for _, nibble := range descriptor.Nibbles {
			if len(nibble) < 4 {
				continue
			}
			item.Genres = append(item.Genres, program.Genre{
				Lv1: nibble[0],
				Lv2: nibble[1],
				Un1: nibble[2],
				Un2: nibble[3],
			})
		}
	case "Component":
		video := &program.Video{}
		if descriptor.StreamContent != nil {
			video.StreamContent = *descriptor.StreamContent
		}
		if descriptor.ComponentType != nil {
			video.ComponentType = *descriptor.ComponentType
		}
		item.Video = video
	case "AudioComponent":
		audio := program.Audio{}
		if descriptor.ComponentType != nil {
			audio.ComponentType = *descriptor.ComponentType
		}
		if descriptor.ComponentTag != nil {
			v := *descriptor.ComponentTag
			audio.ComponentTag = &v
		}
		if descriptor.MainComponent != nil {
			v := *descriptor.MainComponent
			audio.IsMain = &v
		}
		if descriptor.SamplingRate != nil {
			v := samplingRate(*descriptor.SamplingRate)
			audio.SamplingRate = &v
		}
		audio.Langs = descriptorLangs(descriptor)
		item.Audios = append(item.Audios, audio)
	case "ExtendedEvent":
		if item.Extended == nil {
			item.Extended = make(map[string]string)
		}
		for _, field := range descriptor.Items {
			if len(field) >= 2 {
				item.Extended[field[0]] = field[1]
			}
		}
	case "EventGroup":
		var groupType program.RelatedItemType
		if descriptor.GroupType != nil {
			switch *descriptor.GroupType {
			case 0x01:
				groupType = program.RelatedItemTypeShared
			case 0x02:
				groupType = program.RelatedItemTypeRelay
			case 0x03:
				groupType = program.RelatedItemTypeMovement
			}
		}
		for _, event := range descriptor.Events {
			item.RelatedItems = append(item.RelatedItems, program.RelatedItem{
				Type:              groupType,
				NetworkID:         event.OriginalNetworkID,
				TransportStreamID: event.TransportStreamID,
				ServiceID:         event.ServiceID,
				EventID:           event.EventID,
			})
		}
	case "Series":
		series := &program.Series{Name: descriptor.SeriesName}
		if descriptor.SeriesID != nil {
			series.ID = *descriptor.SeriesID
		}
		if descriptor.RepeatLabel != nil {
			series.Repeat = *descriptor.RepeatLabel
		}
		if descriptor.ProgramPattern != nil {
			series.Pattern = *descriptor.ProgramPattern
		}
		if descriptor.ExpireDate != nil {
			v := *descriptor.ExpireDate
			series.ExpiresAt = &v
		}
		if descriptor.EpisodeNumber != nil {
			series.Episode = *descriptor.EpisodeNumber
		}
		if descriptor.LastEpisodeNumber != nil {
			series.LastEpisode = *descriptor.LastEpisodeNumber
		}
		item.Series = series
	}
}

func descriptorLangs(descriptor EITDescriptor) []string {
	langs := make([]string, 0, 2)
	if descriptor.Lang != "" {
		langs = append(langs, descriptor.Lang)
	}
	if descriptor.Lang2 != "" {
		langs = append(langs, descriptor.Lang2)
	}
	return langs
}

func samplingRate(code int) int {
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
		return code
	}
}
