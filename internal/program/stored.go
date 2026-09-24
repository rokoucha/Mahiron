package program

import "github.com/21S1298001/mahiron/internal/model"

// storedEvent is the JSON the programs.event column holds: the descriptor
// details of a model.Event. The key, times, free CA flag, name and
// description have columns of their own. The format belongs to the store
// rather than to model, so that renaming a model field does not break the
// rows already stored; the 202609250001_program_event migration writes the
// same format.
type storedEvent struct {
	Language      string           `json:"language,omitempty"`
	RunningStatus uint8            `json:"runningStatus,omitempty"`
	Genres        []storedGenre    `json:"genres,omitempty"`
	Videos        []storedVideo    `json:"videos,omitempty"`
	Audios        []storedAudio    `json:"audios,omitempty"`
	Extended      []storedExtended `json:"extended,omitempty"`
	Related       []storedRelated  `json:"related,omitempty"`
	Parental      []storedParental `json:"parental,omitempty"`
	Series        *storedSeries    `json:"series,omitempty"`
}

type storedGenre struct {
	Lv1 uint8 `json:"lv1"`
	Lv2 uint8 `json:"lv2"`
	Un1 uint8 `json:"un1"`
	Un2 uint8 `json:"un2"`
}

type storedVideo struct {
	Tag            uint16 `json:"tag,omitempty"`
	Codec          string `json:"codec"`
	Resolution     string `json:"resolution"`
	Aspect         string `json:"aspect"`
	Progressive    bool   `json:"progressive"`
	FrameRateNumer int    `json:"frameRateNumer,omitempty"`
	FrameRateDenom int    `json:"frameRateDenom,omitempty"`
	HasFrameRate   bool   `json:"hasFrameRate,omitempty"`
	Transfer       string `json:"transfer,omitempty"`
	Language       string `json:"language,omitempty"`
	Text           string `json:"text,omitempty"`
}

type storedAudio struct {
	Tag            uint16   `json:"tag"`
	Codec          string   `json:"codec,omitempty"`
	ComponentType  uint8    `json:"componentType"`
	StreamType     uint8    `json:"streamType,omitempty"`
	SimulcastGroup *uint8   `json:"simulcastGroup,omitempty"`
	Main           bool     `json:"main"`
	Quality        *uint8   `json:"quality,omitempty"`
	SamplingHz     int      `json:"samplingHz"`
	Languages      []string `json:"languages"`
	Text           string   `json:"text,omitempty"`
}

type storedExtended struct {
	Language string               `json:"language,omitempty"`
	Items    []storedExtendedItem `json:"items"`
	Body     string               `json:"body,omitempty"`
}

type storedExtendedItem struct {
	Name string `json:"name"`
	Text string `json:"text"`
}

type storedRelated struct {
	GroupType uint8  `json:"groupType"`
	NetworkID uint16 `json:"networkId"`
	StreamID  uint16 `json:"streamId"`
	ServiceID uint16 `json:"serviceId"`
	EventID   uint16 `json:"eventId"`
}

type storedParental struct {
	Country string `json:"country"`
	Age     uint8  `json:"age"`
}

type storedSeries struct {
	ID          int    `json:"id"`
	Repeat      int    `json:"repeat"`
	Pattern     *int   `json:"pattern,omitempty"`
	ExpiresAt   *int64 `json:"expiresAt,omitempty"`
	Episode     int    `json:"episode"`
	LastEpisode int    `json:"lastEpisode"`
	Name        string `json:"name"`
}

func toStoredEvent(e *model.Event) storedEvent {
	out := storedEvent{Language: e.Language, RunningStatus: e.RunningStatus}
	for _, g := range e.Genres {
		out.Genres = append(out.Genres, storedGenre{Lv1: g.Lv1, Lv2: g.Lv2, Un1: g.Un1, Un2: g.Un2})
	}
	for _, v := range e.Videos {
		out.Videos = append(out.Videos, storedVideo{
			Tag: v.Tag, Codec: string(v.Codec), Resolution: string(v.Resolution), Aspect: string(v.Aspect),
			Progressive: v.Progressive, FrameRateNumer: v.FrameRateNumer, FrameRateDenom: v.FrameRateDenom,
			HasFrameRate: v.HasFrameRate, Transfer: string(v.Transfer), Language: v.Language, Text: v.Text,
		})
	}
	for _, a := range e.Audios {
		out.Audios = append(out.Audios, storedAudio{
			Tag: a.Tag, Codec: string(a.Codec), ComponentType: a.ComponentType, StreamType: a.StreamType,
			SimulcastGroup: a.SimulcastGroup, Main: a.Main, Quality: a.Quality, SamplingHz: a.SamplingHz,
			Languages: append([]string{}, a.Languages...), Text: a.Text,
		})
	}
	for _, block := range e.Extended {
		stored := storedExtended{Language: block.Language, Items: []storedExtendedItem{}, Body: block.Body}
		for _, item := range block.Items {
			stored.Items = append(stored.Items, storedExtendedItem{Name: item.Name, Text: item.Text})
		}
		out.Extended = append(out.Extended, stored)
	}
	for _, r := range e.Related {
		out.Related = append(out.Related, storedRelated{
			GroupType: uint8(r.GroupType), NetworkID: r.NetworkID, StreamID: r.StreamID, ServiceID: r.ServiceID, EventID: r.EventID,
		})
	}
	for _, p := range e.Parental {
		out.Parental = append(out.Parental, storedParental{Country: p.Country, Age: p.Age})
	}
	if s := e.Series; s != nil {
		out.Series = &storedSeries{
			ID: s.ID, Repeat: s.Repeat, Pattern: s.Pattern, ExpiresAt: s.ExpiresAt,
			Episode: s.Episode, LastEpisode: s.LastEpisode, Name: s.Name,
		}
	}
	return out
}

func (s storedEvent) applyTo(e *model.Event) {
	e.Language = s.Language
	e.RunningStatus = s.RunningStatus
	for _, g := range s.Genres {
		e.Genres = append(e.Genres, model.Genre{Lv1: g.Lv1, Lv2: g.Lv2, Un1: g.Un1, Un2: g.Un2})
	}
	for _, v := range s.Videos {
		e.Videos = append(e.Videos, model.VideoComponent{
			Tag: v.Tag, Codec: model.VideoCodec(v.Codec), Resolution: model.VideoResolution(v.Resolution),
			Aspect: model.VideoAspect(v.Aspect), Progressive: v.Progressive, FrameRateNumer: v.FrameRateNumer,
			FrameRateDenom: v.FrameRateDenom, HasFrameRate: v.HasFrameRate,
			Transfer: model.TransferCharacteristics(v.Transfer), Language: v.Language, Text: v.Text,
		})
	}
	for _, a := range s.Audios {
		e.Audios = append(e.Audios, model.AudioComponent{
			Tag: a.Tag, Codec: model.AudioCodec(a.Codec), ComponentType: a.ComponentType, StreamType: a.StreamType,
			SimulcastGroup: a.SimulcastGroup, Main: a.Main, Quality: a.Quality, SamplingHz: a.SamplingHz,
			Languages: a.Languages, Text: a.Text,
		})
	}
	for _, block := range s.Extended {
		out := model.ExtendedBlock{Language: block.Language, Body: block.Body}
		for _, item := range block.Items {
			out.Items = append(out.Items, model.ExtendedItem{Name: item.Name, Text: item.Text})
		}
		e.Extended = append(e.Extended, out)
	}
	for _, r := range s.Related {
		e.Related = append(e.Related, model.RelatedEvent{
			GroupType: model.EventGroupType(r.GroupType), NetworkID: r.NetworkID, StreamID: r.StreamID, ServiceID: r.ServiceID, EventID: r.EventID,
		})
	}
	for _, p := range s.Parental {
		e.Parental = append(e.Parental, model.ParentalRating{Country: p.Country, Age: p.Age})
	}
	if series := s.Series; series != nil {
		e.Series = &model.Series{
			ID: series.ID, Repeat: series.Repeat, Pattern: series.Pattern, ExpiresAt: series.ExpiresAt,
			Episode: series.Episode, LastEpisode: series.LastEpisode, Name: series.Name,
		}
	}
}
