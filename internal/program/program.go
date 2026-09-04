package program

type Program struct {
	ID        int64
	EventID   uint16
	ServiceID uint16
	NetworkID uint16
	StartAt   int64
	Duration  int
	IsFree    bool

	Name         string
	Description  string
	Genres       []Genre
	Video        *Video
	Audios       []Audio
	Extended     map[string]string
	RelatedItems []RelatedItem
	Series       *Series
}

type Genre struct {
	Lv1 int
	Lv2 int
	Un1 int
	Un2 int
}

type Video struct {
	StreamContent int
	ComponentType int
}

type Audio struct {
	ComponentType int
	ComponentTag  *int
	IsMain        *bool
	SamplingRate  *int
	Langs         []string
}

type RelatedItem struct {
	Type              RelatedItemType
	NetworkID         *uint16
	TransportStreamID *uint16
	ServiceID         uint16
	EventID           uint16
}

type RelatedItemType string

const (
	RelatedItemTypeShared   RelatedItemType = "shared"
	RelatedItemTypeRelay    RelatedItemType = "relay"
	RelatedItemTypeMovement RelatedItemType = "movement"
)

type Series struct {
	ID          int
	Repeat      int
	Pattern     int
	ExpiresAt   *int64
	Episode     int
	LastEpisode int
	Name        string
}

type Query struct {
	ID        *int64
	NetworkID *uint16
	ServiceID *uint16
	EventID   *uint16
	StartAt   *int64
	EndAt     *int64
}

func ProgramID(networkID, serviceID, eventID uint16) int64 {
	return int64(networkID)*10000000000 + int64(serviceID)*100000 + int64(eventID)
}

func (p *Program) EventData() map[string]any {
	data := map[string]any{
		"id":           p.ID,
		"eventId":      p.EventID,
		"serviceId":    p.ServiceID,
		"networkId":    p.NetworkID,
		"startAt":      p.StartAt,
		"duration":     p.Duration,
		"isFree":       p.IsFree,
		"audios":       audioListEventData(p.Audios),
		"relatedItems": relatedItemListEventData(p.RelatedItems),
	}
	// Mirakurun omits "genres" entirely for programs without genre information;
	// EPGStation reads genres[0].lv1 whenever the key exists.
	if len(p.Genres) > 0 {
		data["genres"] = genreListEventData(p.Genres)
	}
	if p.Name != "" {
		data["name"] = p.Name
	}
	if p.Description != "" {
		data["description"] = p.Description
	}
	if p.Video != nil {
		video := map[string]any{
			"streamContent": p.Video.StreamContent,
			"componentType": p.Video.ComponentType,
		}
		if videoType, ok := VideoType(p.Video.StreamContent); ok {
			video["type"] = videoType
		}
		if resolution, ok := VideoResolution(p.Video.ComponentType); ok {
			video["resolution"] = resolution
		}
		data["video"] = video
	}
	if len(p.Extended) > 0 {
		data["extended"] = p.Extended
	}
	if p.Series != nil {
		series := map[string]any{
			"id":          p.Series.ID,
			"repeat":      p.Series.Repeat,
			"pattern":     p.Series.Pattern,
			"episode":     p.Series.Episode,
			"lastEpisode": p.Series.LastEpisode,
		}
		if p.Series.ExpiresAt != nil {
			series["expiresAt"] = *p.Series.ExpiresAt
		}
		if p.Series.Name != "" {
			series["name"] = p.Series.Name
		}
		data["series"] = series
	}
	return data
}

func VideoType(streamContent int) (string, bool) {
	switch streamContent {
	case 0x1:
		return "mpeg2", true
	case 0x5:
		return "h.264", true
	case 0x9:
		return "h.265", true
	default:
		return "", false
	}
}

func VideoResolution(componentType int) (string, bool) {
	switch {
	case componentType >= 0x01 && componentType <= 0x04:
		return "480i", true
	case componentType == 0x83:
		return "4320p", true
	case componentType >= 0x91 && componentType <= 0x94:
		return "2160p", true
	case componentType >= 0xA1 && componentType <= 0xA4:
		return "480p", true
	case componentType >= 0xB1 && componentType <= 0xB4:
		return "1080i", true
	case componentType >= 0xC1 && componentType <= 0xC4:
		return "720p", true
	case componentType >= 0xD1 && componentType <= 0xD4:
		return "240p", true
	case componentType >= 0xE1 && componentType <= 0xE4:
		return "1080p", true
	default:
		return "", false
	}
}

func genreListEventData(genres []Genre) []map[string]any {
	result := make([]map[string]any, len(genres))
	for i, genre := range genres {
		result[i] = map[string]any{
			"lv1": genre.Lv1,
			"lv2": genre.Lv2,
			"un1": genre.Un1,
			"un2": genre.Un2,
		}
	}
	return result
}

func audioListEventData(audios []Audio) []map[string]any {
	result := make([]map[string]any, len(audios))
	for i, audio := range audios {
		data := map[string]any{
			"componentType": audio.ComponentType,
		}
		if audio.ComponentTag != nil {
			data["componentTag"] = *audio.ComponentTag
		}
		if audio.IsMain != nil {
			data["isMain"] = *audio.IsMain
		}
		if audio.SamplingRate != nil {
			data["samplingRate"] = *audio.SamplingRate
		}
		if len(audio.Langs) > 0 {
			data["langs"] = audio.Langs
		}
		result[i] = data
	}
	return result
}

func relatedItemListEventData(items []RelatedItem) []map[string]any {
	result := make([]map[string]any, len(items))
	for i, item := range items {
		data := map[string]any{}
		if item.Type != "" {
			data["type"] = item.Type
		}
		if item.NetworkID != nil {
			data["networkId"] = *item.NetworkID
		}
		if item.TransportStreamID != nil {
			data["transportStreamId"] = *item.TransportStreamID
		}
		if item.ServiceID != 0 {
			data["serviceId"] = item.ServiceID
		}
		if item.EventID != 0 {
			data["eventId"] = item.EventID
		}
		result[i] = data
	}
	return result
}
