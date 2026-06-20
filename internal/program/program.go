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
}

func ProgramID(networkID, serviceID, eventID uint16) int64 {
	return int64(networkID)*10000000000 + int64(serviceID)*100000 + int64(eventID)
}
