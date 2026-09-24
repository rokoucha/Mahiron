// Package model holds Mahiron's internal representation of broadcast
// information: services and events (ARIB's name for programs) decoded from
// TS or MMT control information.
//
// The types here keep 100% of the meaning shared by both systems (codecs in
// Hz, resolutions, aspects, ordered extended items) and optional fields for
// meanings that exist in only one system (MMT frame rate and transfer
// characteristics). They hold no raw encoding values (table_id,
// stream_content, descriptor bytes): a value that means different things per
// system must never become an internal convention. They also hold no
// Mahiron-managed state (ProgramID, channels, EPG gather state, logo
// presence); that state lives in the program and service packages, which
// embed these types.
package model

// ServiceKey identifies a service across transports. StreamID carries
// transport_stream_id for TS and tlv_stream_id for ISDB-S3; the
// Mirakurun-compatible API exposes it as transportStreamId in both cases.
type ServiceKey struct {
	NetworkID uint16
	StreamID  uint16
	ServiceID uint16
}

// ProgramID computes the Mahiron program ID from a service key and an event
// ID. The numbering scheme is unchanged by the model introduction.
func ProgramID(key ServiceKey, eventID uint16) int64 {
	return int64(key.NetworkID)*10000000000 + int64(key.ServiceID)*100000 + int64(eventID)
}

// MirakurunID computes the Mirakurun-compatible service ID.
func (k ServiceKey) MirakurunID() int64 {
	return int64(k.NetworkID)*100000 + int64(k.ServiceID)
}

// LogoRef points at the logo images for a service. A nil *LogoRef means the
// service carries no logo reference; the sentinel -1 scheme is not used.
type LogoRef struct {
	LogoID         uint16
	Version        uint16
	DownloadDataID uint16
	SimpleLogo     string
	HasSimpleLogo  bool
}

// Service is the broadcast-side information about a service.
type Service struct {
	Key              ServiceKey
	Name             string
	ProviderName     string
	Type             uint8
	RunningStatus    uint8
	FreeCA           bool
	EITSchedule      bool
	EITPresentFollow bool
	RemoteControlKey *uint8
	Logo             *LogoRef
}

// VideoCodec is the meaning-level video codec. It is decoded from TS
// stream_content or MMT asset_type by the session layer, never stored raw.
type VideoCodec string

const (
	VideoCodecUnknown VideoCodec = ""
	VideoCodecMPEG2   VideoCodec = "mpeg2"
	VideoCodecH264    VideoCodec = "h264"
	VideoCodecH265    VideoCodec = "h265"
)

// VideoResolution is a meaning-level resolution.
type VideoResolution string

const (
	VideoResolutionUnknown VideoResolution = ""
	VideoResolution240p    VideoResolution = "240p"
	VideoResolution480i    VideoResolution = "480i"
	VideoResolution480p    VideoResolution = "480p"
	VideoResolution720p    VideoResolution = "720p"
	VideoResolution1080i   VideoResolution = "1080i"
	VideoResolution1080p   VideoResolution = "1080p"
	VideoResolution2160p   VideoResolution = "2160p"
	VideoResolution4320p   VideoResolution = "4320p"
)

// VideoAspect is a meaning-level aspect ratio.
type VideoAspect string

const (
	VideoAspectUnknown VideoAspect = ""
	VideoAspect4x3     VideoAspect = "4:3"
	// VideoAspect16x9PanVector is 16:9 with pan-vector.
	VideoAspect16x9PanVector VideoAspect = "16:9-pan-vector"
	// VideoAspect16x9NoPanVector is 16:9 without pan-vector.
	VideoAspect16x9NoPanVector VideoAspect = "16:9-no-pan-vector"
	// VideoAspect16x9Over is 16:9 over-scan (>16:9).
	VideoAspect16x9Over VideoAspect = "16:9-over"
)

// TransferCharacteristics describes MMT-only HDR metadata. TS has no
// equivalent, so it stays optional.
type TransferCharacteristics string

const (
	TransferUnknown TransferCharacteristics = ""
	TransferSDR     TransferCharacteristics = "sdr"
	TransferHLG     TransferCharacteristics = "hlg"
	TransferPQ      TransferCharacteristics = "pq"
)

// VideoComponent is one video component of an event. component_tag is 16
// bits wide because MMT uses 16-bit tags.
type VideoComponent struct {
	Tag            uint16
	Codec          VideoCodec
	Resolution     VideoResolution
	Aspect         VideoAspect
	Progressive    bool
	FrameRateNumer int
	FrameRateDenom int
	HasFrameRate   bool
	Transfer       TransferCharacteristics
	Language       string
	Text           string
}

// AudioCodec is the meaning-level audio codec.
type AudioCodec string

const (
	AudioCodecUnknown AudioCodec = ""
	AudioCodecAAC     AudioCodec = "aac"
	AudioCodecALS     AudioCodec = "als"
)

// AudioComponent is one audio component of an event. ComponentType keeps the
// raw value because TS and MMT share the same bit layout.
type AudioComponent struct {
	Tag            uint16
	Codec          AudioCodec
	ComponentType  uint8
	StreamType     uint8
	SimulcastGroup *uint8
	Main           bool
	Quality        *uint8
	SamplingHz     int
	Languages      []string
	Text           string
}

// Genre is one content-nibble entry. TS and MMT share the structure.
type Genre struct {
	Lv1 uint8
	Lv2 uint8
	Un1 uint8
	Un2 uint8
}

// ExtendedItem is one ordered heading/text pair of an extended event.
type ExtendedItem struct {
	Name string
	Text string
}

// ExtendedBlock is one language's ordered extended-event items plus the body
// text that follows the items.
type ExtendedBlock struct {
	Language string
	Items    []ExtendedItem
	Body     string
}

// EventGroupType covers group_type 1-5. Types 4 and 5 relay to or move into
// other networks' events and therefore carry network and stream IDs.
type EventGroupType uint8

const (
	EventGroupShared               EventGroupType = 1
	EventGroupRelay                EventGroupType = 2
	EventGroupMovement             EventGroupType = 3
	EventGroupRelayOtherNetwork    EventGroupType = 4
	EventGroupMovementOtherNetwork EventGroupType = 5
)

// RelatedEvent is one event referenced by an event group.
type RelatedEvent struct {
	GroupType EventGroupType
	NetworkID uint16
	StreamID  uint16
	ServiceID uint16
	EventID   uint16
}

// ParentalRating is one country/age pair from a parental rating descriptor.
type ParentalRating struct {
	Country string
	Age     uint8
}

// Series holds series descriptor information. A nil Pattern means the
// descriptor carries no pattern.
type Series struct {
	ID          int
	Repeat      int
	Pattern     *int
	ExpiresAt   *int64
	Episode     int
	LastEpisode int
	Name        string
}

// Event is the broadcast-side information about one program (ARIB event). A
// nil StartAt or Duration means "undecided" (all bits set on the wire);
// downstream conversions decide how to render that, the model never coerces
// it to zero.
type Event struct {
	Key           ServiceKey
	EventID       uint16
	StartAt       *int64
	DurationMS    *int
	RunningStatus uint8
	FreeCA        bool
	Language      string
	Name          string
	Description   string
	Genres        []Genre
	Videos        []VideoComponent
	Audios        []AudioComponent
	Extended      []ExtendedBlock
	Related       []RelatedEvent
	Parental      []ParentalRating
	Series        *Series
}

// ScheduleUpdate is one CollectSchedule callback payload: the full set of
// currently known events for a service, whether the basic and extended
// tables are complete, and a log-only diagnosis of what is still missing.
type ScheduleUpdate struct {
	Service          ServiceKey
	Events           []Event
	BasicComplete    bool
	ExtendedComplete bool
	Diagnosis        string
}

// PresentFollowing is one service's current and next events from EIT p/f.
type PresentFollowing struct {
	Service   ServiceKey
	Present   *Event
	Following *Event
}
