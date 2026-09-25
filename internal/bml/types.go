package bml

type Event struct {
	Type string
	// Sequence orders notifications in one SSE connection. It is not a state
	// version: PCR notifications, for example, do not change the snapshot.
	Sequence    uint64
	Revision    uint64
	Snapshot    Snapshot
	PMT         *PMT
	ModuleList  *ModuleList
	Module      *Module
	ProgramInfo *ProgramInfo
	CurrentTime *CurrentTime
	ESEvent     *ESEvent
	BIT         *BIT
	PCR         *PCR
}

type Snapshot struct {
	ServiceID   uint16
	Revision    uint64
	PMT         *PMT
	Components  []Component
	ProgramInfo *ProgramInfo
	CurrentTime *CurrentTime
	BIT         *BIT
	PCR         *PCR
}

type PMT struct {
	ServiceID     uint16
	Version       byte
	PCRPID        uint16
	Components    []Component
	RawSectionHex string
}

type Component struct {
	ComponentTag       byte
	PID                uint16
	StreamType         byte
	DataComponentID    *uint16
	BXMLInfo           *BXMLInfo
	DataEventID        byte
	ReturnToEntry      *bool
	CarouselStatus     string
	CarouselDownloadID *uint32
	CarouselBlockSize  *uint16
	Modules            []Module
}

type ModuleList struct {
	ComponentTag  byte
	DownloadID    uint32
	BlockSize     uint16
	DataEventID   byte
	ReturnToEntry *bool
	Modules       []Module
}

type Module struct {
	ComponentTag    byte
	ModuleID        uint16
	DownloadID      uint32
	Version         byte
	Size            uint32
	Info            []byte
	Complete        bool
	Status          string
	RejectionReason *string
	ReceivedBlocks  int
	TotalBlocks     int
	ETag            string
	Data            []byte
	Metadata        *ModuleMetadata
}

// BXMLInfo mirrors ts.AdditionalAribBXMLInfo without depending on the TS
// parser: the public BML types stay usable for ISDB-S3 without importing ts.
type BXMLInfo struct {
	TransmissionFormat         byte
	EntryPointFlag             bool
	EntryPointInfo             *BXMLEntryPoint
	AdditionalAribCarouselInfo *BXMLCarousel
}

type BXMLEntryPoint struct {
	AutoStartFlag      bool
	DocumentResolution byte
	UseXML             bool
	DefaultVersionFlag bool
	IndependentFlag    bool
	StyleForTVFlag     bool
	BMLMajorVersion    uint16
	BMLMinorVersion    uint16
	BXMLMajorVersion   *uint16
	BXMLMinorVersion   *uint16
}

type BXMLCarousel struct {
	DataEventID           byte
	EventSectionFlag      bool
	OnDemandRetrievalFlag bool
	FileStorableFlag      bool
	StartPriority         byte
}

// ModuleMetadata mirrors ts.DSMCCModuleMetadata without depending on the TS
// parser: standardized DII module information used to select and cache
// data-broadcast resources.
type ModuleMetadata struct {
	Type                     string
	Name                     string
	CRC32                    *uint32
	EstimatedDownloadSeconds *uint32
	CachingPriority          *byte
	ExpireMode               *byte
	ExpireData               []byte
	ActivationMode           *byte
	ActivationData           []byte
	CompressionType          *byte
	OriginalSize             *uint32
}

type ProgramInfo struct {
	ServiceID     uint16
	EventIDs      []uint16
	RawSectionHex string
}

type CurrentTime struct {
	JSTTimeUnixMilli int64
}

type ESEvent struct {
	ComponentTag        byte
	DataEventID         byte
	EventMessageGroupID uint16
	Version             byte
	SectionNumber       byte
	Events              []GeneralEvent
	RawSectionHex       string
}

type GeneralEvent struct {
	Type                string
	EventMessageGroupID uint16
	TimeMode            byte
	TimeValueHex        string
	EventMessageType    byte
	EventMessageID      uint16
	PrivateData         []byte
	EventMessageNPT     *uint64
	NPTReference        *NPTReference
}

type NPTReference struct {
	PostDiscontinuityIndicator bool
	DSMContentID               byte
	STCReference               uint64
	NPTReference               uint64
	ScaleNumerator             int16
	ScaleDenominator           int16
}

type PCR struct {
	PCRBase      uint64
	PCRExtension uint16
}

type BIT struct {
	OriginalNetworkID uint16
	Version           byte
	Broadcasters      []Broadcaster
	RawSectionHex     string
}

type Broadcaster struct {
	BroadcasterID            byte
	BroadcasterName          *string
	Services                 []Service
	Affiliations             []byte
	AffiliationBroadcasters  []AffiliatedBroadcaster
	TerrestrialBroadcasterID *uint16
}

type Service struct {
	ServiceID   uint16
	ServiceType byte
}
type AffiliatedBroadcaster struct {
	OriginalNetworkID uint16
	BroadcasterID     byte
}

func ptr[T any](v T) *T {
	return &v
}
