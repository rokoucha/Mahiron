package databroadcast

import (
	"github.com/21S1298001/mahiron/ts"
)

type DataBroadcastEvent struct {
	Type string
	// Sequence orders notifications in one SSE connection. It is not a state
	// version: PCR notifications, for example, do not change the snapshot.
	Sequence    uint64
	Revision    uint64
	Snapshot    DataBroadcastSnapshot
	PMT         *DataBroadcastPMT
	ModuleList  *DataBroadcastModuleList
	Module      *DataBroadcastModule
	ProgramInfo *DataBroadcastProgramInfo
	CurrentTime *DataBroadcastCurrentTime
	ESEvent     *DataBroadcastESEvent
	BIT         *DataBroadcastBIT
	PCR         *DataBroadcastPCR
}

type DataBroadcastSnapshot struct {
	ServiceID   uint16
	Revision    uint64
	PMT         *DataBroadcastPMT
	Components  []DataBroadcastComponent
	ProgramInfo *DataBroadcastProgramInfo
	CurrentTime *DataBroadcastCurrentTime
	BIT         *DataBroadcastBIT
	PCR         *DataBroadcastPCR
}

type DataBroadcastPMT struct {
	ServiceID     uint16
	Version       byte
	PCRPID        uint16
	Components    []DataBroadcastComponent
	RawSectionHex string
}

type DataBroadcastComponent struct {
	ComponentTag       byte
	PID                uint16
	StreamType         byte
	DataComponentID    *uint16
	BXMLInfo           *ts.AdditionalAribBXMLInfo
	DataEventID        byte
	ReturnToEntry      *bool
	CarouselStatus     string
	CarouselDownloadID *uint32
	CarouselBlockSize  *uint16
	Modules            []DataBroadcastModule
}

type DataBroadcastModuleList struct {
	ComponentTag  byte
	DownloadID    uint32
	BlockSize     uint16
	DataEventID   byte
	ReturnToEntry *bool
	Modules       []DataBroadcastModule
}

type DataBroadcastModule struct {
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
	Metadata        *ts.DSMCCModuleMetadata
}

type DataBroadcastProgramInfo struct {
	ServiceID     uint16
	EventIDs      []uint16
	RawSectionHex string
}

type DataBroadcastCurrentTime struct {
	JSTTimeUnixMilli int64
}

type DataBroadcastESEvent struct {
	ComponentTag        byte
	DataEventID         byte
	EventMessageGroupID uint16
	Version             byte
	SectionNumber       byte
	Events              []DataBroadcastGeneralEvent
	RawSectionHex       string
}

type DataBroadcastGeneralEvent struct {
	Type                string
	EventMessageGroupID uint16
	TimeMode            byte
	TimeValueHex        string
	EventMessageType    byte
	EventMessageID      uint16
	PrivateData         []byte
	EventMessageNPT     *uint64
	NPTReference        *DataBroadcastNPTReference
}

type DataBroadcastNPTReference struct {
	PostDiscontinuityIndicator bool
	DSMContentID               byte
	STCReference               uint64
	NPTReference               uint64
	ScaleNumerator             int16
	ScaleDenominator           int16
}

type DataBroadcastPCR struct {
	PCRBase      uint64
	PCRExtension uint16
}

type DataBroadcastBIT struct {
	OriginalNetworkID uint16
	Version           byte
	Broadcasters      []DataBroadcastBroadcaster
	RawSectionHex     string
}

type DataBroadcastBroadcaster struct {
	BroadcasterID            byte
	BroadcasterName          *string
	Services                 []DataBroadcastService
	Affiliations             []byte
	AffiliationBroadcasters  []DataBroadcastAffiliatedBroadcaster
	TerrestrialBroadcasterID *uint16
}

type DataBroadcastService struct {
	ServiceID   uint16
	ServiceType byte
}
type DataBroadcastAffiliatedBroadcaster struct {
	OriginalNetworkID uint16
	BroadcasterID     byte
}

func ptr[T any](v T) *T {
	return &v
}
