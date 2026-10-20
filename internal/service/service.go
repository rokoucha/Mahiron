package service

import "github.com/21S1298001/mahiron/internal/model"

// Service is a stored service: the broadcast service and the state Mahiron
// keeps for it.
type Service struct {
	Id string
	model.Service
	HasLogoData bool
	ChannelType string
	ChannelId   string
	EPG         EPGStatus
}

type EPGStatus struct {
	LastAttemptAt    *int64
	LastSuccessAt    *int64
	LastError        string
	BasicUpdatedAt   *int64
	ExtendedProbeAt  *int64
	ExtendedObserved bool
	PartialAt        *int64
}

// CommonDataAnnouncement is a stored announcement of the all-receivers
// common data, with the channel it was observed on.
type CommonDataAnnouncement struct {
	OriginalNetworkID   uint16
	TransportStreamID   uint16
	ServiceID           uint16
	DownloadID          uint32
	VersionID           uint16
	ObservedChannelType string
	ObservedChannelID   string
	SeenAt              int64
}

// ItemId returns the Mirakurun-compatible service ID.
func (s *Service) ItemId() int64 {
	return s.Key.MirakurunID()
}

// Event payloads and API shapes are built in internal/mirakurun from Services.

// EPGReception records acquisition evidence independently of full completion.
type EPGReception struct {
	UpdatedAt        int64
	BasicComplete    bool
	ExtendedObserved bool
	ExtendedProbed   bool
	Partial          bool
	PreserveOutcome  bool
}
