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
	LastAttemptAt *int64
	LastSuccessAt *int64
	LastError     string
}

type LogoTarget struct {
	NetworkId          uint16
	ServiceId          uint16
	TransportStreamId  uint16
	ChannelType        string
	ChannelId          string
	LogoId             int64
	LogoVersion        int64
	LogoDownloadDataId int64
	IsCommonData       bool
	IsSDTTProbe        bool
}

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
