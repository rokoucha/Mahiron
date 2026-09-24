package service

type Service struct {
	Id                  string
	ServiceId           uint16
	NetworkId           uint16
	TransportStreamId   uint16
	Name                string
	Type                uint8
	EITScheduleFlag     bool
	EITPresentFollowing bool
	LogoId              *int64
	LogoVersion         *int64
	LogoDownloadDataId  *int64
	HasLogoData         bool
	RemoteControlKeyId  uint8
	ChannelType         string
	ChannelId           string
	EPG                 EPGStatus
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

func (s *Service) ItemId() int64 {
	return int64(s.NetworkId)*100000 + int64(s.ServiceId)
}

// Event payloads and API shapes are built in internal/mirakurun from Services.
