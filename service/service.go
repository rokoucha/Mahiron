package service

type Service struct {
	Id                 string
	ServiceId          uint16
	NetworkId          uint16
	TransportStreamId  uint16
	Name               string
	Type               uint8
	RemoteControlKeyId uint8
	ChannelType        string
	ChannelId          string
}

func (s *Service) ItemId() int64 {
	return int64(s.NetworkId)*100000 + int64(s.ServiceId)
}
