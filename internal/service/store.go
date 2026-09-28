package service

import "context"

type Store interface {
	List(ctx context.Context) ([]*Service, error)
	Count(ctx context.Context) (int, error)
	GetByID(ctx context.Context, id string) (*Service, error)
	GetByItemID(ctx context.Context, itemID int64) (*Service, error)
	GetByNetworkServiceID(ctx context.Context, networkID, serviceID uint16) (*Service, error)
	GetByChannel(ctx context.Context, channelType, channelId string) ([]*Service, error)
	GetByChannelAndID(ctx context.Context, channelType, channelId string, id string, itemID int64) (*Service, error)
	GetByTriplet(ctx context.Context, networkID, transportStreamID, serviceID uint16) (*Service, error)
	GetLogoByServiceItemID(ctx context.Context, itemID int64) ([]byte, error)
	ListCommonDataAnnouncements(ctx context.Context) ([]CommonDataAnnouncement, error)
	UpsertCommonDataAnnouncement(ctx context.Context, announcement CommonDataAnnouncement) error
	EPGSummary(ctx context.Context, staleAfter int64, now int64) (stale, failed int, lastSuccess *int64, err error)
	ReplaceChannelServices(ctx context.Context, channelType, channelId string, services []*Service) error
	PruneChannels(ctx context.Context, active []ChannelKey) error
	SetEPGAttempt(ctx context.Context, networkID, serviceID uint16, attemptedAt int64, lastError string) error
	SetEPGSuccess(ctx context.Context, networkID, serviceID uint16, succeededAt int64) error
	UpdateServiceLogoMetadata(ctx context.Context, networkID, transportStreamID, serviceID uint16, logoID, logoVersion, downloadDataID int64) (bool, error)
	DeleteLogo(ctx context.Context, networkID, transportStreamID, serviceID uint16, logoID int64, logoType int64, logoVersion int64, downloadDataID int64) error
	UpsertLogo(ctx context.Context, networkID, transportStreamID, serviceID uint16, logoID int64, logoType int64, logoVersion int64, downloadDataID int64, data []byte, updatedAt int64) error
}

type ChannelKey struct {
	Type string
	ID   string
}
