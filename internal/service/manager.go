package service

import (
	"context"
	"reflect"
	"strconv"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/model"
)

const (
	eventTypeCreate = "create"
	eventTypeUpdate = "update"
	eventTypeRemove = "remove"
)

type eventPublisher interface {
	PublishServiceEvent(typ string, svc *Service, channel *config.ChannelConfig)
}

type Manager struct {
	store    Store
	channels config.ChannelsConfig
	events   eventPublisher
}

func NewManager(store Store, channels config.ChannelsConfig, events ...eventPublisher) *Manager {
	var publisher eventPublisher
	if len(events) > 0 {
		publisher = events[0]
	}
	return &Manager{
		store:    store,
		channels: channels,
		events:   publisher,
	}
}

func (s *Manager) CountServices(ctx context.Context) (int, error) {
	return s.store.Count(ctx)
}

func (s *Manager) GetServices(ctx context.Context) ([]*Service, error) {
	services, err := s.store.List(ctx)
	if err != nil {
		return nil, err
	}
	return s.orderServices(services), nil
}

func (s *Manager) SetEPGAttempt(ctx context.Context, networkID, serviceID uint16, attemptedAt int64, lastError string) error {
	if err := s.store.SetEPGAttempt(ctx, networkID, serviceID, attemptedAt, lastError); err != nil {
		return err
	}
	s.publishServiceByKey(ctx, eventTypeUpdate, networkID, serviceID)
	return nil
}

func (s *Manager) SetEPGSuccess(ctx context.Context, networkID, serviceID uint16, succeededAt int64) error {
	if err := s.store.SetEPGSuccess(ctx, networkID, serviceID, succeededAt); err != nil {
		return err
	}
	s.publishServiceByKey(ctx, eventTypeUpdate, networkID, serviceID)
	return nil
}

func (s *Manager) EPGSummary(ctx context.Context, staleAfter int64, now int64) (stale, failed int, lastSuccess *int64, err error) {
	return s.store.EPGSummary(ctx, staleAfter, now)
}

func (s *Manager) ReconcileChannels(ctx context.Context) error {
	active := make([]ChannelKey, 0, len(s.channels))
	for _, channel := range s.channels {
		if !config.IsChannelDisabled(channel) {
			active = append(active, ChannelKey{Type: channel.Type, ID: channel.Channel})
		}
	}
	removed, err := s.prunedServices(ctx, active)
	if err != nil {
		return err
	}
	if err := s.store.PruneChannels(ctx, active); err != nil {
		return err
	}
	for _, svc := range removed {
		s.publishService(eventTypeRemove, svc)
	}
	return nil
}

func (s *Manager) GetServiceById(ctx context.Context, id string) (*Service, error) {
	// Try exact string ID match first
	svc, err := s.store.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if svc != nil {
		return svc, nil
	}

	// Fall back to ItemId() match
	parsedId, parseErr := strconv.ParseInt(id, 10, 64)
	if parseErr != nil {
		return nil, nil
	}
	return s.store.GetByItemID(ctx, parsedId)
}

func (s *Manager) GetServiceByItemID(ctx context.Context, itemID int64) (*Service, error) {
	return s.store.GetByItemID(ctx, itemID)
}

func (s *Manager) GetChannels() config.ChannelsConfig {
	channels := make(config.ChannelsConfig, 0, len(s.channels))
	for _, channel := range s.channels {
		if config.IsChannelDisabled(channel) {
			continue
		}
		channels = append(channels, channel)
	}
	return channels
}

func (s *Manager) GetChannel(channelType string, channelId string) *config.ChannelConfig {
	for i := range s.channels {
		if s.channels[i].Type == channelType && s.channels[i].Channel == channelId && !config.IsChannelDisabled(s.channels[i]) {
			channel := s.channels[i]
			return &channel
		}
	}
	return nil
}

func (s *Manager) GetServicesByChannel(ctx context.Context, channelType string, channelId string) ([]*Service, error) {
	services, err := s.store.GetByChannel(ctx, channelType, channelId)
	if err != nil {
		return nil, err
	}
	return s.orderServices(services), nil
}

// GetServicesGroupedByChannel fetches every service in a single query and
// groups the results by channel, so a caller listing many channels does not
// issue one GetServicesByChannel query per channel.
func (s *Manager) GetServicesGroupedByChannel(ctx context.Context) (map[ChannelKey][]*Service, error) {
	services, err := s.store.List(ctx)
	if err != nil {
		return nil, err
	}
	grouped := make(map[ChannelKey][]*Service, len(s.channels))
	for _, svc := range services {
		key := ChannelKey{Type: svc.ChannelType, ID: svc.ChannelId}
		grouped[key] = append(grouped[key], svc)
	}
	for key, list := range grouped {
		grouped[key] = s.orderServices(list)
	}
	return grouped, nil
}

func (s *Manager) ReplaceChannelServices(ctx context.Context, channelType, channelId string, services []*Service) error {
	beforeList, err := s.store.GetByChannel(ctx, channelType, channelId)
	if err != nil {
		return err
	}
	before := make(map[string]*Service, len(beforeList))
	for _, svc := range beforeList {
		before[svc.Id] = svc
	}
	if err := s.store.ReplaceChannelServices(ctx, channelType, channelId, services); err != nil {
		return err
	}
	afterList, err := s.store.GetByChannel(ctx, channelType, channelId)
	if err != nil {
		return err
	}
	after := make(map[string]*Service, len(afterList))
	for _, svc := range afterList {
		after[svc.Id] = svc
	}
	for _, svc := range services {
		if svc == nil {
			continue
		}
		existing, ok := before[svc.Id]
		delete(before, svc.Id)
		current := after[svc.Id]
		if current == nil {
			current = svc
		}
		switch {
		case !ok:
			s.publishService(eventTypeCreate, current)
		case !sameServiceCore(existing, current):
			s.publishService(eventTypeUpdate, current)
		}
	}
	for _, svc := range before {
		s.publishService(eventTypeRemove, svc)
	}
	return nil
}

func (s *Manager) GetServiceByChannelAndId(ctx context.Context, channelType string, channelId string, id string) (*Service, error) {
	parsedId, parseErr := strconv.ParseInt(id, 10, 64)
	if parseErr != nil {
		parsedId = 0
	}
	return s.store.GetByChannelAndID(ctx, channelType, channelId, id, parsedId)
}

func (s *Manager) GetLogoByServiceItemID(ctx context.Context, itemID int64) ([]byte, error) {
	return s.store.GetLogoByServiceItemID(ctx, itemID)
}

// CommonDataAnnouncements lists the observed announcements of the
// all-receivers common data, the most recently seen first.
func (s *Manager) CommonDataAnnouncements(ctx context.Context) ([]model.CommonDataAnnouncement, error) {
	stored, err := s.store.ListCommonDataAnnouncements(ctx)
	if err != nil {
		return nil, err
	}
	announcements := make([]model.CommonDataAnnouncement, 0, len(stored))
	for _, announcement := range stored {
		announcements = append(announcements, model.CommonDataAnnouncement{
			Service:    model.ServiceKey{NetworkID: announcement.OriginalNetworkID, StreamID: announcement.TransportStreamID, ServiceID: announcement.ServiceID},
			DownloadID: announcement.DownloadID,
			VersionID:  announcement.VersionID,
		})
	}
	return announcements, nil
}

func (s *Manager) UpsertLogo(ctx context.Context, networkID, transportStreamID, serviceID uint16, logoID int64, logoType int64, logoVersion int64, downloadDataID int64, data []byte, updatedAt int64) error {
	if err := s.store.UpsertLogo(ctx, networkID, transportStreamID, serviceID, logoID, logoType, logoVersion, downloadDataID, data, updatedAt); err != nil {
		return err
	}
	s.publishServiceByKey(ctx, eventTypeUpdate, networkID, serviceID)
	return nil
}

func (s *Manager) DeleteLogo(ctx context.Context, networkID, transportStreamID, serviceID uint16, logoID int64, logoType int64, logoVersion int64, downloadDataID int64) error {
	if err := s.store.DeleteLogo(ctx, networkID, transportStreamID, serviceID, logoID, logoType, logoVersion, downloadDataID); err != nil {
		return err
	}
	s.publishServiceByKey(ctx, eventTypeUpdate, networkID, serviceID)
	return nil
}

// UpsertLogoImage stores a broadcast logo, for the services it names, or
// for every service whose logo reference matches it.
func (s *Manager) UpsertLogoImage(ctx context.Context, image model.Logo) error {
	if len(image.Services) > 0 {
		return s.upsertAssignedLogo(ctx, image)
	}
	services, err := s.store.List(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UnixMilli()
	for _, svc := range services {
		logo := svc.Logo
		if svc.Key.NetworkID != image.NetworkID || logo == nil || logo.LogoID != image.LogoID ||
			logo.Version == nil || *logo.Version != image.Version ||
			logo.DownloadDataID == nil || *logo.DownloadDataID != image.DownloadDataID {
			continue
		}
		if image.Deleted {
			err = s.DeleteLogo(ctx, svc.Key.NetworkID, svc.Key.StreamID, svc.Key.ServiceID, int64(image.LogoID), int64(image.LogoType), int64(image.Version), int64(image.DownloadDataID))
		} else {
			err = s.UpsertLogo(ctx, svc.Key.NetworkID, svc.Key.StreamID, svc.Key.ServiceID, int64(image.LogoID), int64(image.LogoType), int64(image.Version), int64(image.DownloadDataID), image.Data, now)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// upsertAssignedLogo stores a logo for the services it names and points
// their logo reference at it, only after the image is stored, so a failed
// store keeps the old logo.
func (s *Manager) upsertAssignedLogo(ctx context.Context, image model.Logo) error {
	logoID := int64(image.LogoID)
	logoType := int64(image.LogoType)
	logoVersion := int64(image.Version)
	downloadID := int64(image.DownloadDataID)
	now := time.Now().UnixMilli()
	for _, key := range image.Services {
		svc, err := s.store.GetByTriplet(ctx, key.NetworkID, key.StreamID, key.ServiceID)
		if err != nil {
			return err
		}
		if svc == nil {
			continue
		}
		if image.Deleted {
			if err := s.DeleteLogo(ctx, key.NetworkID, key.StreamID, key.ServiceID, logoID, logoType, logoVersion, downloadID); err != nil {
				return err
			}
			continue
		}
		if err := s.UpsertLogo(ctx, key.NetworkID, key.StreamID, key.ServiceID, logoID, logoType, logoVersion, downloadID, image.Data, now); err != nil {
			return err
		}
		updated, err := s.store.UpdateServiceLogoMetadata(ctx, key.NetworkID, key.StreamID, key.ServiceID, logoID, logoVersion, downloadID)
		if err != nil {
			return err
		}
		if updated {
			s.publishServiceByKey(ctx, eventTypeUpdate, key.NetworkID, key.ServiceID)
		}
	}
	return nil
}

// UpsertCommonDataAnnouncement records which service carries the
// all-receivers common data, and on which channel it was announced.
func (s *Manager) UpsertCommonDataAnnouncement(ctx context.Context, announcement model.CommonDataAnnouncement, channelType, channelID string) error {
	return s.store.UpsertCommonDataAnnouncement(ctx, CommonDataAnnouncement{
		OriginalNetworkID:   announcement.Service.NetworkID,
		TransportStreamID:   announcement.Service.StreamID,
		ServiceID:           announcement.Service.ServiceID,
		DownloadID:          announcement.DownloadID,
		VersionID:           announcement.VersionID,
		ObservedChannelType: channelType,
		ObservedChannelID:   channelID,
		SeenAt:              time.Now().UnixMilli(),
	})
}

func sameServiceCore(a, b *Service) bool {
	if a == nil || b == nil {
		return a == b
	}
	aCore := *a
	bCore := *b
	aCore.EPG = EPGStatus{}
	bCore.EPG = EPGStatus{}
	return reflect.DeepEqual(aCore, bCore)
}

func (s *Manager) SeedEventLog(ctx context.Context) error {
	services, err := s.store.List(ctx)
	if err != nil {
		return err
	}
	for _, svc := range services {
		s.publishService(eventTypeCreate, svc)
	}
	return nil
}

func (s *Manager) publishServiceByKey(ctx context.Context, typ string, networkID, serviceID uint16) {
	svc, err := s.store.GetByNetworkServiceID(ctx, networkID, serviceID)
	if err != nil {
		return
	}
	s.publishService(typ, svc)
}

func (s *Manager) publishService(typ string, svc *Service) {
	if s.events == nil || svc == nil {
		return
	}
	s.events.PublishServiceEvent(typ, svc, s.GetChannel(svc.ChannelType, svc.ChannelId))
}

func (s *Manager) prunedServices(ctx context.Context, active []ChannelKey) ([]*Service, error) {
	allowed := make(map[ChannelKey]struct{}, len(active))
	for _, key := range active {
		allowed[key] = struct{}{}
	}
	services, err := s.store.List(ctx)
	if err != nil {
		return nil, err
	}
	removed := make([]*Service, 0)
	for _, svc := range services {
		key := ChannelKey{Type: svc.ChannelType, ID: svc.ChannelId}
		if _, ok := allowed[key]; !ok {
			removed = append(removed, svc)
		}
	}
	return removed, nil
}
