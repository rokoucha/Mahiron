package service

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/ts"
)

const (
	eventTypeCreate = "create"
	eventTypeUpdate = "update"
	eventTypeRemove = "remove"
)

type eventPublisher interface {
	PublishServiceEvent(typ string, data map[string]any)
}

type ServiceManager struct {
	store    Store
	channels config.ChannelsConfig
	events   eventPublisher
}

func NewServiceManager(store Store, channels config.ChannelsConfig, events ...eventPublisher) *ServiceManager {
	var publisher eventPublisher
	if len(events) > 0 {
		publisher = events[0]
	}
	return &ServiceManager{
		store:    store,
		channels: channels,
		events:   publisher,
	}
}

func (s *ServiceManager) CountServices(ctx context.Context) (int, error) {
	return s.store.Count(ctx)
}

func (s *ServiceManager) GetServices(ctx context.Context) ([]*Service, error) {
	services, err := s.store.List(ctx)
	if err != nil {
		return nil, err
	}
	return s.orderServices(services), nil
}

func (s *ServiceManager) SetEPGAttempt(ctx context.Context, networkID, serviceID uint16, attemptedAt int64, lastError string) error {
	if err := s.store.SetEPGAttempt(ctx, networkID, serviceID, attemptedAt, lastError); err != nil {
		return err
	}
	s.publishServiceByKey(ctx, eventTypeUpdate, networkID, serviceID)
	return nil
}

func (s *ServiceManager) SetEPGSuccess(ctx context.Context, networkID, serviceID uint16, succeededAt int64) error {
	if err := s.store.SetEPGSuccess(ctx, networkID, serviceID, succeededAt); err != nil {
		return err
	}
	s.publishServiceByKey(ctx, eventTypeUpdate, networkID, serviceID)
	return nil
}

func (s *ServiceManager) EPGSummary(ctx context.Context, staleAfter int64, now int64) (stale, failed int, lastSuccess *int64, err error) {
	return s.store.EPGSummary(ctx, staleAfter, now)
}

func (s *ServiceManager) ReconcileChannels(ctx context.Context) error {
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

func (s *ServiceManager) GetServiceById(ctx context.Context, id string) (*Service, error) {
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

func (s *ServiceManager) GetServiceByItemID(ctx context.Context, itemID int64) (*Service, error) {
	return s.store.GetByItemID(ctx, itemID)
}

func (s *ServiceManager) GetChannels() config.ChannelsConfig {
	channels := make(config.ChannelsConfig, 0, len(s.channels))
	for _, channel := range s.channels {
		if config.IsChannelDisabled(channel) {
			continue
		}
		channels = append(channels, channel)
	}
	return channels
}

func (s *ServiceManager) GetChannel(channelType string, channelId string) *config.ChannelConfig {
	for i := range s.channels {
		if s.channels[i].Type == channelType && s.channels[i].Channel == channelId && !config.IsChannelDisabled(s.channels[i]) {
			channel := s.channels[i]
			return &channel
		}
	}
	return nil
}

func (s *ServiceManager) GetServicesByChannel(ctx context.Context, channelType string, channelId string) ([]*Service, error) {
	services, err := s.store.GetByChannel(ctx, channelType, channelId)
	if err != nil {
		return nil, err
	}
	return s.orderServices(services), nil
}

func (s *ServiceManager) ReplaceChannelServices(ctx context.Context, channelType, channelId string, services []*Service) error {
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

func (s *ServiceManager) GetServiceByChannelAndId(ctx context.Context, channelType string, channelId string, id string) (*Service, error) {
	parsedId, parseErr := strconv.ParseInt(id, 10, 64)
	if parseErr != nil {
		parsedId = 0
	}
	return s.store.GetByChannelAndID(ctx, channelType, channelId, id, parsedId)
}

func (s *ServiceManager) GetLogoByServiceItemID(ctx context.Context, itemID int64) ([]byte, error) {
	return s.store.GetLogoByServiceItemID(ctx, itemID)
}

func (s *ServiceManager) KnownLogoTargets(ctx context.Context) ([]LogoTarget, error) {
	return s.store.KnownLogoTargets(ctx)
}

func (s *ServiceManager) MissingLogoTargets(ctx context.Context) ([]LogoTarget, error) {
	return s.store.MissingLogoTargets(ctx)
}

func (s *ServiceManager) LogoGatherTargets(ctx context.Context) ([]LogoTarget, error) {
	targets, err := s.store.MissingLogoTargets(ctx)
	if err != nil {
		return nil, err
	}
	seen := make(map[LogoTarget]struct{}, len(targets))
	for _, target := range targets {
		seen[target] = struct{}{}
	}
	known, err := s.store.KnownLogoTargets(ctx)
	if err != nil {
		return nil, err
	}
	for _, target := range known {
		if _, ok := seen[target]; ok {
			continue
		}
		targets = append(targets, target)
		seen[target] = struct{}{}
	}
	return s.appendCommonLogoTargets(ctx, targets)
}

func (s *ServiceManager) appendCommonLogoTargets(ctx context.Context, targets []LogoTarget) ([]LogoTarget, error) {
	services, err := s.store.List(ctx)
	if err != nil {
		return nil, err
	}
	commonChannel, err := s.commonDataChannel(ctx)
	if err != nil {
		return nil, err
	}
	commonServices, err := s.commonDataServiceKeys(ctx)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		seen[commonLogoTargetKey(target)] = struct{}{}
	}
	var refreshTarget *LogoTarget
	for _, svc := range services {
		if !ts.IsSatelliteOriginalNetworkID(svc.NetworkId) {
			continue
		}
		if _, ok := commonServices[commonDataServiceKey{svc.NetworkId, svc.TransportStreamId, svc.ServiceId}]; ok {
			continue
		}
		target := commonLogoTargetForService(svc, commonChannel)
		if commonChannel != nil && refreshTarget == nil {
			refreshTarget = &target
		}
		if svc.HasLogoData {
			continue
		}
		key := commonLogoTargetKey(target)
		if _, ok := seen[key]; ok {
			continue
		}
		targets = append(targets, target)
		seen[key] = struct{}{}
	}
	if refreshTarget != nil {
		key := commonLogoTargetKey(*refreshTarget)
		if _, ok := seen[key]; !ok {
			targets = append(targets, *refreshTarget)
		}
	}
	return targets, nil
}

func commonLogoTargetForService(svc *Service, commonChannel *ChannelKey) LogoTarget {
	channel := ChannelKey{Type: svc.ChannelType, ID: svc.ChannelId}
	isProbe := true
	if commonChannel != nil {
		channel = *commonChannel
		isProbe = false
	}
	return LogoTarget{
		NetworkId:         svc.NetworkId,
		ServiceId:         svc.ServiceId,
		TransportStreamId: svc.TransportStreamId,
		ChannelType:       channel.Type,
		ChannelId:         channel.ID,
		IsCommonData:      true,
		IsSDTTProbe:       isProbe,
	}
}

type commonDataServiceKey struct {
	networkID, transportStreamID, serviceID uint16
}

func (s *ServiceManager) commonDataServiceKeys(ctx context.Context) (map[commonDataServiceKey]struct{}, error) {
	announcements, err := s.store.ListCommonDataAnnouncements(ctx)
	if err != nil {
		return nil, err
	}
	result := make(map[commonDataServiceKey]struct{}, len(announcements)+1)
	for _, announcement := range announcements {
		result[commonDataServiceKey{announcement.OriginalNetworkID, announcement.TransportStreamID, announcement.ServiceID}] = struct{}{}
	}
	defaultAnnouncement := ts.DefaultCommonDataAnnouncement()
	result[commonDataServiceKey{defaultAnnouncement.OriginalNetworkID, defaultAnnouncement.TransportStreamID, defaultAnnouncement.ServiceID}] = struct{}{}
	return result, nil
}

func (s *ServiceManager) commonDataChannel(ctx context.Context) (*ChannelKey, error) {
	announcements, err := s.store.ListCommonDataAnnouncements(ctx)
	if err != nil {
		return nil, err
	}
	for _, announcement := range announcements {
		if !ts.IsSatelliteOriginalNetworkID(announcement.OriginalNetworkID) {
			continue
		}
		svc, err := s.store.GetByTriplet(ctx, announcement.OriginalNetworkID, announcement.TransportStreamID, announcement.ServiceID)
		if err != nil {
			return nil, err
		}
		if svc == nil {
			continue
		}
		return &ChannelKey{Type: svc.ChannelType, ID: svc.ChannelId}, nil
	}
	defaultAnnouncement := ts.DefaultCommonDataAnnouncement()
	svc, err := s.store.GetByTriplet(ctx, defaultAnnouncement.OriginalNetworkID, defaultAnnouncement.TransportStreamID, defaultAnnouncement.ServiceID)
	if err != nil {
		return nil, err
	}
	if svc == nil {
		return nil, nil
	}
	return &ChannelKey{Type: svc.ChannelType, ID: svc.ChannelId}, nil
}

func commonLogoTargetKey(target LogoTarget) string {
	return fmt.Sprintf("%d/%d/%d/%t/%t/%s/%s", target.NetworkId, target.TransportStreamId, target.ServiceId, target.IsCommonData, target.IsSDTTProbe, target.ChannelType, target.ChannelId)
}

func (s *ServiceManager) UpsertLogo(ctx context.Context, networkID, transportStreamID, serviceID uint16, logoID int64, logoType int64, logoVersion int64, downloadDataID int64, data []byte, updatedAt int64) error {
	if err := s.store.UpsertLogo(ctx, networkID, transportStreamID, serviceID, logoID, logoType, logoVersion, downloadDataID, data, updatedAt); err != nil {
		return err
	}
	s.publishServiceByKey(ctx, eventTypeUpdate, networkID, serviceID)
	return nil
}

func (s *ServiceManager) DeleteLogo(ctx context.Context, networkID, transportStreamID, serviceID uint16, logoID int64, logoType int64, logoVersion int64, downloadDataID int64) error {
	if err := s.store.DeleteLogo(ctx, networkID, transportStreamID, serviceID, logoID, logoType, logoVersion, downloadDataID); err != nil {
		return err
	}
	s.publishServiceByKey(ctx, eventTypeUpdate, networkID, serviceID)
	return nil
}

func (s *ServiceManager) UpsertLogoImage(ctx context.Context, image *ts.LogoImage) error {
	targets, err := s.store.KnownLogoTargets(ctx)
	if err != nil {
		return err
	}
	var data []byte
	now := time.Now().UnixMilli()
	for _, target := range targets {
		if target.NetworkId != image.OriginalNetworkID ||
			target.LogoId != int64(image.LogoID) ||
			target.LogoVersion != int64(image.LogoVersion) ||
			target.LogoDownloadDataId != int64(image.DownloadDataID) {
			continue
		}
		if image.IsDeleted {
			if err := s.DeleteLogo(ctx, target.NetworkId, target.TransportStreamId, target.ServiceId, target.LogoId, int64(image.LogoType), int64(image.LogoVersion), int64(image.DownloadDataID)); err != nil {
				return err
			}
		} else {
			if data == nil {
				data, err = ts.NormalizeARIBLogoPNG(image.Data)
				if err != nil {
					return err
				}
			}
			if err := s.UpsertLogo(ctx, target.NetworkId, target.TransportStreamId, target.ServiceId, target.LogoId, int64(image.LogoType), int64(image.LogoVersion), int64(image.DownloadDataID), data, now); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *ServiceManager) UpsertCommonLogoImage(ctx context.Context, image ts.CommonLogoImage) error {
	if image.IsNetwork {
		return nil
	}
	var data []byte
	var err error
	if !image.IsDeleted {
		data, err = ts.NormalizeARIBLogoPNG(image.Data)
		if err != nil {
			return err
		}
	}
	logoID := int64(image.LogoID)
	logoType := int64(image.LogoType)
	logoVersion := int64(image.LogoVersion)
	downloadID := int64(image.DownloadID)
	now := time.Now().UnixMilli()
	for _, target := range image.Services {
		if target.TransportStreamID == ts.NetworkLogoTransportStreamWildcard || target.ServiceID == ts.NetworkLogoServiceWildcard {
			continue
		}
		svc, err := s.store.GetByTriplet(ctx, target.OriginalNetworkID, target.TransportStreamID, target.ServiceID)
		if err != nil {
			return err
		}
		if svc == nil {
			continue
		}
		if image.IsDeleted {
			if err := s.DeleteLogo(ctx, target.OriginalNetworkID, target.TransportStreamID, target.ServiceID, logoID, logoType, logoVersion, downloadID); err != nil {
				return err
			}
			continue
		}
		if err := s.UpsertLogo(ctx, target.OriginalNetworkID, target.TransportStreamID, target.ServiceID, logoID, logoType, logoVersion, downloadID, data, now); err != nil {
			return err
		}
		updated, err := s.store.UpdateServiceLogoMetadata(ctx, target.OriginalNetworkID, target.TransportStreamID, target.ServiceID, logoID, logoVersion, downloadID)
		if err != nil {
			return err
		}
		if updated {
			s.publishServiceByKey(ctx, eventTypeUpdate, target.OriginalNetworkID, target.ServiceID)
		}
	}
	return nil
}

func (s *ServiceManager) UpsertCommonDataAnnouncement(ctx context.Context, announcement ts.CommonDataAnnouncement, channelType, channelID string) error {
	return s.store.UpsertCommonDataAnnouncement(ctx, CommonDataAnnouncement{
		OriginalNetworkID:   announcement.OriginalNetworkID,
		TransportStreamID:   announcement.TransportStreamID,
		ServiceID:           announcement.ServiceID,
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

func (s *ServiceManager) SeedEventLog(ctx context.Context) error {
	services, err := s.store.List(ctx)
	if err != nil {
		return err
	}
	for _, svc := range services {
		s.publishService(eventTypeCreate, svc)
	}
	return nil
}

func (s *ServiceManager) publishServiceByKey(ctx context.Context, typ string, networkID, serviceID uint16) {
	svc, err := s.store.GetByNetworkServiceID(ctx, networkID, serviceID)
	if err != nil {
		return
	}
	s.publishService(typ, svc)
}

func (s *ServiceManager) publishService(typ string, svc *Service) {
	if s.events == nil || svc == nil {
		return
	}
	s.events.PublishServiceEvent(typ, svc.EventData(s.GetChannel(svc.ChannelType, svc.ChannelId)))
}

func (s *ServiceManager) prunedServices(ctx context.Context, active []ChannelKey) ([]*Service, error) {
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
