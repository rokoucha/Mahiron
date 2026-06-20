package service

import (
	"context"
	"reflect"
	"strconv"

	"github.com/21S1298001/Mahiron5/internal/config"
)

const (
	eventTypeCreate = "create"
	eventTypeUpdate = "update"
	eventTypeRemove = "remove"
)

type eventPublisher interface {
	PublishServiceEvent(typ string, svc *Service, channel *config.ChannelConfig)
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
	services, err := s.store.List(ctx)
	if err != nil {
		return 0, err
	}
	return len(services), nil
}

func (s *ServiceManager) GetServices(ctx context.Context) ([]*Service, error) {
	return s.store.List(ctx)
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
	services, err := s.store.List(ctx)
	if err != nil {
		return 0, 0, nil, err
	}
	for _, svc := range services {
		if svc.EPG.LastSuccessAt == nil || now-*svc.EPG.LastSuccessAt > staleAfter {
			stale++
		}
		if svc.EPG.LastError != "" {
			failed++
		}
		if svc.EPG.LastSuccessAt != nil && (lastSuccess == nil || *svc.EPG.LastSuccessAt > *lastSuccess) {
			v := *svc.EPG.LastSuccessAt
			lastSuccess = &v
		}
	}
	return
}

func (s *ServiceManager) ReconcileChannels(ctx context.Context) error {
	active := make([]ChannelKey, 0, len(s.channels))
	for _, channel := range s.channels {
		if !isDisabled(channel) {
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
	services, err := s.store.List(ctx)
	if err != nil {
		return nil, err
	}
	for _, service := range services {
		if service.ItemId() == parsedId {
			return service, nil
		}
	}
	return nil, nil
}

func (s *ServiceManager) GetChannels() config.ChannelsConfig {
	channels := make(config.ChannelsConfig, 0, len(s.channels))
	for _, channel := range s.channels {
		if isDisabled(channel) {
			continue
		}
		channels = append(channels, channel)
	}
	return channels
}

func (s *ServiceManager) GetChannel(channelType string, channelId string) *config.ChannelConfig {
	for i := range s.channels {
		if s.channels[i].Type == channelType && s.channels[i].Channel == channelId && !isDisabled(s.channels[i]) {
			channel := s.channels[i]
			return &channel
		}
	}
	return nil
}

func (s *ServiceManager) GetServicesByChannel(ctx context.Context, channelType string, channelId string) ([]*Service, error) {
	return s.store.GetByChannel(ctx, channelType, channelId)
}

func (s *ServiceManager) GetByChannel(ctx context.Context, channelType string, channelId string) ([]*Service, error) {
	return s.store.GetByChannel(ctx, channelType, channelId)
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
	services, err := s.store.GetByChannel(ctx, channelType, channelId)
	if err != nil {
		return nil, err
	}
	parsedId, parseErr := strconv.ParseInt(id, 10, 64)
	for _, service := range services {
		if service.Id == id || (parseErr == nil && service.ItemId() == parsedId) {
			return service, nil
		}
	}
	return nil, nil
}

func isDisabled(channel config.ChannelConfig) bool {
	return channel.IsDisabled != nil && *channel.IsDisabled
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
	services, err := s.store.List(ctx)
	if err != nil {
		return
	}
	for _, svc := range services {
		if svc.NetworkId == networkID && svc.ServiceId == serviceID {
			s.publishService(typ, svc)
			return
		}
	}
}

func (s *ServiceManager) publishService(typ string, svc *Service) {
	if s.events == nil || svc == nil {
		return
	}
	s.events.PublishServiceEvent(typ, svc, s.GetChannel(svc.ChannelType, svc.ChannelId))
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
