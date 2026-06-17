package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/stream"
)

type ServiceManager struct {
	mu       sync.RWMutex
	channels config.ChannelsConfig
	services []*Service
}

type ServiceManagerConfig struct {
	Channels config.ChannelsConfig
	Services []*Service
}

func NewServiceManager(config *ServiceManagerConfig) *ServiceManager {
	return &ServiceManager{
		channels: config.Channels,
		services: cloneServices(
			config.Services,
		),
	}
}

func (s *ServiceManager) CountServices() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.services)
}

func (s *ServiceManager) GetServices() []*Service {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneServices(s.services)
}

func (s *ServiceManager) GetServiceById(id string) *Service {
	s.mu.RLock()
	defer s.mu.RUnlock()
	parsedId, parseErr := strconv.ParseInt(id, 10, 64)
	for _, service := range s.services {
		if service.Id == id || (parseErr == nil && service.ItemId() == parsedId) {
			return cloneService(service)
		}
	}

	return nil
}

func (s *ServiceManager) GetChannels() config.ChannelsConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()

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
	s.mu.RLock()
	defer s.mu.RUnlock()

	for i := range s.channels {
		if s.channels[i].Type == channelType && s.channels[i].Channel == channelId && !isDisabled(s.channels[i]) {
			channel := s.channels[i]
			return &channel
		}
	}
	return nil
}

func (s *ServiceManager) GetServicesByChannel(channelType string, channelId string) []*Service {
	s.mu.RLock()
	defer s.mu.RUnlock()

	services := make([]*Service, 0)
	for _, service := range s.services {
		if service.ChannelType == channelType && service.ChannelId == channelId {
			services = append(services, cloneService(service))
		}
	}
	return services
}

func (s *ServiceManager) GetServiceByChannelAndId(channelType string, channelId string, id string) *Service {
	s.mu.RLock()
	defer s.mu.RUnlock()
	parsedId, parseErr := strconv.ParseInt(id, 10, 64)

	for _, service := range s.services {
		if service.ChannelType == channelType &&
			service.ChannelId == channelId &&
			(service.Id == id || (parseErr == nil && service.ItemId() == parsedId)) {
			return cloneService(service)
		}
	}
	return nil
}

type scanService struct {
	Nid                uint16 `json:"nid"`
	Tsid               uint16 `json:"tsid"`
	Sid                uint16 `json:"sid"`
	Name               string `json:"name"`
	Type               uint8  `json:"type"`
	LogoId             uint64 `json:"logoId"`
	RemoteControlKeyId uint8  `json:"remoteControlKeyId"`
}

func (s *ServiceManager) ScanServices(ctx context.Context, streamManager *stream.StreamManager, channelType string, channelId string) error {
	out := bytes.Buffer{}

	session, err := streamManager.GetOrCreate(ctx, channelType, channelId)
	if err != nil {
		return err
	}
	if err := session.ScanServices(ctx, &out); err != nil {
		return err
	}

	var services []*scanService
	if err := json.Unmarshal(out.Bytes(), &services); err != nil {
		return err
	}

	scanned := make([]*Service, len(services))
	for i, service := range services {
		scanned[i] = &Service{
			Id:                 fmt.Sprintf("%05d%05d", service.Nid, service.Sid),
			ServiceId:          service.Sid,
			NetworkId:          service.Nid,
			TransportStreamId:  service.Tsid,
			Name:               service.Name,
			Type:               service.Type,
			RemoteControlKeyId: service.RemoteControlKeyId,
			ChannelType:        channelType,
			ChannelId:          channelId,
		}
	}

	s.updateServices(scanned)

	return nil
}

func (s *ServiceManager) updateServices(scanned []*Service) {
	s.mu.Lock()
	defer s.mu.Unlock()

	positions := make(map[string]int, len(s.services))
	for i, service := range s.services {
		positions[service.Id] = i
	}
	for _, service := range scanned {
		cloned := cloneService(service)
		if i, ok := positions[service.Id]; ok {
			s.services[i] = cloned
			continue
		}
		positions[service.Id] = len(s.services)
		s.services = append(s.services, cloned)
	}
}

func cloneServices(services []*Service) []*Service {
	cloned := make([]*Service, len(services))
	for i, service := range services {
		cloned[i] = cloneService(service)
	}
	return cloned
}

func cloneService(service *Service) *Service {
	if service == nil {
		return nil
	}
	cloned := *service
	return &cloned
}

func isDisabled(channel config.ChannelConfig) bool {
	return channel.IsDisabled != nil && *channel.IsDisabled
}
