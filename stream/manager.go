package stream

import (
	"context"
	"errors"
	"io"
	"sort"
	"sync"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/filter"
	"github.com/21S1298001/Mahiron5/processor"
	"github.com/21S1298001/Mahiron5/tuner"
)

type StreamManager struct {
	mu                 sync.Mutex
	channels           config.ChannelsConfig
	descramblerFactory DescramblerFactory
	filter             ServiceFilter
	eitCollector       EITCollector
	eitUpdater         EITSectionUpdater
	scanner            ServiceScanner
	sessions           map[sessionKey]*ChannelSession
	sessionTypes       map[sessionKey]string
	tunerManager       TunerManager
}

type StreamManagerConfig struct {
	Channels           config.ChannelsConfig
	DescramblerFactory DescramblerFactory
	Filter             ServiceFilter
	EITCollector       EITCollector
	EITUpdater         EITSectionUpdater
	Scanner            ServiceScanner
	TunerManager       TunerManager
}

type sessionKey struct {
	channel string
	typ     string
}

func NewStreamManager(cfg StreamManagerConfig) *StreamManager {
	serviceFilter := cfg.Filter
	if serviceFilter == nil {
		serviceFilter = filter.NewServiceFilter()
	}
	scanner := cfg.Scanner
	if scanner == nil {
		scanner = processor.NewServiceScanner()
	}
	eitCollector := cfg.EITCollector
	if eitCollector == nil {
		eitCollector = processor.NewEITCollector()
	}
	descramblerFactory := cfg.DescramblerFactory
	if descramblerFactory == nil {
		descramblerFactory = NewCommandDescrambler
	}
	return &StreamManager{
		channels:           cfg.Channels,
		descramblerFactory: descramblerFactory,
		eitCollector:       eitCollector,
		eitUpdater:         cfg.EITUpdater,
		filter:             serviceFilter,
		scanner:            scanner,
		sessions:           map[sessionKey]*ChannelSession{},
		sessionTypes:       map[sessionKey]string{},
		tunerManager:       cfg.TunerManager,
	}
}

func (m *StreamManager) GetOrCreate(ctx context.Context, channelType, channel string) (*ChannelSession, error) {
	key := sessionKey{typ: channelType, channel: channel}

	m.mu.Lock()
	defer m.mu.Unlock()

	if session := m.sessions[key]; session != nil {
		return session, nil
	}

	channelConfig := m.findChannel(channelType, channel)
	if channelConfig == nil {
		return nil, ErrChannelNotFound
	}
	if channelConfig.IsDisabled != nil && *channelConfig.IsDisabled {
		return nil, ErrChannelNotFound
	}

	route, routeChannelConfig, device, err := m.newRouteDevice(channelConfig)
	if err != nil {
		return nil, err
	}

	var descrambler Descrambler
	if provider, ok := m.tunerManager.(DecoderCommandProvider); ok {
		if command := provider.DecoderCommandByType(route.Type); command != "" {
			descrambler = m.descramblerFactory(command)
		}
	}

	session := NewChannelSession(ChannelSessionConfig{
		Channel:       channel,
		ChannelConfig: routeChannelConfig,
		Descrambler:   descrambler,
		Device:        device,
		EITCollector:  m.eitCollector,
		EITUpdater:    m.eitUpdater,
		Filter:        m.filter,
		OnStop:        func() { m.remove(key) },
		Scanner:       m.scanner,
		Type:          channelType,
	})
	m.sessions[key] = session
	m.sessionTypes[key] = route.Type
	return session, nil
}

func (m *StreamManager) HasSession(channelType, channel string) bool {
	key := sessionKey{typ: channelType, channel: channel}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[key] != nil
}

func (m *StreamManager) ActiveSessionCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

func (m *StreamManager) ActiveSessionCountByType(channelType string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, typ := range m.sessionTypes {
		if typ == channelType {
			count++
		}
	}
	return count
}

func (m *StreamManager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	sessions := make([]*ChannelSession, 0, len(m.sessions))
	for _, session := range m.sessions {
		sessions = append(sessions, session)
	}
	m.mu.Unlock()

	var result error
	for _, session := range sessions {
		if err := session.Stop(ctx); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (m *StreamManager) findChannel(channelType, channel string) *config.ChannelConfig {
	for i := range m.channels {
		if m.channels[i].Type == channelType && m.channels[i].Channel == channel {
			return &m.channels[i]
		}
	}
	return nil
}

func (m *StreamManager) newRouteDevice(channel *config.ChannelConfig) (config.ChannelRouteConfig, *config.ChannelConfig, TunerDevice, error) {
	routes := enabledRoutes(channel.RoutesOrDefault())
	var lastErr error
	for _, route := range routes {
		routeChannel := channel.RouteChannelConfig(route)
		device, err := m.tunerManager.NewDeviceByType(route.Type, &routeChannel)
		if err == nil {
			return route, &routeChannel, device, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return config.ChannelRouteConfig{}, nil, nil, lastErr
	}
	return config.ChannelRouteConfig{}, nil, nil, ErrChannelNotFound
}

func enabledRoutes(routes []config.ChannelRouteConfig) []config.ChannelRouteConfig {
	enabled := make([]config.ChannelRouteConfig, 0, len(routes))
	for _, route := range routes {
		if route.IsDisabled != nil && *route.IsDisabled {
			continue
		}
		enabled = append(enabled, route)
	}
	sort.SliceStable(enabled, func(i, j int) bool {
		left, right := 0, 0
		if enabled[i].Priority != nil {
			left = *enabled[i].Priority
		}
		if enabled[j].Priority != nil {
			right = *enabled[j].Priority
		}
		return left < right
	})
	return enabled
}

func (m *StreamManager) remove(key sessionKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, key)
	delete(m.sessionTypes, key)
}

var (
	ErrChannelNotFound  = errors.New("channel not found")
	ErrTunerNotFound    = tuner.ErrTunerNotFound
	ErrUnsupportedTuner = tuner.ErrUnsupportedTuner
)

type TunerManager interface {
	NewDeviceByType(string, *config.ChannelConfig) (TunerDevice, error)
}

type DecoderCommandProvider interface {
	DecoderCommandByType(string) string
}

type TunerDevice = tuner.Device

type ServiceFilter interface {
	FilterService(context.Context, uint16, io.Reader, io.Writer) error
}

type ServiceScanner interface {
	ScanServices(context.Context, io.Reader, io.Writer) error
}

type EITCollector interface {
	CollectEITS(context.Context, io.Reader, io.Writer) error
	CollectEITPF(context.Context, io.Reader, io.Writer) error
}

type EITSectionUpdater interface {
	UpsertEITSectionJSON([]byte) error
}
