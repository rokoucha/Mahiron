package stream

import (
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/tuner"
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
	descramblerFactory := cfg.DescramblerFactory
	if descramblerFactory == nil {
		descramblerFactory = NewCommandDescrambler
	}
	return &StreamManager{
		channels:           cfg.Channels,
		descramblerFactory: descramblerFactory,
		eitCollector:       cfg.EITCollector,
		eitUpdater:         cfg.EITUpdater,
		filter:             cfg.Filter,
		scanner:            cfg.Scanner,
		sessions:           map[sessionKey]*ChannelSession{},
		sessionTypes:       map[sessionKey]string{},
		tunerManager:       cfg.TunerManager,
	}
}

func (m *StreamManager) GetOrCreate(ctx context.Context, channelType, channel string) (*ChannelSession, error) {
	return m.getOrCreate(ctx, channelType, channel, false)
}

func (m *StreamManager) GetOrCreateWait(ctx context.Context, channelType, channel string) (*ChannelSession, error) {
	return m.getOrCreate(ctx, channelType, channel, true)
}

func (m *StreamManager) getOrCreate(ctx context.Context, channelType, channel string, wait bool) (*ChannelSession, error) {
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

	route, routeChannelConfig, device, decoderCommand, err := m.newRouteDevice(ctx, channelConfig, wait)
	if err != nil {
		return nil, err
	}

	var descrambler Descrambler
	if decoderCommand == "" {
		if provider, ok := m.tunerManager.(DecoderCommandProvider); ok {
			decoderCommand = provider.DecoderCommandByType(route.Type)
		}
	}
	if decoderCommand != "" {
		descrambler = m.descramblerFactory(decoderCommand)
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

func (m *StreamManager) newRouteDevice(ctx context.Context, channel *config.ChannelConfig, wait bool) (config.ChannelRouteConfig, *config.ChannelConfig, TunerDevice, string, error) {
	routes := enabledRoutes(channel.RoutesOrDefault())
	for {
		var lastErr error
		unavailable := false
		for _, route := range routes {
			routeChannel := channel.RouteChannelConfig(route)
			var device TunerDevice
			var decoder string
			var err error
			if allocator, ok := m.tunerManager.(TunerAllocator); ok {
				device, decoder, err = allocator.AcquireDevice(ctx, route.Type, channel, &routeChannel, false)
			} else {
				device, err = m.tunerManager.NewDeviceByType(route.Type, &routeChannel)
			}
			if err == nil {
				return route, &routeChannel, device, decoder, nil
			}
			if errors.Is(err, tuner.ErrTunerUnavailable) {
				unavailable = true
			}
			lastErr = err
		}
		if !wait || !unavailable {
			if lastErr != nil {
				return config.ChannelRouteConfig{}, nil, nil, "", lastErr
			}
			return config.ChannelRouteConfig{}, nil, nil, "", ErrChannelNotFound
		}
		select {
		case <-ctx.Done():
			return config.ChannelRouteConfig{}, nil, nil, "", ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
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
	ErrChannelNotFound             = errors.New("channel not found")
	ErrEITCollectorNotConfigured   = errors.New("EIT collector not configured")
	ErrServiceFilterNotConfigured  = errors.New("service filter not configured")
	ErrServiceScannerNotConfigured = errors.New("service scanner not configured")
	ErrTunerNotFound               = tuner.ErrTunerNotFound
	ErrUnsupportedTuner            = tuner.ErrUnsupportedTuner
	ErrTunerUnavailable            = tuner.ErrTunerUnavailable
)

type TunerManager interface {
	NewDeviceByType(string, *config.ChannelConfig) (TunerDevice, error)
}

type TunerAllocator interface {
	AcquireDevice(context.Context, string, *config.ChannelConfig, *config.ChannelConfig, bool) (TunerDevice, string, error)
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
	UpsertEITSectionJSON(ctx context.Context, data []byte) error
}
