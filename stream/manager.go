package stream

import (
	"context"
	"errors"
	"io"
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
	scanner            ServiceScanner
	sessions           map[sessionKey]*ChannelSession
	sessionGroups      map[sessionKey]string
	tunerManager       TunerManager
}

type StreamManagerConfig struct {
	Channels           config.ChannelsConfig
	DescramblerFactory DescramblerFactory
	Filter             ServiceFilter
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
	descramblerFactory := cfg.DescramblerFactory
	if descramblerFactory == nil {
		descramblerFactory = NewCommandDescrambler
	}
	return &StreamManager{
		channels:           cfg.Channels,
		descramblerFactory: descramblerFactory,
		filter:             serviceFilter,
		scanner:            scanner,
		sessions:           map[sessionKey]*ChannelSession{},
		sessionGroups:      map[sessionKey]string{},
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

	group := channelConfig.Type
	if len(channelConfig.TunerGroups) > 0 {
		group = channelConfig.TunerGroups[0]
	}

	device, err := m.tunerManager.NewDeviceByGroup(group, channelConfig)
	if err != nil {
		return nil, err
	}

	var descrambler Descrambler
	if provider, ok := m.tunerManager.(DecoderCommandProvider); ok {
		if command := provider.DecoderCommandByGroup(group); command != "" {
			descrambler = m.descramblerFactory(command)
		}
	}

	session := NewChannelSession(ChannelSessionConfig{
		Channel:       channel,
		ChannelConfig: channelConfig,
		Descrambler:   descrambler,
		Device:        device,
		Filter:        m.filter,
		OnStop:        func() { m.remove(key) },
		Scanner:       m.scanner,
		Type:          channelType,
	})
	m.sessions[key] = session
	m.sessionGroups[key] = group
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

func (m *StreamManager) ActiveSessionCountByGroup(group string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, g := range m.sessionGroups {
		if g == group {
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

func (m *StreamManager) remove(key sessionKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, key)
	delete(m.sessionGroups, key)
}

var (
	ErrChannelNotFound  = errors.New("channel not found")
	ErrTunerNotFound    = tuner.ErrTunerNotFound
	ErrUnsupportedTuner = tuner.ErrUnsupportedTuner
)

type TunerManager interface {
	NewDeviceByGroup(string, *config.ChannelConfig) (TunerDevice, error)
}

type DecoderCommandProvider interface {
	DecoderCommandByGroup(string) string
}

type TunerDevice = tuner.Device

type ServiceFilter interface {
	FilterService(context.Context, uint16, io.Reader, io.Writer) error
}

type ServiceScanner interface {
	ScanServices(context.Context, io.Reader, io.Writer) error
}
