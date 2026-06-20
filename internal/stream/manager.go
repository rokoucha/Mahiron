package stream

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/tuner"
)

type StreamManager struct {
	mu           sync.Mutex
	eitCollector EITCollector
	eitUpdater   EITSectionUpdater
	filter       ServiceFilter
	scanner      ServiceScanner
	sessions     map[sessionKey]*ChannelSession
	sessionTypes map[sessionKey]string
	sources      *SourcePool
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
		eitCollector: cfg.EITCollector,
		eitUpdater:   cfg.EITUpdater,
		filter:       cfg.Filter,
		scanner:      cfg.Scanner,
		sessions:     map[sessionKey]*ChannelSession{},
		sessionTypes: map[sessionKey]string{},
		sources:      NewSourcePool(cfg.Channels, cfg.TunerManager, descramblerFactory),
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

	lease, err := m.sources.Acquire(ctx, channelType, channel, wait)
	if err != nil {
		return nil, err
	}

	hooks := []BroadcastHook{}
	if piggyback := NewEITPFPiggyback(channelType, channel, m.eitCollector, m.eitUpdater); piggyback != nil {
		hooks = append(hooks, piggyback.Hook)
	}
	broadcast := NewBroadcast(lease.Source, hooks, func() { m.remove(key) })

	session := NewChannelSession(ChannelSessionConfig{
		Channel:       channel,
		ChannelConfig: lease.Channel,
		Broadcast:     broadcast,
		Descrambler:   lease.Descrambler,
		EITCollector:  m.eitCollector,
		EITUpdater:    m.eitUpdater,
		Filter:        m.filter,
		Scanner:       m.scanner,
		Type:          channelType,
	})
	m.sessions[key] = session
	m.sessionTypes[key] = lease.RouteType
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
