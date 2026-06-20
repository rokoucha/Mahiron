package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/internal/tuner"
)

type StreamManager struct {
	mu           sync.Mutex
	eitCollector EITCollector
	eitUpdater   EITSectionUpdater
	filter       ServiceFilter
	remotes      map[string]*RemoteClient
	scanner      ServiceScanner
	sessions     map[sessionKey]Session
	sessionTypes map[sessionKey]string
	sources      *SourcePool
}

type StreamManagerConfig struct {
	Channels           config.ChannelsConfig
	DescramblerFactory DescramblerFactory
	Filter             ServiceFilter
	EITCollector       EITCollector
	EITUpdater         EITSectionUpdater
	Remotes            config.RemotesConfig
	Scanner            ServiceScanner
	TunerManager       TunerManager
}

type sessionKey struct {
	channel string
	typ     string
}

type Session interface {
	ChannelStream(context.Context, bool, io.Writer) error
	ProgramStream(context.Context, *program.Program, bool, io.Writer) error
	ServiceStream(context.Context, uint16, bool, io.Writer) error
	ScanServices(context.Context, io.Writer) error
	CollectEITS(context.Context, io.Writer) error
	CollectEITPF(context.Context, io.Writer) error
	Stop(context.Context) error
}

func NewStreamManager(cfg StreamManagerConfig) *StreamManager {
	descramblerFactory := cfg.DescramblerFactory
	if descramblerFactory == nil {
		descramblerFactory = NewCommandDescrambler
	}
	remotes := make(map[string]*RemoteClient, len(cfg.Remotes))
	for _, remote := range cfg.Remotes {
		remotes[remote.Name] = newRemoteClient(remote)
	}
	return &StreamManager{
		eitCollector: cfg.EITCollector,
		eitUpdater:   cfg.EITUpdater,
		filter:       cfg.Filter,
		remotes:      remotes,
		scanner:      cfg.Scanner,
		sessions:     map[sessionKey]Session{},
		sessionTypes: map[sessionKey]string{},
		sources:      NewSourcePool(cfg.Channels, cfg.TunerManager, descramblerFactory, remotes),
	}
}

func (m *StreamManager) GetOrCreate(ctx context.Context, channelType, channel string) (Session, error) {
	return m.getOrCreate(ctx, channelType, channel, false)
}

func (m *StreamManager) GetOrCreateWait(ctx context.Context, channelType, channel string) (Session, error) {
	return m.getOrCreate(ctx, channelType, channel, true)
}

func (m *StreamManager) getOrCreate(ctx context.Context, channelType, channel string, wait bool) (Session, error) {
	key := sessionKey{typ: channelType, channel: channel}

	m.mu.Lock()
	defer m.mu.Unlock()

	if session := m.sessions[key]; session != nil {
		slog.Debug("reusing stream session", "type", channelType, "channel", channel)
		return session, nil
	}

	hooks := []BroadcastHook{}
	if piggyback := NewEITPFPiggyback(channelType, channel, m.eitCollector, m.eitUpdater); piggyback != nil {
		hooks = append(hooks, piggyback.Hook)
	}
	slog.Debug("creating stream session", "type", channelType, "channel", channel, "wait", wait)
	lease, err := m.sources.Acquire(ctx, channelType, channel, wait, hooks)
	if err != nil {
		slog.Debug("failed to acquire stream source", "type", channelType, "channel", channel, "wait", wait, "err", err)
		return nil, err
	}
	if lease.Session != nil {
		m.sessions[key] = lease.Session
		m.sessionTypes[key] = lease.RouteType
		slog.Info("stream session created", "type", channelType, "channel", channel, "routeType", lease.RouteType, "source", "remote")
		return lease.Session, nil
	}
	broadcast := lease.Broadcast
	if broadcast == nil {
		broadcast = NewBroadcast(lease.Source, hooks, func() { m.remove(key) })
	}

	session := NewChannelSession(ChannelSessionConfig{
		Channel:       channel,
		ChannelConfig: lease.Channel,
		Broadcast:     broadcast,
		Descrambler:   lease.Descrambler,
		EITCollector:  m.eitCollector,
		EITUpdater:    m.eitUpdater,
		Filter:        m.filter,
		OnStop:        func() { m.remove(key) },
		Scanner:       m.scanner,
		Type:          channelType,
	})
	m.sessions[key] = session
	m.sessionTypes[key] = lease.RouteType
	slog.Info("stream session created", "type", channelType, "channel", channel, "routeType", lease.RouteType, "source", "local")
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
	sessions := make([]Session, 0, len(m.sessions))
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
	slog.Debug("stream session removed", "type", key.typ, "channel", key.channel)
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
