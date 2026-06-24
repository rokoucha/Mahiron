package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
	"github.com/google/uuid"
)

type StreamManager struct {
	mu                    sync.Mutex
	eitUpdater            EITSectionUpdater
	logoUpdater           LogoUpdater
	programUpdater        ProgramUpdater
	remoteEventSyncCancel context.CancelFunc
	remoteEventSyncOnce   sync.Once
	remoteEventSyncWG     sync.WaitGroup
	remotes               map[string]*RemoteClient
	serviceLister         ServiceLister
	sessionDetails        map[sessionKey]sessionMetricDetail
	sessions              map[sessionKey]Session
	sessionTypes          map[sessionKey]string
	sources               *SourcePool
}

type StreamManagerConfig struct {
	Channels           config.ChannelsConfig
	DescramblerFactory DescramblerFactory
	EITUpdater         EITSectionUpdater
	Remotes            config.RemotesConfig
	LogoUpdater        LogoUpdater
	ProgramUpdater     ProgramUpdater
	ServiceLister      ServiceLister
	TunerManager       TunerManager
}

type sessionKey struct {
	channel string
	typ     string
}

type sessionMetricDetail struct {
	routeType string
	source    string
	startedAt time.Time
}

const (
	remoteProgramEventSyncInitialBackoff = time.Second
	remoteProgramEventSyncMaxBackoff     = time.Minute
)

type Session interface {
	ChannelStream(context.Context, bool, io.Writer) error
	ProgramStream(context.Context, *program.Program, bool, io.Writer) error
	ServiceStream(context.Context, uint16, bool, io.Writer) error
	ScanServices(context.Context) ([]ts.ServiceInfo, error)
	CollectEIT(context.Context, func(*ts.EIT) error) error
	ObserveLogos(context.Context, func(*ts.LogoImage) error) error
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
		eitUpdater:     cfg.EITUpdater,
		logoUpdater:    cfg.LogoUpdater,
		programUpdater: cfg.ProgramUpdater,
		remotes:        remotes,
		serviceLister:  cfg.ServiceLister,
		sessionDetails: map[sessionKey]sessionMetricDetail{},
		sessions:       map[sessionKey]Session{},
		sessionTypes:   map[sessionKey]string{},
		sources:        NewSourcePool(cfg.Channels, cfg.TunerManager, descramblerFactory, remotes),
	}
}

func (m *StreamManager) StartRemoteProgramEventSync(ctx context.Context) {
	if m.programUpdater == nil || len(m.remotes) == 0 {
		return
	}
	m.remoteEventSyncOnce.Do(func() {
		syncCtx, cancel := context.WithCancel(ctx)
		m.remoteEventSyncCancel = cancel
		updater := m.remoteProgramUpdater()
		for name, client := range m.remotes {
			name, client := name, client
			m.remoteEventSyncWG.Add(1)
			go func() {
				defer m.remoteEventSyncWG.Done()
				m.runRemoteProgramEventSync(syncCtx, name, client, updater)
			}()
		}
	})
}

func (m *StreamManager) remoteProgramUpdater() ProgramUpdater {
	if m.serviceLister == nil {
		return m.programUpdater
	}
	return newKnownServiceProgramUpdater(m.programUpdater, m.serviceLister)
}

func (m *StreamManager) runRemoteProgramEventSync(ctx context.Context, name string, client *RemoteClient, updater ProgramUpdater) {
	backoff := remoteProgramEventSyncInitialBackoff
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		slog.Debug("starting remote program event sync", "remote", name)
		err := client.StreamProgramEvents(ctx, updater)
		if err := ctx.Err(); err != nil {
			return
		}
		slog.Warn("remote program event sync stopped", "remote", name, "err", err, "retryIn", backoff)
		if !sleepContext(ctx, backoff) {
			return
		}
		backoff = min(backoff*2, remoteProgramEventSyncMaxBackoff)
	}
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (m *StreamManager) GetOrCreate(ctx context.Context, channelType, channel string) (Session, error) {
	return m.getOrCreate(ctx, channelType, channel, false)
}

func (m *StreamManager) GetOrCreateWait(ctx context.Context, channelType, channel string) (Session, error) {
	return m.getOrCreate(ctx, channelType, channel, true)
}

func ensureUserContext(ctx context.Context, channelType, channel string) context.Context {
	if _, ok := tuner.UserFromContext(ctx); ok {
		return ctx
	}
	return tuner.WithUser(ctx, tuner.User{
		ID:       uuid.NewString(),
		Priority: -1,
		Agent:    "Mahiron Internal",
		StreamSetting: tuner.StreamSetting{
			Channel: &config.ChannelConfig{Type: channelType, Channel: channel},
		},
	})
}

func (m *StreamManager) getOrCreate(ctx context.Context, channelType, channel string, wait bool) (session Session, err error) {
	ctx = ensureUserContext(ctx, channelType, channel)
	ctx, span := observability.StartSpan(ctx, observability.SpanStreamGetOrCreate,
		observability.AttrChannelType.String(channelType),
		observability.AttrChannelID.String(channel),
		observability.AttrWait.Bool(wait),
	)
	recordStart := true
	defer func() {
		if recordStart {
			observability.RecordStreamSessionStart(ctx, channelType, "", "", streamSessionStartResult(err))
		}
		observability.EndSpan(span, err)
	}()

	key := sessionKey{typ: channelType, channel: channel}

	m.mu.Lock()
	defer m.mu.Unlock()

	if session := m.sessions[key]; session != nil {
		recordStart = false
		slog.Debug("reusing stream session", "type", channelType, "channel", channel)
		return session, nil
	}

	slog.Debug("creating stream session", "type", channelType, "channel", channel, "wait", wait)
	lease, err := m.sources.Acquire(ctx, channelType, channel, wait)
	if err != nil {
		slog.Debug("failed to acquire stream source", "type", channelType, "channel", channel, "wait", wait, "err", err)
		return nil, err
	}
	if lease.Session != nil {
		source := streamSessionSource(lease)
		m.sessions[key] = lease.Session
		m.sessionTypes[key] = lease.RouteType
		m.sessionDetails[key] = sessionMetricDetail{routeType: lease.RouteType, source: source, startedAt: time.Now()}
		observability.RecordStreamSessionStart(ctx, channelType, lease.RouteType, source, "success")
		recordStart = false
		slog.Info("stream session created", "type", channelType, "channel", channel, "routeType", lease.RouteType, "source", "remote")
		return lease.Session, nil
	}
	broadcast := lease.Broadcast
	if broadcast == nil {
		broadcast = NewBroadcast(lease.Source, func() { m.remove(key) })
	} else {
		if !broadcast.AddOnStop(func() { m.remove(key) }) {
			return nil, errors.New("broadcast stopped")
		}
	}

	session = NewChannelSession(ChannelSessionConfig{
		Channel:     channel,
		Broadcast:   broadcast,
		Descrambler: lease.Descrambler,
		EITUpdater:  m.eitUpdater,
		LogoUpdater: m.logoUpdater,
		OnStop:      func() { m.remove(key) },
		Type:        channelType,
	})
	m.sessions[key] = session
	m.sessionTypes[key] = lease.RouteType
	m.sessionDetails[key] = sessionMetricDetail{routeType: lease.RouteType, source: streamSessionSource(lease), startedAt: time.Now()}
	observability.RecordStreamSessionStart(ctx, channelType, lease.RouteType, streamSessionSource(lease), "success")
	recordStart = false
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
	var result error
	if m.remoteEventSyncCancel != nil {
		m.remoteEventSyncCancel()
	}
	eventSyncDone := make(chan struct{})
	go func() {
		m.remoteEventSyncWG.Wait()
		close(eventSyncDone)
	}()
	select {
	case <-eventSyncDone:
	case <-ctx.Done():
		result = errors.Join(result, ctx.Err())
	}

	m.mu.Lock()
	sessions := make([]Session, 0, len(m.sessions))
	for _, session := range m.sessions {
		sessions = append(sessions, session)
	}
	m.mu.Unlock()

	for _, session := range sessions {
		if err := session.Stop(ctx); err != nil {
			result = errors.Join(result, err)
		}
	}
	m.mu.Lock()
	for key := range m.sessions {
		m.recordSessionDurationLocked(key)
		delete(m.sessions, key)
		delete(m.sessionTypes, key)
		delete(m.sessionDetails, key)
	}
	m.mu.Unlock()
	return result
}

func (m *StreamManager) remove(key sessionKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recordSessionDurationLocked(key)
	delete(m.sessions, key)
	delete(m.sessionTypes, key)
	delete(m.sessionDetails, key)
	slog.Debug("stream session removed", "type", key.typ, "channel", key.channel)
}

func (m *StreamManager) recordSessionDurationLocked(key sessionKey) {
	detail, ok := m.sessionDetails[key]
	if !ok || detail.startedAt.IsZero() {
		return
	}
	observability.RecordStreamSessionDuration(context.Background(), key.typ, detail.routeType, detail.source, time.Since(detail.startedAt).Milliseconds())
}

func streamSessionSource(lease *SourceLease) string {
	if lease != nil && lease.Session != nil {
		return "remote"
	}
	return "local"
}

func streamSessionStartResult(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, ErrChannelNotFound):
		return "not_found"
	case errors.Is(err, ErrTunerNotFound):
		return "tuner_not_found"
	case errors.Is(err, ErrUnsupportedTuner):
		return "unsupported"
	case errors.Is(err, ErrTunerUnavailable):
		return "unavailable"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled"
	default:
		return "failure"
	}
}

var (
	ErrChannelNotFound            = errors.New("channel not found")
	ErrEITObservationUnsupported  = errors.New("EIT observation is not supported by remote sessions")
	ErrLogoObservationUnsupported = errors.New("logo observation is not supported by remote sessions")
	ErrTunerNotFound              = tuner.ErrTunerNotFound
	ErrUnsupportedTuner           = tuner.ErrUnsupportedTuner
	ErrTunerUnavailable           = tuner.ErrTunerUnavailable
)

type TunerManager interface {
	NewDeviceByType(string, *config.ChannelConfig) (TunerDevice, error)
}

type ServiceLister interface {
	GetServices(context.Context) ([]*service.Service, error)
}

type TunerAllocator interface {
	AcquireDevice(context.Context, string, *config.ChannelConfig, *config.ChannelConfig, bool) (TunerDevice, string, error)
}

type DecoderCommandProvider interface {
	DecoderCommandByType(string) string
}

type TunerDevice = tuner.Device

type EITSectionUpdater interface {
	UpsertEIT(ctx context.Context, eit *ts.EIT) error
}

type LogoUpdater interface {
	UpsertLogoImage(context.Context, *ts.LogoImage) error
}
