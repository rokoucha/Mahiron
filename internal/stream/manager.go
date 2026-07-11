package stream

import (
	"cmp"
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/stream/databroadcast"
	"github.com/21S1298001/mahiron/internal/stream/remote"
	"github.com/21S1298001/mahiron/internal/stream/source"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
	"github.com/google/uuid"
)

type StreamManager struct {
	eitUpdater            EITSectionUpdater
	logoUpdater           LogoUpdater
	programUpdater        ProgramUpdater
	remoteEventSyncCancel context.CancelFunc
	remoteEventSyncOnce   sync.Once
	remoteEventSyncWG     sync.WaitGroup
	remotes               map[string]*remote.Client
	remoteTunerTypes      map[string]map[string]struct{}
	serviceLister         ServiceLister
	registry              *sessionRegistry
	sources               *source.Pool
}

// RemoteTunerStatus identifies a tuner that belongs to a configured remote
// Mahiron server.
type RemoteTunerStatus struct {
	Remote string
	Status tuner.Status
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

type Session interface {
	ChannelStream(context.Context, bool, io.Writer) error
	ProgramStream(context.Context, *program.Program, bool, io.Writer) error
	ServiceStream(context.Context, uint16, bool, io.Writer) error
	ScanServices(context.Context) ([]ts.ServiceInfo, error)
	CollectEIT(context.Context, func(*ts.EIT) error) error
	ObserveLogos(context.Context, func(*ts.LogoImage) error) error
	ObserveDataBroadcast(context.Context, uint16, bool, func(databroadcast.DataBroadcastEvent) error) error
	DataBroadcastModule(uint16, byte, uint16) (databroadcast.DataBroadcastModule, bool)
	Stop(context.Context) error
}

func NewStreamManager(cfg StreamManagerConfig) *StreamManager {
	descramblerFactory := cfg.DescramblerFactory
	if descramblerFactory == nil {
		descramblerFactory = NewCommandDescrambler
	}
	remotes := make(map[string]*remote.Client, len(cfg.Remotes))
	for _, remoteConfig := range cfg.Remotes {
		remotes[remoteConfig.Name] = newRemoteClient(remoteConfig)
	}
	remoteTunerTypes := make(map[string]map[string]struct{}, len(remotes))
	for _, channel := range cfg.Channels {
		if config.IsChannelDisabled(channel) {
			continue
		}
		for _, route := range channel.RoutesOrDefault() {
			if route.Remote == "" || route.IsDisabled != nil && *route.IsDisabled {
				continue
			}
			if remoteTunerTypes[route.Remote] == nil {
				remoteTunerTypes[route.Remote] = map[string]struct{}{}
			}
			remoteTunerTypes[route.Remote][route.Type] = struct{}{}
		}
	}
	return &StreamManager{
		eitUpdater:       cfg.EITUpdater,
		logoUpdater:      cfg.LogoUpdater,
		programUpdater:   cfg.ProgramUpdater,
		remotes:          remotes,
		remoteTunerTypes: remoteTunerTypes,
		serviceLister:    cfg.ServiceLister,
		registry:         newSessionRegistry(),
		sources:          source.NewPool(cfg.Channels, cfg.TunerManager, descramblerFactory, remotes),
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
				remote.RunProgramEventSync(syncCtx, name, client, updater)
			}()
		}
	})
}

func (m *StreamManager) remoteProgramUpdater() ProgramUpdater {
	if m.serviceLister == nil {
		return m.programUpdater
	}
	return remote.NewKnownServiceProgramUpdater(m.programUpdater, m.serviceLister)
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
	agent := "Mahiron Internal"
	if info, ok := run.JobInfoFromContext(ctx); ok && info.Name != "" {
		agent = info.Name
	}
	return tuner.WithUser(ctx, tuner.User{
		ID:       uuid.NewString(),
		Priority: -1,
		Agent:    agent,
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

	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		res := m.registry.acquire(key)
		switch {
		case res.hasEntry:
			if !sessionAlive(res.entry.session) {
				m.registry.removeIfSame(key, res.entry.session)
				continue
			}
			recordStart = false
			slog.Debug("reusing stream session", "type", channelType, "channel", channel)
			return res.entry.session, nil
		case res.shuttingDown:
			return nil, ErrStreamManagerShutdown
		case res.pending != nil:
			select {
			case <-res.pending.done:
				if res.pending.err != nil {
					return nil, res.pending.err
				}
				recordStart = false
				slog.Debug("reusing stream session", "type", channelType, "channel", channel)
				return res.pending.session, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		default:
			create := res.create

			slog.Debug("creating stream session", "type", channelType, "channel", channel, "wait", wait)
			session, routeType, source, err := m.createSession(ctx, key, channelType, channel, wait)
			if err != nil {
				slog.Debug("failed to acquire stream source", "type", channelType, "channel", channel, "wait", wait, "err", err)
			}

			m.registry.completeCreate(key, create, session, routeType, source, err)

			if err != nil {
				if errors.Is(err, errBroadcastStopped) && attempt < maxAttempts-1 {
					continue
				}
				return nil, err
			}
			observability.RecordStreamSessionStart(ctx, channelType, routeType, source, "success")
			recordStart = false
			slog.Info("stream session created", "type", channelType, "channel", channel, "routeType", routeType, "source", source)
			return session, nil
		}
	}
	return nil, ErrSessionAcquireFailed
}

// aliveChecker is implemented by session types that can outlive their
// underlying resources and go dead before being evicted from the session
// registry (see local.Session.Alive). Sessions without a lifecycle race
// (e.g. remote sessions) are treated as always alive.
type aliveChecker interface {
	Alive() bool
}

func sessionAlive(session Session) bool {
	if checker, ok := session.(aliveChecker); ok {
		return checker.Alive()
	}
	return true
}

func (m *StreamManager) HasSession(channelType, channel string) bool {
	return m.registry.has(sessionKey{typ: channelType, channel: channel})
}

func (m *StreamManager) GetExisting(channelType, channel string) (Session, bool) {
	session, ok := m.registry.get(sessionKey{typ: channelType, channel: channel})
	if !ok || !sessionAlive(session) {
		return nil, false
	}
	return session, true
}

func (m *StreamManager) ActiveSessionCount() int {
	return m.registry.count()
}

// RemoteTunerStatuses collects remote tuner state concurrently. A failed
// remote is omitted so the local tuner status endpoint remains available.
func (m *StreamManager) RemoteTunerStatuses(ctx context.Context) []RemoteTunerStatus {
	type result struct {
		name     string
		statuses []tuner.Status
	}
	results := make(chan result, len(m.remotes))
	var wg sync.WaitGroup
	for name, client := range m.remotes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			statuses, err := client.TunerStatuses(ctx)
			if err == nil {
				results <- result{name: name, statuses: m.configuredRemoteTuners(name, statuses)}
			}
		}()
	}
	wg.Wait()
	close(results)

	var collected []RemoteTunerStatus
	for item := range results {
		for _, status := range item.statuses {
			status.Users = m.remoteSessionUsers(item.name, status)
			collected = append(collected, RemoteTunerStatus{Remote: item.name, Status: status})
		}
	}
	slices.SortFunc(collected, func(a, b RemoteTunerStatus) int {
		if a.Remote != b.Remote {
			return strings.Compare(a.Remote, b.Remote)
		}
		return cmp.Compare(a.Status.Index, b.Status.Index)
	})
	return collected
}

func (m *StreamManager) remoteSessionUsers(remoteName string, status tuner.Status) []tuner.User {
	var users []tuner.User
	for _, session := range m.registry.activeSessions() {
		remoteSession, ok := session.(*remote.Session)
		if !ok || remoteSession.RemoteName() != remoteName || !remoteSession.MatchesTuner(status) {
			continue
		}
		users = append(users, remoteSession.Users()...)
	}
	return users
}

func (m *StreamManager) configuredRemoteTuners(remoteName string, statuses []tuner.Status) []tuner.Status {
	types := m.remoteTunerTypes[remoteName]
	result := make([]tuner.Status, 0, len(statuses))
	for _, status := range statuses {
		if slices.ContainsFunc(status.Types, func(typ string) bool {
			_, ok := types[typ]
			return ok
		}) {
			result = append(result, status)
		}
	}
	return result
}

func (m *StreamManager) Shutdown(ctx context.Context) error {
	var result error
	if m.remoteEventSyncCancel != nil {
		m.remoteEventSyncCancel()
	}

	creates := m.registry.beginShutdown()

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

	if err := waitSessionCreates(ctx, creates); err != nil {
		result = errors.Join(result, err)
	}

	for _, session := range m.registry.activeSessions() {
		if err := session.Stop(ctx); err != nil {
			result = errors.Join(result, err)
		}
	}
	m.registry.clear()
	return result
}

func (m *StreamManager) remove(key sessionKey) {
	m.registry.remove(key)
	slog.Debug("stream session removed", "type", key.typ, "channel", key.channel)
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
	ErrChannelNotFound            = remote.ErrChannelNotFound
	ErrEITObservationUnsupported  = remote.ErrEITObservationUnsupported
	ErrLogoObservationUnsupported = remote.ErrLogoObservationUnsupported
	ErrStreamManagerShutdown      = errors.New("stream manager is shut down")
	ErrSessionAcquireFailed       = errors.New("failed to acquire a stable stream session")
	ErrTunerNotFound              = tuner.ErrTunerNotFound
	ErrUnsupportedTuner           = tuner.ErrUnsupportedTuner
	ErrTunerUnavailable           = tuner.ErrTunerUnavailable
	errBroadcastStopped           = source.ErrBroadcastStopped
)

// newRemoteClient is a test seam allowing manager tests to stub the upstream
// HTTP client of every remote.
var newRemoteClient = func(cfg config.RemoteConfig) *remote.Client {
	return remote.NewClient(cfg)
}

type ServiceLister = remote.ServiceLister

type ProgramUpdater = remote.ProgramUpdater
