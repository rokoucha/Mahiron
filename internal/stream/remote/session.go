package remote

import (
	"context"
	"io"
	"sort"
	"sync"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/stream/databroadcast"
	"github.com/21S1298001/mahiron/internal/stream/demux"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
)

type SessionConfig struct {
	Client       *Client
	Channel      *config.ChannelConfig
	Remote       string
	RouteChannel *config.ChannelConfig
}

type Session struct {
	channel       *config.ChannelConfig
	client        *Client
	remote        string
	routeChannel  *config.ChannelConfig
	mu            sync.Mutex
	users         map[string]remoteUser
	dataBroadcast *databroadcast.DataBroadcastHub
}

type remoteUser struct {
	user tuner.User
	refs int
}

func NewSession(config SessionConfig) *Session {
	return &Session{
		channel:       config.Channel,
		client:        config.Client,
		remote:        config.Remote,
		routeChannel:  config.RouteChannel,
		users:         map[string]remoteUser{},
		dataBroadcast: databroadcast.NewDataBroadcastHub(),
	}
}

func (s *Session) ChannelStream(ctx context.Context, decode bool, dst io.Writer) error {
	return s.withUser(ctx, func() error {
		return s.client.ChannelStream(ctx, s.routeChannel.Type, s.routeChannel.Channel, decode, dst)
	})
}

func (s *Session) ServiceStream(ctx context.Context, serviceID uint16, decode bool, dst io.Writer) error {
	return s.withUser(ctx, func() error {
		return s.client.ServiceStream(ctx, s.routeChannel.Type, s.routeChannel.Channel, serviceID, decode, dst)
	})
}

func (s *Session) ProgramStream(ctx context.Context, p *program.Program, decode bool, dst io.Writer) error {
	return s.withUser(ctx, func() error {
		return s.client.ProgramStream(ctx, p.ID, decode, dst)
	})
}

// RemoteName identifies the configured remote used by this session.
func (s *Session) RemoteName() string { return s.remote }

// MatchesTuner reports whether the remote tuner is serving this session's route.
func (s *Session) MatchesTuner(status tuner.Status) bool {
	return status.TunedChannelType == s.routeChannel.Type && status.TunedChannel == s.routeChannel.Channel ||
		status.CurrentChannelType == s.routeChannel.Type && status.CurrentChannel == s.routeChannel.Channel
}

// Users returns the local users currently streaming through this session.
func (s *Session) Users() []tuner.User {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]tuner.User, 0, len(s.users))
	for _, tracked := range s.users {
		result = append(result, tracked.user)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (s *Session) withUser(ctx context.Context, run func() error) error {
	user, ok := tuner.UserFromContext(ctx)
	if !ok || user.ID == "" {
		return run()
	}
	s.addUser(user)
	defer s.removeUser(user.ID)
	return run()
}

func (s *Session) addUser(user tuner.User) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tracked := s.users[user.ID]
	tracked.user = user
	tracked.refs++
	s.users[user.ID] = tracked
}

func (s *Session) removeUser(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tracked := s.users[id]
	if tracked.refs <= 1 {
		delete(s.users, id)
		return
	}
	tracked.refs--
	s.users[id] = tracked
}

func (s *Session) ScanServices(ctx context.Context) ([]ts.ServiceInfo, error) {
	return s.client.ScanServices(ctx, s.routeChannel.Type, s.routeChannel.Channel)
}

func (s *Session) ListServicePrograms(ctx context.Context, networkID, serviceID uint16) ([]*program.Program, error) {
	return s.client.ListServicePrograms(ctx, networkID, serviceID)
}

func (s *Session) CollectEIT(context.Context, func(*ts.EIT) error) error {
	return ErrEITObservationUnsupported
}

func (s *Session) ObserveLogos(ctx context.Context, observe func(*ts.LogoImage) error) error {
	services, err := s.client.ListChannelServices(ctx, s.routeChannel.Type, s.routeChannel.Channel)
	if err != nil {
		return err
	}
	for _, svc := range services {
		if !remoteServiceHasLogo(svc) {
			continue
		}
		data, err := s.client.GetLogoImage(ctx, int64(svc.NetworkID)*100000+int64(svc.ServiceID))
		if err != nil {
			return err
		}
		image := &ts.LogoImage{
			OriginalNetworkID: svc.NetworkID,
			LogoID:            uint16(*svc.LogoID),
			LogoVersion:       *remoteLogoVersion(),
			DownloadDataID:    *remoteLogoDownloadDataID(svc),
			LogoType:          5,
			Data:              data,
		}
		if err := observe(image); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) ObserveDataBroadcast(ctx context.Context, serviceID uint16, decode bool, observe func(databroadcast.DataBroadcastEvent) error) error {
	// Keep the requesting client associated with this remote session for the
	// full lifetime of the observation. The demuxer's source context is
	// intentionally detached from the HTTP request context.
	return s.withUser(ctx, func() error {
		return s.observeDataBroadcast(ctx, serviceID, decode, observe)
	})
}

func (s *Session) observeDataBroadcast(ctx context.Context, serviceID uint16, decode bool, observe func(databroadcast.DataBroadcastEvent) error) error {
	snapshot, events, unsubscribe := s.dataBroadcast.Subscribe(ctx, serviceID)
	defer unsubscribe()
	if err := observe(databroadcast.DataBroadcastEvent{Type: "snapshot", Snapshot: snapshot}); err != nil {
		return err
	}

	// Remotes only need Mirakurun-compatible stream endpoints. Decode the
	// received transport stream here, just as local sessions do.
	demuxer := demux.New(func(streamCtx context.Context, dst io.Writer) error {
		return s.client.ChannelStream(streamCtx, s.routeChannel.Type, s.routeChannel.Channel, decode, dst)
	}, nil).WithPIDSections(s.dataBroadcast.Observe)
	defer demuxer.Stop()

	observeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- demuxer.ObserveSections(observeCtx, acceptDataBroadcastSection, func(ts.Section) error {
			return nil
		})
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-done:
			return err
		case event, ok := <-events:
			if !ok {
				return nil
			}
			if err := observe(event); err != nil {
				return err
			}
		}
	}
}

func (s *Session) DataBroadcastModule(serviceID uint16, componentTag byte, moduleID uint16) (databroadcast.DataBroadcastModule, bool) {
	return s.dataBroadcast.Module(serviceID, componentTag, moduleID)
}

func acceptDataBroadcastSection(section ts.Section) bool {
	switch section.TableID() {
	case ts.TableIDPMT, ts.TableIDDSMCCDII, ts.TableIDDSMCCDDB, ts.TableIDTOT:
		return true
	default:
		return ts.IsEITPF(section.TableID())
	}
}

func (s *Session) Stop(context.Context) error {
	return nil
}
