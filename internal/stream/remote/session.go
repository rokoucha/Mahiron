package remote

import (
	"context"
	"sort"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/stream/channel"
	"github.com/21S1298001/mahiron/internal/stream/source"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
)

type SessionConfig struct {
	Client *Client
	Handle source.InputHandle
}

// Session adds remote API-backed operations to the shared TS ChannelSession.
type Session struct {
	*channel.ChannelSession
	client       *Client
	input        source.ChannelInput
	remote       string
	routeChannel config.ChannelConfig
}

func NewSession(config SessionConfig) *Session {
	metadata := config.Handle.Metadata()
	return &Session{
		ChannelSession: channel.NewChannelSession(channel.Config{Channel: metadata.PublicChannel.Channel, Handle: config.Handle, Type: metadata.PublicChannel.Type}),
		client:         config.Client,
		input:          config.Handle.Input(),
		remote:         metadata.Remote,
		routeChannel:   metadata.RouteChannel,
	}
}

func (s *Session) RemoteName() string { return s.remote }

func (s *Session) MatchesTuner(status tuner.Status) bool {
	return status.TunedChannelType == s.routeChannel.Type && status.TunedChannel == s.routeChannel.Channel ||
		status.CurrentChannelType == s.routeChannel.Type && status.CurrentChannel == s.routeChannel.Channel
}

func (s *Session) Users() []tuner.User {
	provider, ok := s.input.(interface{ Users() []tuner.User })
	if !ok {
		return nil
	}
	users := provider.Users()
	sort.Slice(users, func(i, j int) bool { return users[i].ID < users[j].ID })
	return users
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
		image := &ts.LogoImage{OriginalNetworkID: svc.NetworkID, LogoID: uint16(*svc.LogoID), LogoVersion: *remoteLogoVersion(), DownloadDataID: *remoteLogoDownloadDataID(svc), LogoType: 5, Data: data}
		if err := observe(image); err != nil {
			return err
		}
	}
	return nil
}
