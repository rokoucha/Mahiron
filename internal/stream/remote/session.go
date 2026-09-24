package remote

import (
	"context"
	"sort"

	"github.com/21S1298001/mahiron/internal/bml"
	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/mirakurun"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/stream/channel"
	"github.com/21S1298001/mahiron/internal/stream/source"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
)

type SessionConfig struct {
	Client      *Client
	Handle      source.InputHandle
	ModuleStore bml.ModuleStore
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
		ChannelSession: channel.NewChannelSession(channel.Config{Channel: metadata.PublicChannel.Channel, Handle: config.Handle, Type: metadata.PublicChannel.Type, ModuleStore: config.ModuleStore}),
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

func (s *Session) ScanServices(ctx context.Context) ([]model.Service, error) {
	return s.client.ScanServices(ctx, s.routeChannel.Type, s.routeChannel.Channel)
}

func (s *Session) ListServicePrograms(ctx context.Context, networkID, serviceID uint16) ([]model.Event, error) {
	return s.client.ListServicePrograms(ctx, networkID, serviceID)
}

// CollectSchedule is not supported: EPG gathering copies the remote's stored
// programs through ListServicePrograms instead.
func (s *Session) CollectSchedule(context.Context, func(model.ScheduleUpdate) error, func(model.PresentFollowing) error) error {
	return ErrEITObservationUnsupported
}

// ObserveLogos reports the logos of the remote's services, completed with
// the common fixed palette like the logos a local session decodes.
func (s *Session) ObserveLogos(ctx context.Context, observe func(model.Logo) error) error {
	services, err := s.client.ListChannelServices(ctx, s.routeChannel.Type, s.routeChannel.Channel)
	if err != nil {
		return err
	}
	for i := range services {
		// The scan conversion holds the only logo heuristic: a logo counts
		// only when the remote reports both an ID and actual logo data.
		scan := mirakurun.ScanServiceModelFromAPI(&services[i])
		if scan.Logo == nil || scan.Logo.Version == nil || scan.Logo.DownloadDataID == nil {
			continue
		}
		data, err := s.client.GetLogoImage(ctx, scan.Key.MirakurunID())
		if err != nil {
			return err
		}
		data, err = ts.NormalizeARIBLogoPNG(data)
		if err != nil {
			return err
		}
		logo := model.Logo{NetworkID: scan.Key.NetworkID, LogoID: scan.Logo.LogoID, Version: *scan.Logo.Version, DownloadDataID: *scan.Logo.DownloadDataID, LogoType: 5, Data: data}
		if err := observe(logo); err != nil {
			return err
		}
	}
	return nil
}
