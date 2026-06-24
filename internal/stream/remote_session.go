package stream

import (
	"context"
	"io"
	"log/slog"
	"sync"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/ts"
)

type RemoteSessionConfig struct {
	Client       *RemoteClient
	Channel      *config.ChannelConfig
	RouteChannel *config.ChannelConfig
}

type RemoteSession struct {
	channel      *config.ChannelConfig
	client       *RemoteClient
	eventCancel  context.CancelFunc
	eventOnce    sync.Once
	routeChannel *config.ChannelConfig
}

func NewRemoteSession(config RemoteSessionConfig) *RemoteSession {
	return &RemoteSession{
		channel:      config.Channel,
		client:       config.Client,
		routeChannel: config.RouteChannel,
	}
}

func (s *RemoteSession) ChannelStream(ctx context.Context, decode bool, dst io.Writer) error {
	return s.client.ChannelStream(ctx, s.routeChannel.Type, s.routeChannel.Channel, decode, dst)
}

func (s *RemoteSession) ServiceStream(ctx context.Context, serviceID uint16, decode bool, dst io.Writer) error {
	return s.client.ServiceStream(ctx, s.routeChannel.Type, s.routeChannel.Channel, serviceID, decode, dst)
}

func (s *RemoteSession) ProgramStream(ctx context.Context, p *program.Program, decode bool, dst io.Writer) error {
	return s.client.ProgramStream(ctx, p.ID, decode, dst)
}

func (s *RemoteSession) ScanServices(ctx context.Context) ([]ts.ServiceInfo, error) {
	return s.client.ScanServices(ctx, s.routeChannel.Type, s.routeChannel.Channel)
}

func (s *RemoteSession) ListServicePrograms(ctx context.Context, networkID, serviceID uint16) ([]*program.Program, error) {
	return s.client.ListServicePrograms(ctx, networkID, serviceID)
}

func (s *RemoteSession) StartProgramEventSync(updater ProgramUpdater) {
	if updater == nil {
		return
	}
	s.eventOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		s.eventCancel = cancel
		go func() {
			slog.Debug("starting remote program event sync")
			defer slog.Debug("finished remote program event sync")
			if err := s.client.StreamProgramEvents(ctx, updater); err != nil && ctx.Err() == nil {
				slog.Warn("remote program event sync stopped", "err", err)
			}
		}()
	})
}

func (s *RemoteSession) CollectEIT(context.Context, func(*ts.EIT) error) error {
	return ErrEITObservationUnsupported
}

func (s *RemoteSession) ObserveLogos(context.Context, func(*ts.LogoImage) error) error {
	return ErrLogoObservationUnsupported
}

func (s *RemoteSession) Stop(context.Context) error {
	if s.eventCancel != nil {
		s.eventCancel()
	}
	return nil
}
