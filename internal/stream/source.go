package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/tuner"
	"github.com/google/uuid"
)

type LiveSource interface {
	Start(context.Context, io.Writer) error
	Stop(context.Context) error
	Done() <-chan struct{}
	Err() error
	WithUser(context.Context, func() error) error
}

type SourceLease struct {
	Channel     *config.ChannelConfig
	Descrambler Descrambler
	RouteType   string
	Session     Session
	Source      LiveSource
}

type SourcePool struct {
	channels           config.ChannelsConfig
	descramblerFactory DescramblerFactory
	remotes            map[string]*RemoteClient
	tunerManager       TunerManager
}

func NewSourcePool(channels config.ChannelsConfig, tunerManager TunerManager, descramblerFactory DescramblerFactory, remotes map[string]*RemoteClient) *SourcePool {
	return &SourcePool{
		channels:           channels,
		descramblerFactory: descramblerFactory,
		remotes:            remotes,
		tunerManager:       tunerManager,
	}
}

func (p *SourcePool) Acquire(ctx context.Context, channelType, channel string, wait bool) (*SourceLease, error) {
	channelConfig := p.findChannel(channelType, channel)
	if channelConfig == nil {
		return nil, ErrChannelNotFound
	}
	if channelConfig.IsDisabled != nil && *channelConfig.IsDisabled {
		return nil, ErrChannelNotFound
	}

	route, routeChannelConfig, device, decoderCommand, err := p.newRouteDevice(ctx, channelConfig, wait)
	if err != nil {
		return nil, err
	}
	if route.Remote != "" {
		remote := p.remotes[route.Remote]
		if remote == nil {
			return nil, ErrTunerNotFound
		}
		slog.Debug("selected remote stream route", "type", channelType, "channel", channel, "routeType", route.Type, "remote", route.Remote)
		return &SourceLease{
			Channel:   &routeChannelConfig,
			RouteType: route.Type,
			Session: NewRemoteSession(RemoteSessionConfig{
				Client: remote,
				Channel: &config.ChannelConfig{
					Type:    channelType,
					Channel: channel,
				},
				RouteChannel: &routeChannelConfig,
			}),
		}, nil
	}

	if decoderCommand == "" {
		if provider, ok := p.tunerManager.(DecoderCommandProvider); ok {
			decoderCommand = provider.DecoderCommandByType(route.Type)
		}
	}

	var descrambler Descrambler
	if decoderCommand != "" && p.descramblerFactory != nil {
		descrambler = p.descramblerFactory(decoderCommand)
	}

	slog.Debug("selected local stream route", "type", channelType, "channel", channel, "routeType", route.Type, "decoder", decoderCommand != "")
	return &SourceLease{
		Channel:     &routeChannelConfig,
		Descrambler: descrambler,
		RouteType:   route.Type,
		Source: &tunerLiveSource{
			channel: &config.ChannelConfig{Type: channelType, Channel: channel},
			device:  device,
		},
	}, nil
}

func (p *SourcePool) findChannel(channelType, channel string) *config.ChannelConfig {
	for i := range p.channels {
		if p.channels[i].Type == channelType && p.channels[i].Channel == channel {
			return &p.channels[i]
		}
	}
	return nil
}

func (p *SourcePool) newRouteDevice(ctx context.Context, channel *config.ChannelConfig, wait bool) (config.ChannelRouteConfig, config.ChannelConfig, TunerDevice, string, error) {
	routes := enabledRoutes(channel.RoutesOrDefault())
	for {
		var lastErr error
		unavailable := false
		for _, route := range routes {
			routeChannel := channel.RouteChannelConfig(route)
			slog.Debug("trying stream route", "type", channel.Type, "channel", channel.Channel, "routeType", route.Type, "remote", route.Remote, "wait", wait)
			var device TunerDevice
			var decoder string
			var err error
			if route.Remote != "" {
				remote := p.remotes[route.Remote]
				if remote == nil {
					err = tuner.ErrTunerNotFound
				} else if err = remote.CheckAvailable(ctx, route.Type); err == nil {
					return route, routeChannel, nil, "", nil
				} else {
					err = tuner.ErrTunerUnavailable
				}
			} else if allocator, ok := p.tunerManager.(TunerAllocator); ok {
				device, decoder, err = allocator.AcquireDevice(ctx, route.Type, channel, &routeChannel, false)
			} else {
				device, err = p.tunerManager.NewDeviceByType(route.Type, &routeChannel)
			}
			if err == nil {
				slog.Debug("stream route acquired", "type", channel.Type, "channel", channel.Channel, "routeType", route.Type, "remote", route.Remote)
				return route, routeChannel, device, decoder, nil
			}
			slog.Debug("stream route unavailable", "type", channel.Type, "channel", channel.Channel, "routeType", route.Type, "remote", route.Remote, "err", err)
			if errors.Is(err, tuner.ErrTunerUnavailable) {
				unavailable = true
			}
			lastErr = err
		}
		if !wait || !unavailable {
			if lastErr != nil {
				return config.ChannelRouteConfig{}, config.ChannelConfig{}, nil, "", lastErr
			}
			return config.ChannelRouteConfig{}, config.ChannelConfig{}, nil, "", ErrChannelNotFound
		}
		slog.Debug("waiting for stream route", "type", channel.Type, "channel", channel.Channel)
		select {
		case <-ctx.Done():
			return config.ChannelRouteConfig{}, config.ChannelConfig{}, nil, "", ctx.Err()
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

type tunerUserDevice interface {
	AddUser(tuner.User)
	RemoveUser(string)
}

type tunerLiveSource struct {
	channel *config.ChannelConfig
	device  TunerDevice
}

func (s *tunerLiveSource) Start(ctx context.Context, dst io.Writer) error {
	return s.device.Start(ctx, dst)
}

func (s *tunerLiveSource) Stop(ctx context.Context) error {
	return s.device.Stop(ctx)
}

func (s *tunerLiveSource) Done() <-chan struct{} {
	return s.device.Done()
}

func (s *tunerLiveSource) Err() error {
	return s.device.Err()
}

func (s *tunerLiveSource) WithUser(ctx context.Context, run func() error) error {
	device, ok := s.device.(tunerUserDevice)
	if !ok {
		return run()
	}
	user, ok := tuner.UserFromContext(ctx)
	if !ok {
		user = tuner.User{
			ID:            uuid.NewString(),
			Agent:         "Mahiron Internal",
			StreamSetting: tuner.StreamSetting{Channel: s.channel},
		}
	}
	device.AddUser(user)
	defer device.RemoveUser(user.ID)
	return run()
}
