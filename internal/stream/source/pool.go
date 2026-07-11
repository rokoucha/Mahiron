package source

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/stream/remote"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/google/uuid"
)

type LiveSource interface {
	Start(context.Context, io.Writer) error
	Stop(context.Context) error
	Done() <-chan struct{}
	Err() error
	WithUser(context.Context, func(context.Context) error) error
}

type Lease struct {
	Broadcast   *Broadcast
	Descrambler Descrambler
	RouteType   string
	Remote      *remote.Session
	Source      LiveSource
}

type Pool struct {
	channels           config.ChannelsConfig
	descramblerFactory DescramblerFactory
	mu                 sync.Mutex
	remotes            map[string]*remote.Client
	routeSourceCreates map[routeSourceKey]chan struct{}
	routeSources       map[routeSourceKey]*sharedRouteSource
	tunerManager       TunerManager
}

func NewPool(channels config.ChannelsConfig, tunerManager TunerManager, descramblerFactory DescramblerFactory, remotes map[string]*remote.Client) *Pool {
	return &Pool{
		channels:           channels,
		descramblerFactory: descramblerFactory,
		remotes:            remotes,
		routeSourceCreates: map[routeSourceKey]chan struct{}{},
		routeSources:       map[routeSourceKey]*sharedRouteSource{},
		tunerManager:       tunerManager,
	}
}

func (p *Pool) Acquire(ctx context.Context, channelType, channel string, wait bool) (lease *Lease, err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanStreamSourceAcquire,
		observability.AttrChannelType.String(channelType),
		observability.AttrChannelID.String(channel),
		observability.AttrWait.Bool(wait),
	)
	defer func() { observability.EndSpan(span, err) }()

	channelConfig := p.findChannel(channelType, channel)
	if channelConfig == nil {
		return nil, remote.ErrChannelNotFound
	}

	selected, err := p.selectRoute(ctx, channelConfig, wait)
	if err != nil {
		return nil, err
	}
	if selected.route.Remote != "" {
		return p.remoteLease(channelType, channel, selected)
	}
	return p.localLease(channelType, channel, selected), nil
}

func (p *Pool) remoteLease(channelType, channel string, selected routeSelection) (*Lease, error) {
	client := p.remotes[selected.route.Remote]
	if client == nil {
		return nil, tuner.ErrTunerNotFound
	}
	slog.Debug("selected remote stream route", "type", channelType, "channel", channel, "routeType", selected.route.Type, "remote", selected.route.Remote)
	return &Lease{
		RouteType: selected.route.Type,
		Remote: remote.NewSession(remote.SessionConfig{
			Client: client,
			Channel: &config.ChannelConfig{
				Type:    channelType,
				Channel: channel,
			},
			Remote:       selected.route.Remote,
			RouteChannel: &selected.channel,
		}),
	}, nil
}

func (p *Pool) localLease(channelType, channel string, selected routeSelection) *Lease {
	decoderCommand := selected.decoder
	if decoderCommand == "" {
		if provider, ok := p.tunerManager.(DecoderCommandProvider); ok {
			decoderCommand = provider.DecoderCommandByType(selected.route.Type)
		}
	}

	var descrambler Descrambler
	if decoderCommand != "" && p.descramblerFactory != nil {
		descrambler = p.descramblerFactory(decoderCommand)
	}

	slog.Debug("selected local stream route", "type", channelType, "channel", channel, "routeType", selected.route.Type, "decoder", decoderCommand != "")
	return &Lease{
		Broadcast:   selected.broadcast,
		Descrambler: descrambler,
		RouteType:   selected.route.Type,
		Source: &tunerLiveSource{
			channel: &config.ChannelConfig{Type: channelType, Channel: channel},
			device:  selected.device,
		},
	}
}

func (p *Pool) findChannel(channelType, channel string) *config.ChannelConfig {
	for i := range p.channels {
		if p.channels[i].Type == channelType && p.channels[i].Channel == channel &&
			!config.IsChannelDisabled(p.channels[i]) {
			return &p.channels[i]
		}
	}
	return nil
}

func (p *Pool) selectRoute(ctx context.Context, channel *config.ChannelConfig, wait bool) (selected routeSelection, err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanStreamSourceSelectRoute,
		observability.AttrChannelType.String(channel.Type),
		observability.AttrChannelID.String(channel.Channel),
		observability.AttrWait.Bool(wait),
		observability.AttrRouteCount.Int(len(channel.RoutesOrDefault())),
	)
	defer func() { observability.EndSpan(span, err) }()

	routes := enabledRoutes(channel.RoutesOrDefault())
	for {
		var lastErr error
		unavailable := false
		for _, route := range routes {
			selected, err := p.tryRoute(ctx, channel, route, wait)
			if err == nil {
				return selected, nil
			}
			slog.Debug("stream route unavailable", "type", channel.Type, "channel", channel.Channel, "routeType", route.Type, "remote", route.Remote, "err", err)
			if errors.Is(err, tuner.ErrTunerUnavailable) {
				unavailable = true
			}
			lastErr = err
		}
		if !wait || !unavailable {
			if lastErr != nil {
				return routeSelection{}, lastErr
			}
			return routeSelection{}, remote.ErrChannelNotFound
		}
		if err := waitForRouteRetry(ctx, channel); err != nil {
			return routeSelection{}, err
		}
	}
}

type routeSelection struct {
	route     config.ChannelRouteConfig
	channel   config.ChannelConfig
	device    TunerDevice
	decoder   string
	broadcast *Broadcast
}

func (p *Pool) tryRoute(ctx context.Context, channel *config.ChannelConfig, route config.ChannelRouteConfig, wait bool) (selected routeSelection, err error) {
	routeChannel := channel.RouteChannelConfig(route)
	routeCtx, routeSpan := observability.StartSpan(ctx, observability.SpanStreamSourceTryRoute,
		observability.AttrChannelType.String(channel.Type),
		observability.AttrChannelID.String(channel.Channel),
		observability.AttrRouteType.String(route.Type),
		observability.AttrRouteChannel.String(route.Channel),
		observability.AttrRouteRemote.String(route.Remote),
	)
	defer func() { observability.EndSpan(routeSpan, err) }()

	slog.Debug("trying stream route", "type", channel.Type, "channel", channel.Channel, "routeType", route.Type, "remote", route.Remote, "wait", wait)
	if route.Remote != "" {
		selected, err = p.tryRemoteRoute(routeCtx, route, routeChannel)
	} else {
		selected, err = p.tryLocalRoute(routeCtx, channel, route, routeChannel, wait)
	}
	if err != nil {
		return routeSelection{}, err
	}
	slog.Debug("stream route acquired", "type", channel.Type, "channel", channel.Channel, "routeType", route.Type, "remote", route.Remote)
	return selected, nil
}

func (p *Pool) tryRemoteRoute(ctx context.Context, route config.ChannelRouteConfig, routeChannel config.ChannelConfig) (routeSelection, error) {
	client := p.remotes[route.Remote]
	if client == nil {
		return routeSelection{}, tuner.ErrTunerNotFound
	}
	if err := client.CheckAvailableForRoute(ctx, route.Type, route.Channel); err != nil {
		return routeSelection{}, err
	}
	return routeSelection{route: route, channel: routeChannel}, nil
}

func (p *Pool) tryLocalRoute(ctx context.Context, channel *config.ChannelConfig, route config.ChannelRouteConfig, routeChannel config.ChannelConfig, wait bool) (routeSelection, error) {
	key := newRouteSourceKey(route)
	source, finishCreate, err := p.beginRouteSourceCreate(ctx, key)
	if err != nil {
		return routeSelection{}, err
	}
	if source != nil {
		slog.Debug("reusing local stream route", "type", channel.Type, "channel", channel.Channel, "routeType", route.Type)
		return routeSelection{route: route, channel: routeChannel, decoder: source.decoderCommand, broadcast: source.broadcast}, nil
	}
	defer finishCreate()

	var device TunerDevice
	var decoder string
	if allocator, ok := p.tunerManager.(TunerAllocator); ok {
		device, decoder, err = allocator.AcquireDevice(ctx, route.Type, channel, &routeChannel, wait)
	} else {
		device, err = p.tunerManager.NewDeviceByType(route.Type, &routeChannel)
	}
	if err != nil {
		return routeSelection{}, err
	}

	broadcast := p.commitRouteSource(key, &tunerLiveSource{
		channel: &config.ChannelConfig{Type: channel.Type, Channel: channel.Channel},
		device:  device,
	}, decoder)
	return routeSelection{route: route, channel: routeChannel, device: device, decoder: decoder, broadcast: broadcast}, nil
}

func waitForRouteRetry(ctx context.Context, channel *config.ChannelConfig) error {
	slog.Debug("waiting for stream route", "type", channel.Type, "channel", channel.Channel)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(100 * time.Millisecond):
		return nil
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

type tunerUserStreamInfoDevice interface {
	UpdateUserStreamInfo(string, string, tuner.StreamInfo)
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

func (s *tunerLiveSource) WithUser(ctx context.Context, runFn func(context.Context) error) error {
	device, ok := s.device.(tunerUserDevice)
	if !ok {
		return runFn(ctx)
	}
	user, ok := tuner.UserFromContext(ctx)
	if !ok {
		agent := "Mahiron Internal"
		if info, ok := run.JobInfoFromContext(ctx); ok && info.Name != "" {
			agent = info.Name
		}
		user = tuner.User{
			ID:            uuid.NewString(),
			Priority:      -1,
			Agent:         agent,
			StreamSetting: tuner.StreamSetting{Channel: s.channel},
		}
	}
	device.AddUser(user)
	defer device.RemoveUser(user.ID)
	if infoDevice, ok := s.device.(tunerUserStreamInfoDevice); ok {
		ctx = tuner.WithStreamInfoReporter(ctx, infoDevice.UpdateUserStreamInfo)
	}
	return runFn(ctx)
}
