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
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
	"github.com/google/uuid"
)

type LiveSource interface {
	Start(context.Context, io.Writer) error
	Stop(context.Context) error
	Done() <-chan struct{}
	Err() error
	WithUser(context.Context, func(context.Context) error) error
}

type StreamVariant uint8

const (
	StreamRaw StreamVariant = iota
	StreamDecoded
)

// ChannelInput is the narrow transport interface consumed by a channel
// session. User accounting belongs to the input so the acquisition user is
// retained for each subscription's full lifetime.
type ChannelInput interface {
	Subscribe(context.Context, StreamVariant, io.Writer) error
	WithUser(context.Context, func(context.Context) error) error
}

type InputMetadata struct {
	PublicChannel config.ChannelConfig
	Remote        string
	RouteChannel  config.ChannelConfig
}

type RemoteClient interface {
	CheckAvailableForRoute(context.Context, string, string) error
	ChannelStream(context.Context, string, string, bool, io.Writer) error
}

// ScanRemoteServices reads a remote channel's already-scanned services without
// acquiring a tuner. handled is false when the selected route is local.
func (p *Pool) ScanRemoteServices(ctx context.Context, channelType, channel string) (services []ts.ServiceInfo, handled bool, err error) {
	channelConfig := p.findChannel(channelType, channel)
	if channelConfig == nil {
		return nil, true, ErrChannelNotFound
	}
	var lastErr error
	for _, route := range enabledRoutes(channelConfig.RoutesOrDefault()) {
		if route.Remote == "" {
			return nil, false, nil
		}
		client := p.remotes[route.Remote]
		if client == nil {
			lastErr = tuner.ErrTunerNotFound
			continue
		}
		scanner, ok := client.(interface {
			ScanServices(context.Context, string, string) ([]ts.ServiceInfo, error)
		})
		if !ok {
			return nil, false, nil
		}
		services, err := scanner.ScanServices(ctx, route.Type, route.Channel)
		if err == nil {
			return services, true, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return nil, true, lastErr
	}
	return nil, true, ErrChannelNotFound
}

type InputHandle interface {
	Input() ChannelInput
	Descrambler() Descrambler
	Metadata() InputMetadata
	RouteType() string
	SourceLabel() string
	Release(context.Context) error
}

type inputHandle struct {
	input       ChannelInput
	descrambler Descrambler
	metadata    InputMetadata
	routeType   string
	sourceLabel string
}

func (h *inputHandle) Input() ChannelInput           { return h.input }
func (h *inputHandle) Descrambler() Descrambler      { return h.descrambler }
func (h *inputHandle) Metadata() InputMetadata       { return h.metadata }
func (h *inputHandle) RouteType() string             { return h.routeType }
func (h *inputHandle) SourceLabel() string           { return h.sourceLabel }
func (h *inputHandle) Release(context.Context) error { return nil }

type Pool struct {
	channels           config.ChannelsConfig
	descramblerFactory DescramblerFactory
	mu                 sync.Mutex
	remotes            map[string]RemoteClient
	routeSourceCreates map[routeSourceKey]chan struct{}
	routeSources       map[routeSourceKey]*sharedRouteSource
	tunerManager       TunerManager
}

func NewPool(channels config.ChannelsConfig, tunerManager TunerManager, descramblerFactory DescramblerFactory, remotes map[string]RemoteClient) *Pool {
	return &Pool{
		channels:           channels,
		descramblerFactory: descramblerFactory,
		remotes:            remotes,
		routeSourceCreates: map[routeSourceKey]chan struct{}{},
		routeSources:       map[routeSourceKey]*sharedRouteSource{},
		tunerManager:       tunerManager,
	}
}

func (p *Pool) Acquire(ctx context.Context, channelType, channel string, wait bool) (handle InputHandle, err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanStreamSourceAcquire,
		observability.AttrChannelType.String(channelType),
		observability.AttrChannelID.String(channel),
		observability.AttrWait.Bool(wait),
	)
	defer func() { observability.EndSpan(span, err) }()

	channelConfig := p.findChannel(channelType, channel)
	if channelConfig == nil {
		return nil, ErrChannelNotFound
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

// CheckAvailable verifies that at least one route can currently be acquired
// without reserving a tuner or creating a stream source.
func (p *Pool) CheckAvailable(ctx context.Context, channelType, channel string) error {
	channelConfig := p.findChannel(channelType, channel)
	if channelConfig == nil {
		return ErrChannelNotFound
	}
	var lastErr error
	for _, route := range enabledRoutes(channelConfig.RoutesOrDefault()) {
		if route.Remote != "" {
			client := p.remotes[route.Remote]
			if client == nil {
				lastErr = tuner.ErrTunerNotFound
				continue
			}
			if err := client.CheckAvailableForRoute(ctx, route.Type, route.Channel); err == nil {
				return nil
			} else {
				lastErr = err
			}
			continue
		}
		key := newRouteSourceKey(route)
		p.mu.Lock()
		source := p.routeSources[key]
		p.mu.Unlock()
		if source != nil {
			return nil
		}
		if checker, ok := p.tunerManager.(TunerAvailabilityChecker); ok {
			if err := checker.CheckAvailable(ctx, route.Type); err == nil {
				return nil
			} else {
				lastErr = err
			}
			continue
		}
		// Custom managers predating the optional checker cannot be inspected
		// without allocating a device, so preserve their historical behavior.
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return ErrChannelNotFound
}

// Shutdown stops physical inputs owned by the pool. Releasing an individual
// session handle intentionally does not do this because routes may be shared.
func (p *Pool) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	broadcasts := make([]*Broadcast, 0, len(p.routeSources))
	for _, shared := range p.routeSources {
		broadcasts = append(broadcasts, shared.broadcast)
	}
	p.mu.Unlock()
	var result error
	for _, broadcast := range broadcasts {
		result = errors.Join(result, broadcast.Stop(ctx))
	}
	return result
}

func (p *Pool) remoteLease(channelType, channel string, selected routeSelection) (InputHandle, error) {
	client := p.remotes[selected.route.Remote]
	if client == nil {
		return nil, tuner.ErrTunerNotFound
	}
	slog.Debug("selected remote stream route", "type", channelType, "channel", channel, "routeType", selected.route.Type, "remote", selected.route.Remote)
	return NewRemoteInputHandle(client, config.ChannelConfig{Type: channelType, Channel: channel}, selected.channel, selected.route.Remote, selected.route.Type), nil
}

func (p *Pool) localLease(channelType, channel string, selected routeSelection) InputHandle {
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
	broadcast := selected.broadcast
	if broadcast == nil {
		broadcast = NewBroadcast(&tunerLiveSource{
			channel: &config.ChannelConfig{Type: channelType, Channel: channel},
			device:  selected.device,
		}, nil)
	}
	return &inputHandle{
		input:       broadcastInput{broadcast},
		descrambler: descrambler,
		metadata: InputMetadata{
			PublicChannel: config.ChannelConfig{Type: channelType, Channel: channel},
			RouteChannel:  selected.channel,
		},
		routeType:   selected.route.Type,
		sourceLabel: "local",
	}
}

type broadcastInput struct{ *Broadcast }

func (i broadcastInput) Subscribe(ctx context.Context, _ StreamVariant, dst io.Writer) error {
	return i.SubscribeRaw(ctx, dst)
}

type remoteInputHandle struct {
	inputHandle
}

func NewRemoteInputHandle(client RemoteClient, channel, routeChannel config.ChannelConfig, remoteName, routeType string) InputHandle {
	return &remoteInputHandle{inputHandle: inputHandle{
		input: newRemoteInput(client, routeChannel, remoteName),
		metadata: InputMetadata{
			PublicChannel: channel,
			Remote:        remoteName,
			RouteChannel:  routeChannel,
		},
		routeType: routeType, sourceLabel: "remote",
	}}
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
			return routeSelection{}, ErrChannelNotFound
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

func (s *tunerLiveSource) StreamMetricLabels() (string, string) {
	if s.channel == nil {
		return "", ""
	}
	return s.channel.Type, s.channel.Channel
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
