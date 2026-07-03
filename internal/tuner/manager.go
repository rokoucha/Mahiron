package tuner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/observability"
)

const (
	eventTypeCreate = "create"
	eventTypeUpdate = "update"
)

type eventPublisher interface {
	PublishTunerStatusEvent(typ string, data map[string]any)
}

type TunerManager struct {
	tuners     []*Tuner
	mu         sync.Mutex
	inUse      map[*Tuner]bool
	runtime    map[*Tuner]*tunerRuntime
	nextByType map[string]int
	changed    chan struct{}
	events     eventPublisher
}

type TunerManagerConfig struct {
	TunersConfig config.TunersConfig
	EventHub     eventPublisher
}

func NewTunerManager(cfg *TunerManagerConfig) *TunerManager {
	tuners := make([]*Tuner, len(cfg.TunersConfig))
	runtime := make(map[*Tuner]*tunerRuntime, len(tuners))
	for i, tunerConfig := range cfg.TunersConfig {
		tuners[i] = NewTuner(tunerConfig)
		runtime[tuners[i]] = &tunerRuntime{users: make(map[string]*trackedUser)}
	}
	return &TunerManager{
		tuners:     tuners,
		inUse:      make(map[*Tuner]bool),
		runtime:    runtime,
		nextByType: make(map[string]int),
		changed:    make(chan struct{}),
		events:     cfg.EventHub,
	}
}

func (tm *TunerManager) Shutdown(context.Context) error { return nil }

func (tm *TunerManager) GetTuner(name string) *Tuner {
	for _, item := range tm.tuners {
		if item.Name() == name {
			return item
		}
	}
	return nil
}

func (tm *TunerManager) GetTunerByType(channelType string) *Tuner {
	for _, item := range tm.tuners {
		if !item.IsDisabled() && slices.Contains(item.Groups(), channelType) {
			return item
		}
	}
	return nil
}

// NewDeviceByType reserves one physical tuner and returns a device that releases
// that reservation when it stops.
func (tm *TunerManager) NewDeviceByType(channelType string, channel *config.ChannelConfig) (Device, error) {
	device, _, err := tm.AcquireDevice(context.Background(), channelType, channel, channel, false)
	return device, err
}

func (tm *TunerManager) AcquireDevice(ctx context.Context, channelType string, requestedChannel, tunedChannel *config.ChannelConfig, wait bool) (device Device, decoder string, err error) {
	start := time.Now()
	ctx, span := observability.StartSpan(ctx, observability.SpanTunerAcquireDevice,
		observability.AttrChannelType.String(channelType),
		observability.AttrChannelID.String(channelID(requestedChannel)),
		observability.AttrTunedChannelType.String(channelTypeOf(tunedChannel)),
		observability.AttrTunedChannelID.String(channelID(tunedChannel)),
		observability.AttrWait.Bool(wait),
	)
	defer func() {
		observability.RecordTunerAcquire(ctx, channelType, tunerAcquireResult(err), wait, time.Since(start).Milliseconds())
		observability.EndSpan(span, err)
	}()

	requestPriority := priorityFromContext(ctx)
	for {
		tm.mu.Lock()
		attempt := tm.tryAcquireLocked(channelType, requestPriority, requestedChannel, tunedChannel)
		tm.mu.Unlock()

		if attempt.device != nil {
			tm.publishStatus(eventTypeUpdate, attempt.status)
			return attempt.device, attempt.decoder, nil
		}
		if !attempt.found {
			slog.Warn("tuner not found", "type", channelType, "channel", channelID(requestedChannel))
			return nil, "", ErrTunerNotFound
		}
		if !attempt.usable {
			slog.Warn("tuner unsupported", "type", channelType, "channel", channelID(requestedChannel))
			return nil, "", ErrUnsupportedTuner
		}
		if attempt.grab.device != nil {
			slog.Info("grabbing tuner",
				"name", attempt.grab.tunerName,
				"type", channelType,
				"channel", channelID(requestedChannel),
				"priority", requestPriority,
				"victimPriority", attempt.grab.priority,
			)
			if err := attempt.grab.device.Stop(ctx); err != nil {
				return nil, "", err
			}
			select {
			case <-ctx.Done():
				return nil, "", ctx.Err()
			case <-attempt.changed:
			}
			continue
		}
		if !wait {
			slog.Debug("tuner unavailable", "type", channelType, "channel", channelID(requestedChannel))
			return nil, "", ErrTunerUnavailable
		}
		slog.Debug("waiting for tuner", "type", channelType, "channel", channelID(requestedChannel))
		select {
		case <-ctx.Done():
			slog.Debug("tuner wait canceled", "type", channelType, "channel", channelID(requestedChannel), "err", ctx.Err())
			return nil, "", ctx.Err()
		case <-attempt.changed:
		}
	}
}

func tunerAcquireResult(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, ErrTunerNotFound):
		return "not_found"
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

type acquireAttempt struct {
	device  Device
	decoder string
	found   bool
	usable  bool
	grab    grabCandidate
	changed <-chan struct{}
	status  Status
}

type grabCandidate struct {
	device    Device
	tunerName string
	priority  int
}

func priorityFromContext(ctx context.Context) int {
	if user, ok := UserFromContext(ctx); ok {
		return user.Priority
	}
	return 0
}

func (tm *TunerManager) tryAcquireLocked(channelType string, requestPriority int, requestedChannel, tunedChannel *config.ChannelConfig) acquireAttempt {
	result := acquireAttempt{changed: tm.changed}
	start := tm.nextByType[channelType]
	for offset := range len(tm.tuners) {
		index := (start + offset) % len(tm.tuners)
		item := tm.tuners[index]
		if item.IsDisabled() || !slices.Contains(item.Groups(), channelType) {
			continue
		}
		result.found = true
		if !item.Usable() {
			continue
		}
		result.usable = true
		runtime := tm.runtime[item]
		if runtime.fault {
			continue
		}
		if tm.inUse[item] {
			result.grab = betterGrabCandidate(result.grab, item, runtime, requestPriority)
			continue
		}
		managed, decoder, ok := tm.reserveLocked(item, requestPriority, requestedChannel, tunedChannel)
		if !ok {
			continue
		}
		tm.nextByType[channelType] = (index + 1) % len(tm.tuners)
		result.device = managed
		result.decoder = decoder
		result.status = tm.statusLocked(index)
		slog.Info("tuner acquired",
			"name", item.Name(),
			"type", channelType,
			"channel", channelID(requestedChannel),
			"tunedType", channelTypeOf(tunedChannel),
			"tunedChannel", channelID(tunedChannel),
			"decoder", decoder != "",
		)
		return result
	}
	return result
}

func betterGrabCandidate(current grabCandidate, item *Tuner, runtime *tunerRuntime, requestPriority int) grabCandidate {
	if runtime.device == nil {
		return current
	}
	effectivePriority := runtime.effectivePriority()
	if requestPriority <= effectivePriority {
		return current
	}
	if current.device != nil && current.priority <= effectivePriority {
		return current
	}
	return grabCandidate{
		device:    runtime.device,
		tunerName: item.Name(),
		priority:  effectivePriority,
	}
}

func (tm *TunerManager) reserveLocked(item *Tuner, priority int, requestedChannel, tunedChannel *config.ChannelConfig) (Device, string, bool) {
	base := item.NewDevice(tunedChannel)
	if base == nil {
		return nil, "", false
	}
	runtime := tm.runtime[item]
	tm.inUse[item] = true
	runtime.inUse = true
	runtime.running = false
	runtime.stopped = false
	runtime.reservationPriority = priority
	runtime.requested = requestedChannel
	runtime.tuned = tunedChannel
	managed := &managedDevice{Device: base, manager: tm, tuner: item}
	runtime.device = managed
	return managed, item.DecoderCommand(), true
}

func (tm *TunerManager) KillProcess(ctx context.Context, index int) error {
	tm.mu.Lock()
	if index < 0 || index >= len(tm.tuners) {
		tm.mu.Unlock()
		return ErrTunerNotFound
	}
	item := tm.tuners[index]
	device := tm.runtime[item].device
	tm.mu.Unlock()

	if device == nil {
		return nil
	}
	return device.Stop(ctx)
}

func (tm *TunerManager) release(item *Tuner) {
	var update tunerStatusUpdate
	wasFaulted := false
	tm.mu.Lock()
	if tm.inUse[item] {
		delete(tm.inUse, item)
		runtime := tm.runtime[item]
		wasFaulted = runtime.resetReservation()
		tm.notifyChangedLocked()
		update = tm.statusUpdateLocked(item)
		if wasFaulted {
			slog.Info("tuner fault cleared", "name", item.Name())
		}
		slog.Info("tuner released", "name", item.Name())
	}
	tm.mu.Unlock()
	tm.publishTunerStatusUpdate(eventTypeUpdate, update)
}

func (tm *TunerManager) DecoderCommandByType(channelType string) string {
	item := tm.GetTunerByType(channelType)
	if item == nil {
		return ""
	}
	return item.DecoderCommand()
}

type managedDevice struct {
	Device
	manager *TunerManager
	tuner   *Tuner
	once    sync.Once
}

func (d *managedDevice) Start(ctx context.Context, dst io.Writer) error {
	err := d.Device.Start(ctx, dst)
	if err != nil {
		slog.Warn("failed to start tuner", "name", d.tuner.Name(), "err", err)
		d.manager.markFault(d.tuner, d)
		d.releaseOnce()
		return err
	}
	slog.Info("tuner started", "name", d.tuner.Name())
	d.manager.markRunning(d.tuner, d)
	go func() {
		<-d.Done()
		if err := d.Err(); err != nil {
			slog.Warn("tuner stopped with error", "name", d.tuner.Name(), "err", err)
			d.manager.markFault(d.tuner, d)
		} else {
			slog.Debug("tuner stopped", "name", d.tuner.Name())
			d.manager.markStopped(d.tuner, d)
		}
	}()
	return nil
}

func (d *managedDevice) Stop(ctx context.Context) error {
	err := d.Device.Stop(ctx)
	if err != nil {
		slog.Warn("failed to stop tuner", "name", d.tuner.Name(), "err", err)
	} else {
		slog.Info("tuner stop requested", "name", d.tuner.Name())
	}
	d.releaseOnce()
	return err
}

func (d *managedDevice) ProcessStatus() ProcessInfo {
	process, ok := d.Device.(ProcessStatus)
	if !ok {
		return ProcessInfo{}
	}
	return process.ProcessStatus()
}

func (d *managedDevice) ProcessStartedAt() time.Time {
	process, ok := d.Device.(ProcessUptimeStatus)
	if !ok {
		return time.Time{}
	}
	return process.ProcessStartedAt()
}

func (d *managedDevice) AddUser(user User) { d.manager.addUser(d.tuner, user) }

func (d *managedDevice) RemoveUser(id string) { d.manager.removeUser(d.tuner, id) }

func (d *managedDevice) UpdateUserStreamInfo(userID, key string, info StreamInfo) {
	d.manager.updateUserStreamInfo(d.tuner, userID, key, info)
}

func (d *managedDevice) releaseOnce() { d.once.Do(func() { d.manager.release(d.tuner) }) }

func (tm *TunerManager) markRunning(item *Tuner, device *managedDevice) {
	tm.mu.Lock()
	update := tm.updateRuntimeStatusLocked(item, func(runtime *tunerRuntime) bool {
		if !runtime.inUse || runtime.device != device {
			return false
		}
		runtime.running = true
		runtime.stopped = false
		return true
	})
	tm.mu.Unlock()
	tm.publishTunerStatusUpdate(eventTypeUpdate, update)
}

func (tm *TunerManager) markStopped(item *Tuner, device *managedDevice) {
	tm.mu.Lock()
	update := tm.updateRuntimeStatusLocked(item, func(runtime *tunerRuntime) bool {
		if !runtime.inUse || runtime.device != device {
			return false
		}
		runtime.running = false
		runtime.stopped = true
		return true
	})
	tm.mu.Unlock()
	tm.publishTunerStatusUpdate(eventTypeUpdate, update)
}

func (tm *TunerManager) markFault(item *Tuner, device *managedDevice) {
	tm.mu.Lock()
	update := tm.updateRuntimeStatusLocked(item, func(runtime *tunerRuntime) bool {
		if !runtime.inUse || runtime.device != device {
			return false
		}
		runtime.running = false
		runtime.fault = true
		return true
	})
	tm.mu.Unlock()
	if update.publish {
		slog.Warn("tuner marked fault", "name", item.Name())
	}
	tm.publishTunerStatusUpdate(eventTypeUpdate, update)
}

func (tm *TunerManager) notifyChangedLocked() {
	close(tm.changed)
	tm.changed = make(chan struct{})
}

func (tm *TunerManager) updateRuntimeStatusLocked(item *Tuner, update func(*tunerRuntime) bool) tunerStatusUpdate {
	runtime := tm.runtime[item]
	if runtime == nil || !update(runtime) {
		return tunerStatusUpdate{}
	}
	return tm.statusUpdateLocked(item)
}

func (tm *TunerManager) statusUpdateLocked(item *Tuner) tunerStatusUpdate {
	return tunerStatusUpdate{status: tm.statusLockedByTuner(item), publish: true}
}

func (tm *TunerManager) publishTunerStatusUpdate(typ string, update tunerStatusUpdate) {
	if update.publish {
		tm.publishStatus(typ, update.status)
	}
}

func channelTypeOf(channel *config.ChannelConfig) string {
	if channel == nil {
		return ""
	}
	return channel.Type
}

func channelID(channel *config.ChannelConfig) string {
	if channel == nil {
		return ""
	}
	return channel.Channel
}

var (
	ErrTunerNotFound    = errors.New("tuner not found")
	ErrUnsupportedTuner = errors.New("unsupported tuner")
	ErrTunerUnavailable = errors.New("tuner unavailable")
)
