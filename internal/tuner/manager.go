package tuner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"

	"github.com/21S1298001/Mahiron5/internal/config"
)

type TunerManager struct {
	tuners     []*Tuner
	mu         sync.Mutex
	inUse      map[*Tuner]bool
	runtime    map[*Tuner]*tunerRuntime
	nextByType map[string]int
	changed    chan struct{}
}

type TunerManagerConfig struct{ TunersConfig config.TunersConfig }

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

func (tm *TunerManager) AcquireDevice(ctx context.Context, channelType string, requestedChannel, tunedChannel *config.ChannelConfig, wait bool) (Device, string, error) {
	for {
		tm.mu.Lock()
		found := false
		usable := false
		start := tm.nextByType[channelType]
		for offset := range len(tm.tuners) {
			index := (start + offset) % len(tm.tuners)
			item := tm.tuners[index]
			if item.IsDisabled() || !slices.Contains(item.Groups(), channelType) {
				continue
			}
			found = true
			if !item.Usable() {
				continue
			}
			usable = true
			runtime := tm.runtime[item]
			if runtime.fault || tm.inUse[item] {
				continue
			}
			base := item.NewDevice(tunedChannel)
			if base == nil {
				continue
			}
			tm.inUse[item] = true
			runtime.inUse = true
			runtime.running = false
			runtime.stopped = false
			runtime.requested = requestedChannel
			runtime.tuned = tunedChannel
			managed := &managedDevice{Device: base, manager: tm, tuner: item}
			tm.nextByType[channelType] = (index + 1) % len(tm.tuners)
			runtime.device = managed
			decoder := item.DecoderCommand()
			slog.Info("tuner acquired",
				"name", item.Name(),
				"type", channelType,
				"channel", channelID(requestedChannel),
				"tunedType", channelTypeOf(tunedChannel),
				"tunedChannel", channelID(tunedChannel),
				"decoder", decoder != "",
			)
			tm.mu.Unlock()
			return managed, decoder, nil
		}
		changed := tm.changed
		tm.mu.Unlock()

		if !found {
			slog.Warn("tuner not found", "type", channelType, "channel", channelID(requestedChannel))
			return nil, "", ErrTunerNotFound
		}
		if !usable {
			slog.Warn("tuner unsupported", "type", channelType, "channel", channelID(requestedChannel))
			return nil, "", ErrUnsupportedTuner
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
		case <-changed:
		}
	}
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
	tm.mu.Lock()
	if tm.inUse[item] {
		delete(tm.inUse, item)
		runtime := tm.runtime[item]
		runtime.inUse = false
		runtime.running = false
		runtime.stopped = false
		runtime.device = nil
		runtime.requested = nil
		runtime.tuned = nil
		runtime.users = make(map[string]*trackedUser)
		close(tm.changed)
		tm.changed = make(chan struct{})
		slog.Info("tuner released", "name", item.Name())
	}
	tm.mu.Unlock()
}

func (tm *TunerManager) DecoderCommandByType(channelType string) string {
	item := tm.GetTunerByType(channelType)
	if item == nil {
		return ""
	}
	return item.DecoderCommand()
}

func (tm *TunerManager) TunerCount() int { return len(tm.tuners) }

func (tm *TunerManager) TunerCountByType(channelType string) int {
	count := 0
	for _, item := range tm.tuners {
		if !item.IsDisabled() && slices.Contains(item.Groups(), channelType) {
			count++
		}
	}
	return count
}

func (tm *TunerManager) CountTunersByType() map[string]int {
	counts := make(map[string]int)
	for _, item := range tm.tuners {
		if item.IsDisabled() {
			continue
		}
		for _, group := range item.Groups() {
			counts[group]++
		}
	}
	return counts
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
		d.manager.markFault(d.tuner)
		d.releaseOnce()
		return err
	}
	slog.Info("tuner started", "name", d.tuner.Name())
	d.manager.markRunning(d.tuner)
	go func() {
		<-d.Device.Done()
		if err := d.Device.Err(); err != nil {
			slog.Warn("tuner stopped with error", "name", d.tuner.Name(), "err", err)
			d.manager.markFault(d.tuner)
		} else {
			slog.Debug("tuner stopped", "name", d.tuner.Name())
			d.manager.markStopped(d.tuner)
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

func (d *managedDevice) AddUser(user User) { d.manager.addUser(d.tuner, user) }

func (d *managedDevice) RemoveUser(id string) { d.manager.removeUser(d.tuner, id) }

func (d *managedDevice) releaseOnce() { d.once.Do(func() { d.manager.release(d.tuner) }) }

func (tm *TunerManager) markRunning(item *Tuner) {
	tm.mu.Lock()
	tm.runtime[item].running = true
	tm.runtime[item].stopped = false
	tm.mu.Unlock()
}

func (tm *TunerManager) markStopped(item *Tuner) {
	tm.mu.Lock()
	runtime := tm.runtime[item]
	if runtime.inUse {
		runtime.running = false
		runtime.stopped = true
	}
	tm.mu.Unlock()
}

func (tm *TunerManager) markFault(item *Tuner) {
	tm.mu.Lock()
	runtime := tm.runtime[item]
	marked := false
	if runtime.inUse {
		runtime.running = false
		runtime.fault = true
		marked = true
	}
	tm.mu.Unlock()
	if marked {
		slog.Warn("tuner marked fault", "name", item.Name())
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
