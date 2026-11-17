package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/21S1298001/mahiron/internal/tuner"
)

type fakeTunerUserDevice struct {
	fakeStopErrorDevice
	mu    sync.Mutex
	added []tuner.User
}

func (d *fakeTunerUserDevice) AddUser(user tuner.User) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.added = append(d.added, user)
}

func (d *fakeTunerUserDevice) RemoveUser(string) {}

func (d *fakeTunerUserDevice) lastUser() tuner.User {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.added[len(d.added)-1]
}

func TestTunerLiveSourceWithUserDefaultsFallbackUserToLowestPriority(t *testing.T) {
	done := make(chan struct{})
	close(done)
	device := &fakeTunerUserDevice{fakeStopErrorDevice: fakeStopErrorDevice{done: done}}
	src := &tunerLiveSource{
		channel: &config.ChannelConfig{Type: "GR", Channel: "27"},
		device:  device,
	}

	ctx := run.WithJob(context.Background(), run.JobInfo{Name: "EPG Gather NID 6"})
	if err := src.WithUser(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}

	user := device.lastUser()
	if user.Priority != -1 {
		t.Fatalf("fallback user priority = %d, want -1", user.Priority)
	}
	if user.Agent != "EPG Gather NID 6" {
		t.Fatalf("fallback user agent = %q, want %q", user.Agent, "EPG Gather NID 6")
	}
}

func TestTunerLiveSourceWithUserPassesThroughExplicitUser(t *testing.T) {
	done := make(chan struct{})
	close(done)
	device := &fakeTunerUserDevice{fakeStopErrorDevice: fakeStopErrorDevice{done: done}}
	src := &tunerLiveSource{
		channel: &config.ChannelConfig{Type: "GR", Channel: "27"},
		device:  device,
	}

	ctx := tuner.WithUser(context.Background(), tuner.User{ID: "explicit", Priority: 42})
	if err := src.WithUser(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}

	user := device.lastUser()
	if user.ID != "explicit" || user.Priority != 42 {
		t.Fatalf("user = %+v, want ID=explicit Priority=42", user)
	}
}

var errNoTunerFake = errors.New("no tuner (fake)")

type noDeviceTunerManager struct{}

func (noDeviceTunerManager) NewDeviceByType(string, *config.ChannelConfig) (tuner.Device, error) {
	return nil, errNoTunerFake
}

func TestFindChannelSkipsDisabledEntryToReachEnabledSibling(t *testing.T) {
	isDisabled := true
	channels := config.ChannelsConfig{
		{Type: "EXT1", Channel: "38", ServiceId: uint32Ptr(100), IsDisabled: &isDisabled},
		{Type: "EXT1", Channel: "38", ServiceId: uint32Ptr(119)},
	}
	pool := NewPool(channels, noDeviceTunerManager{}, nil, nil)

	_, err := pool.Acquire(context.Background(), "EXT1", "38", false)
	if !errors.Is(err, errNoTunerFake) {
		t.Fatalf("Acquire error = %v, want to reach tuner acquisition (errNoTunerFake)", err)
	}
}

func TestFindChannelReturnsNotFoundWhenAllMatchesDisabled(t *testing.T) {
	isDisabled := true
	channels := config.ChannelsConfig{
		{Type: "EXT1", Channel: "38", ServiceId: uint32Ptr(100), IsDisabled: &isDisabled},
	}
	pool := NewPool(channels, noDeviceTunerManager{}, nil, nil)

	_, err := pool.Acquire(context.Background(), "EXT1", "38", false)
	if !errors.Is(err, ErrChannelNotFound) {
		t.Fatalf("Acquire error = %v, want ErrChannelNotFound", err)
	}
}

func uint32Ptr(v uint32) *uint32 { return &v }

type fakeAllocatorDevice struct {
	done chan struct{}
}

func (d *fakeAllocatorDevice) Start(context.Context, io.Writer) error { return nil }

func (d *fakeAllocatorDevice) Stop(context.Context) error { return nil }

func (d *fakeAllocatorDevice) Done() <-chan struct{} { return d.done }

func (d *fakeAllocatorDevice) Err() error { return nil }

type fakeAllocatorManager struct {
	device   tuner.Device
	decoders tuner.DecoderCommands
}

func (m *fakeAllocatorManager) NewDeviceByType(string, *config.ChannelConfig) (tuner.Device, error) {
	return m.device, nil
}

func (m *fakeAllocatorManager) AcquireDevice(context.Context, string, *config.ChannelConfig, *config.ChannelConfig, bool) (tuner.Device, tuner.DecoderCommands, error) {
	return m.device, m.decoders, nil
}

type captureDescramblerFactory struct {
	mu       sync.Mutex
	commands []string
}

func (f *captureDescramblerFactory) New(command string) Descrambler {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commands = append(f.commands, command)
	return CommandDescrambler{}
}

func (f *captureDescramblerFactory) last() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commands[len(f.commands)-1]
}

func TestPoolSelectsB61DecoderForTLVChannel(t *testing.T) {
	done := make(chan struct{})
	close(done)
	channels := config.ChannelsConfig{
		{Type: "GR", Channel: "27", Transport: config.TransportTS},
		{Type: "BS4K", Channel: "101", Transport: config.TransportTLV},
	}
	manager := &fakeAllocatorManager{
		device:   &fakeAllocatorDevice{done: done},
		decoders: tuner.DecoderCommands{Decoder: "b25", B61Decoder: "b61"},
	}
	factory := &captureDescramblerFactory{}
	pool := NewPool(channels, manager, factory.New, nil)

	tsHandle, err := pool.Acquire(context.Background(), "GR", "27", false)
	if err != nil {
		t.Fatal(err)
	}
	if got := factory.last(); got != "b25" {
		t.Fatalf("TS descrambler = %q, want b25", got)
	}
	if tsHandle.Descrambler() == nil {
		t.Fatal("TS descrambler is nil")
	}

	tlvHandle, err := pool.Acquire(context.Background(), "BS4K", "101", false)
	if err != nil {
		t.Fatal(err)
	}
	if got := factory.last(); got != "b61" {
		t.Fatalf("TLV descrambler = %q, want b61", got)
	}
	if tlvHandle.Descrambler() == nil {
		t.Fatal("TLV descrambler is nil")
	}
}

func TestPoolTLVWithoutB61DecoderHasNoDescrambler(t *testing.T) {
	done := make(chan struct{})
	close(done)
	channels := config.ChannelsConfig{
		{Type: "BS4K", Channel: "101", Transport: config.TransportTLV},
	}
	manager := &fakeAllocatorManager{
		device:   &fakeAllocatorDevice{done: done},
		decoders: tuner.DecoderCommands{Decoder: "b25"},
	}
	factory := &captureDescramblerFactory{}
	pool := NewPool(channels, manager, factory.New, nil)

	handle, err := pool.Acquire(context.Background(), "BS4K", "101", false)
	if err != nil {
		t.Fatal(err)
	}
	if handle.Descrambler() != nil {
		t.Fatal("TLV descrambler without b61Decoder should be nil (decode=1 falls back to raw)")
	}
	if !pool.IsTLVChannel("BS4K", "101") {
		t.Fatal("IsTLVChannel(BS4K/101) = false, want true")
	}
	if pool.IsTLVChannel("GR", "27") {
		t.Fatal("IsTLVChannel(unknown) = true, want false")
	}
}

type fallbackDecoderManager struct{ *fakeAllocatorManager }

func (m fallbackDecoderManager) DecoderCommandByType(string) string    { return "local-b25" }
func (m fallbackDecoderManager) B61DecoderCommandByType(string) string { return "local-b61" }

func TestAgentDecodedRouteSkipsLocalFallbackOnReuse(t *testing.T) {
	manager := fallbackDecoderManager{&fakeAllocatorManager{device: &fakeAllocatorDevice{done: make(chan struct{})}, decoders: tuner.DecoderCommands{AgentDecoder: true, AgentB61Decoder: true}}}
	pool := NewPool(config.ChannelsConfig{{Type: "GR", Channel: "27"}, {Type: "BS4K", Channel: "101", Transport: config.TransportTLV}}, manager, func(string) Descrambler { t.Fatal("local decoder invoked"); return nil }, nil)
	for _, ch := range []struct{ typ, channel string }{{"GR", "27"}, {"BS4K", "101"}} {
		for range 2 {
			handle, err := pool.Acquire(context.Background(), ch.typ, ch.channel, false)
			if err != nil {
				t.Fatal(err)
			}
			if handle.Descrambler() != nil {
				t.Fatal("unexpected local decoder")
			}
		}
	}
}

func TestStoredEPGFollowsEveryEnabledRouteAndTransport(t *testing.T) {
	disabled := true
	remote := config.ChannelRouteConfig{Remote: "upstream", Type: "CUSTOM", Channel: "A"}
	local := config.ChannelRouteConfig{Type: "CUSTOM", Channel: "A"}
	disabledLocal := local
	disabledLocal.IsDisabled = &disabled
	for _, tc := range []struct {
		name    string
		channel config.ChannelConfig
		want    bool
	}{
		{"remote TS", config.ChannelConfig{Routes: []config.ChannelRouteConfig{remote}}, true},
		{"remote TLV", config.ChannelConfig{Transport: config.TransportTLV, Routes: []config.ChannelRouteConfig{remote}}, false},
		{"local", config.ChannelConfig{}, false},
		{"remote with local fallback", config.ChannelConfig{Routes: []config.ChannelRouteConfig{remote, local}}, false},
		{"local with remote fallback", config.ChannelConfig{Routes: []config.ChannelRouteConfig{local, remote}}, false},
		{"disabled local fallback", config.ChannelConfig{Routes: []config.ChannelRouteConfig{remote, disabledLocal}}, true},
		{"no enabled route", config.ChannelConfig{Routes: []config.ChannelRouteConfig{disabledLocal}}, false},
		{"disabled channel", config.ChannelConfig{Routes: []config.ChannelRouteConfig{remote}, IsDisabled: &disabled}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.channel.Type = "CUSTOM"
			tc.channel.Channel = "A"
			pool := NewPool(config.ChannelsConfig{tc.channel}, nil, nil, nil)
			if got := pool.StoredEPG("CUSTOM", "A"); got != tc.want {
				t.Fatalf("StoredEPG = %v, want %v", got, tc.want)
			}
			if pool.StoredEPG("CUSTOM", "missing") {
				t.Fatal("unknown channel has stored EPG")
			}
		})
	}
}

// Match the allocator's real waiting contract: wait=true never returns an
// unavailable error while the first route's tuner is offline or occupied.
type waitingRouteManager struct {
	calls      int
	readyAfter int
	device     tuner.Device
}

func (m *waitingRouteManager) NewDeviceByType(string, *config.ChannelConfig) (tuner.Device, error) {
	return nil, tuner.ErrTunerUnavailable
}
func (m *waitingRouteManager) AcquireDevice(ctx context.Context, _ string, _, _ *config.ChannelConfig, wait bool) (tuner.Device, tuner.DecoderCommands, error) {
	m.calls++
	if wait {
		<-ctx.Done()
		return nil, tuner.DecoderCommands{}, ctx.Err()
	}
	if m.readyAfter > 0 && m.calls >= m.readyAfter {
		return m.device, tuner.DecoderCommands{}, nil
	}
	return nil, tuner.DecoderCommands{}, tuner.ErrTunerUnavailable
}

type availableRouteRemote struct{}

func (availableRouteRemote) CheckAvailableForRoute(context.Context, string, string) error { return nil }
func (availableRouteRemote) ChannelStream(context.Context, string, string, bool, io.Writer) error {
	return nil
}

func TestWaitingAcquireTriesRemoteAfterUnavailableLocalRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	manager := &waitingRouteManager{}
	channel := config.ChannelConfig{Type: "GR", Channel: "27", Routes: []config.ChannelRouteConfig{{Type: "GR", Channel: "27"}, {Remote: "upstream", Type: "GR", Channel: "27"}}}
	pool := NewPool(config.ChannelsConfig{channel}, manager, nil, map[string]RemoteClient{"upstream": availableRouteRemote{}})
	handle, err := pool.Acquire(ctx, "GR", "27", true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = handle.Release(t.Context()) }()
	if handle.Metadata().Remote != "upstream" || manager.calls != 1 {
		t.Fatalf("metadata=%+v local attempts=%d", handle.Metadata(), manager.calls)
	}
}

func TestWaitingAcquireRetriesLocalRoutesUntilAvailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	manager := &waitingRouteManager{readyAfter: 3, device: &fakeAllocatorDevice{done: make(chan struct{})}}
	pool := NewPool(config.ChannelsConfig{{Type: "GR", Channel: "27"}}, manager, nil, nil)
	handle, err := pool.Acquire(ctx, "GR", "27", true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = handle.Release(t.Context()) }()
	if manager.calls != 3 {
		t.Fatalf("attempts=%d", manager.calls)
	}
}

func TestUnavailableRouteWaitHonorsContextAndNonWaitingAcquire(t *testing.T) {
	for _, wait := range []bool{false, true} {
		t.Run(fmt.Sprintf("wait=%t", wait), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
			defer cancel()
			manager := &waitingRouteManager{}
			pool := NewPool(config.ChannelsConfig{{Type: "GR", Channel: "27"}}, manager, nil, nil)
			_, err := pool.Acquire(ctx, "GR", "27", wait)
			if wait {
				if !errors.Is(err, context.DeadlineExceeded) || manager.calls < 2 {
					t.Fatalf("err=%v attempts=%d", err, manager.calls)
				}
			} else if !errors.Is(err, tuner.ErrTunerUnavailable) || manager.calls != 1 {
				t.Fatalf("err=%v attempts=%d", err, manager.calls)
			}
		})
	}
}
