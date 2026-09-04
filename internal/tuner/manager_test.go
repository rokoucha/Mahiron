package tuner

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
)

func TestTunerManagerReservesIndividualTuners(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "first", Types: []string{"GR"}, Command: "first", Decoder: "decode-first"},
		{Name: "second", Types: []string{"GR"}, Command: "second", Decoder: "decode-second"},
	}})
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	first, firstDecoder, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	second, secondDecoder, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	if firstDecoder != "decode-first" || secondDecoder != "decode-second" {
		t.Fatalf("decoders = %q, %q", firstDecoder, secondDecoder)
	}
	if _, _, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false); !errors.Is(err, ErrTunerUnavailable) {
		t.Fatalf("third acquire error = %v", err)
	}
	if err := first.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	reused, decoder, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
	if err != nil || decoder != "decode-first" {
		t.Fatalf("reused decoder = %q, err = %v", decoder, err)
	}
	_ = reused.Stop(context.Background())
	_ = second.Stop(context.Background())
}

func TestTunerManagerWaitCanBeCancelled(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "only", Types: []string{"GR"}, Command: "only"},
	}})
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	device, _, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := mgr.AcquireDevice(ctx, "GR", channel, channel, true); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting acquire error = %v", err)
	}
	_ = device.Stop(context.Background())
}

func TestStaleDeviceCompletionDoesNotFaultNewReservation(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "only", Types: []string{"GR"}, Command: "true"},
	}})
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	first, _, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, _, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}

	mgr.markFault(mgr.tuners[0], first.(*managedDevice))
	status, _ := mgr.Status(0)
	if status.IsFault || !status.IsAvailable {
		t.Fatalf("stale completion changed new reservation status: %+v", status)
	}
	_ = second.Stop(context.Background())
}

func TestFaultClearsOnReleaseAndTunerCanBeReacquired(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "only", Types: []string{"GR"}, Command: "true"},
	}})
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	device, _, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	managed := device.(*managedDevice)
	mgr.markFault(mgr.tuners[0], managed)
	status, _ := mgr.Status(0)
	if !status.IsFault || status.IsAvailable {
		t.Fatalf("current device fault was not reported: %+v", status)
	}
	if err := device.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, _ = mgr.Status(0)
	if status.IsFault || !status.IsFree {
		t.Fatalf("fault was not cleared on release: %+v", status)
	}
	reused, _, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
	if err != nil {
		t.Fatalf("reacquire after fault: %v", err)
	}
	_ = reused.Stop(context.Background())
}

func TestTunerManagerSelectsTunersRoundRobin(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "first", Types: []string{"GR"}, Command: "first", Decoder: "decode-first"},
		{Name: "second", Types: []string{"GR"}, Command: "second", Decoder: "decode-second"},
	}})
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	want := []string{"decode-first", "decode-second", "decode-first", "decode-second"}
	for i, expected := range want {
		device, decoder, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		if decoder != expected {
			t.Fatalf("acquire %d decoder = %q, want %q", i, decoder, expected)
		}
		if err := device.Stop(context.Background()); err != nil {
			t.Fatalf("stop %d: %v", i, err)
		}
	}
}

func TestTunerManagerHighPriorityGrabsLowPriorityTuner(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "only", Types: []string{"GR"}, Command: "true", Decoder: "decode-only"},
	}})
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	lowCtx := WithUser(context.Background(), User{ID: "low", Priority: 1})
	low, _, err := mgr.AcquireDevice(lowCtx, "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	low.(interface{ AddUser(User) }).AddUser(User{ID: "low", Priority: 1})

	highCtx := WithUser(context.Background(), User{ID: "high", Priority: 2})
	high, decoder, err := mgr.AcquireDevice(highCtx, "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	if decoder != "decode-only" {
		t.Fatalf("decoder = %q, want decode-only", decoder)
	}
	if low == high {
		t.Fatal("grabbed acquire should return a new managed device")
	}
	_ = high.Stop(context.Background())
}

func TestTunerManagerEqualPriorityCannotGrab(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "only", Types: []string{"GR"}, Command: "true"},
	}})
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	firstCtx := WithUser(context.Background(), User{ID: "first", Priority: 1})
	first, _, err := mgr.AcquireDevice(firstCtx, "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	first.(interface{ AddUser(User) }).AddUser(User{ID: "first", Priority: 1})

	secondCtx := WithUser(context.Background(), User{ID: "second", Priority: 1})
	if _, _, err := mgr.AcquireDevice(secondCtx, "GR", channel, channel, false); !errors.Is(err, ErrTunerUnavailable) {
		t.Fatalf("equal priority acquire error = %v, want ErrTunerUnavailable", err)
	}
	_ = first.Stop(context.Background())
}

func TestTunerManagerLowerPriorityCannotGrab(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "only", Types: []string{"GR"}, Command: "true"},
	}})
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	highCtx := WithUser(context.Background(), User{ID: "high", Priority: 2})
	high, _, err := mgr.AcquireDevice(highCtx, "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	high.(interface{ AddUser(User) }).AddUser(User{ID: "high", Priority: 2})

	lowCtx := WithUser(context.Background(), User{ID: "low", Priority: 1})
	if _, _, err := mgr.AcquireDevice(lowCtx, "GR", channel, channel, false); !errors.Is(err, ErrTunerUnavailable) {
		t.Fatalf("lower priority acquire error = %v, want ErrTunerUnavailable", err)
	}
	_ = high.Stop(context.Background())
}

func TestTunerManagerDefaultPriorityGrabsNegativeReservation(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "only", Types: []string{"GR"}, Command: "true"},
	}})
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	epgCtx := WithUser(context.Background(), User{ID: "epg", Priority: -1})
	epg, _, err := mgr.AcquireDevice(epgCtx, "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}

	viewer, _, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	if epg == viewer {
		t.Fatal("default-priority grab should return a new managed device")
	}
	_ = viewer.Stop(context.Background())
}

func TestTunerManagerDefaultPriorityGrabsTunerHeldByNegativePriorityUser(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "only", Types: []string{"GR"}, Command: "true"},
	}})
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	epgCtx := WithUser(context.Background(), User{ID: "epg", Priority: -1})
	epg, _, err := mgr.AcquireDevice(epgCtx, "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	epg.(interface{ AddUser(User) }).AddUser(User{ID: "epg", Priority: -1})

	viewer, _, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	if epg == viewer {
		t.Fatal("default-priority request should grab a tuner held only by priority -1 users")
	}
	_ = viewer.Stop(context.Background())
}

func TestTunerManagerHighestActiveUserPriorityProtectsTuner(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "only", Types: []string{"GR"}, Command: "true"},
	}})
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	device, _, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	tracked := device.(interface{ AddUser(User) })
	tracked.AddUser(User{ID: "low", Priority: -1})
	tracked.AddUser(User{ID: "high", Priority: 10})

	requestCtx := WithUser(context.Background(), User{ID: "request", Priority: 5})
	if _, _, err := mgr.AcquireDevice(requestCtx, "GR", channel, channel, false); !errors.Is(err, ErrTunerUnavailable) {
		t.Fatalf("protected acquire error = %v, want ErrTunerUnavailable", err)
	}
	_ = device.Stop(context.Background())
}

func TestTunerManagerGrabsLowestPriorityCandidate(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "first", Types: []string{"GR"}, Command: "true", Decoder: "decode-first"},
		{Name: "second", Types: []string{"GR"}, Command: "true", Decoder: "decode-second"},
	}})
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	first, _, err := mgr.AcquireDevice(WithUser(context.Background(), User{ID: "first", Priority: 4}), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	first.(interface{ AddUser(User) }).AddUser(User{ID: "first", Priority: 4})
	second, _, err := mgr.AcquireDevice(WithUser(context.Background(), User{ID: "second", Priority: 1}), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	second.(interface{ AddUser(User) }).AddUser(User{ID: "second", Priority: 1})

	grabber, decoder, err := mgr.AcquireDevice(WithUser(context.Background(), User{ID: "grabber", Priority: 5}), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	if decoder != "decode-second" {
		t.Fatalf("grabbed decoder = %q, want decode-second", decoder)
	}
	_ = grabber.Stop(context.Background())
	_ = first.Stop(context.Background())
}

func TestTunerManagerReservesDVBCommandTuner(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "dvb", Types: []string{"SKY"}, Command: "true", DvbDevicePath: "/dev/null", Decoder: "decode-dvb"},
	}})
	channel := &config.ChannelConfig{Type: "SKY", Channel: "JCSAT3A"}
	device, decoder, err := mgr.AcquireDevice(context.Background(), "SKY", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	if decoder != "decode-dvb" {
		t.Fatalf("decoder = %q, want decode-dvb", decoder)
	}
	if err := device.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTunerManagerCheckAvailableDoesNotReserve(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "first", Types: []string{"GR"}, Command: "sleep 10"},
	}})
	if err := mgr.CheckAvailable(context.Background(), "GR"); err != nil {
		t.Fatal(err)
	}
	status, ok := mgr.Status(0)
	if !ok {
		t.Fatal("tuner status not found")
	}
	if !status.IsFree || status.IsUsing || status.PID != 0 {
		t.Fatalf("availability check reserved tuner: %+v", status)
	}
}

func TestTunerManagerKillProcess(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "first", Types: []string{"GR"}, Command: "sleep 10"},
	}})
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	device, _, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	tracked := device.(interface{ AddUser(User) })
	tracked.AddUser(User{ID: "viewer"})
	if err := device.Start(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}

	if err := mgr.KillProcess(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	status, _ := mgr.Status(0)
	if !status.IsFree || status.PID != 0 || len(status.Users) != 0 {
		t.Fatalf("unexpected status after kill: %+v", status)
	}
}

func TestTunerManagerKillProcessIdleAndMissing(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "first", Types: []string{"GR"}, Command: "sleep 1"},
	}})
	if err := mgr.KillProcess(context.Background(), 0); err != nil {
		t.Fatalf("idle kill error = %v", err)
	}
	if err := mgr.KillProcess(context.Background(), 1); !errors.Is(err, ErrTunerNotFound) {
		t.Fatalf("missing kill error = %v", err)
	}
}
