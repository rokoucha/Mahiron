package tuner

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
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
