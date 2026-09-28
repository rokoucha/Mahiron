package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/stream/source"
	"github.com/21S1298001/mahiron/internal/stream/tlv"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/mmt"
)

func tlvTestPacket(payload byte) []byte {
	return []byte{mmt.TLVSyncByte, 0xFF, 0x00, 0x01, payload}
}

type tlvEmitterRecorder struct {
	mu      sync.Mutex
	devices []*tlvEmitterDevice
	hang    bool
}

func (r *tlvEmitterRecorder) NewDevice(*config.ChannelConfig) source.TunerDevice {
	r.mu.Lock()
	defer r.mu.Unlock()
	device := &tlvEmitterDevice{done: make(chan struct{}), hang: r.hang}
	r.devices = append(r.devices, device)
	return device
}

func (r *tlvEmitterRecorder) starts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, device := range r.devices {
		device.mu.Lock()
		count += device.starts
		device.mu.Unlock()
	}
	return count
}

type tlvEmitterDevice struct {
	done   chan struct{}
	mu     sync.Mutex
	starts int
	stop   chan struct{}
	hang   bool
	once   sync.Once
}

func (d *tlvEmitterDevice) Start(_ context.Context, dst io.Writer) error {
	d.mu.Lock()
	d.starts++
	d.mu.Unlock()
	if d.hang {
		d.stop = make(chan struct{})
		go func() {
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-d.stop:
					return
				case <-ticker.C:
					if _, err := dst.Write(tlvTestPacket(0xAA)); err != nil {
						return
					}
				}
			}
		}()
		return nil
	}
	go func() {
		_, err := dst.Write(append(tlvTestPacket(0x01), tlvTestPacket(0x02)...))
		_ = err
		close(d.done)
	}()
	return nil
}

func (d *tlvEmitterDevice) Stop(context.Context) error {
	d.once.Do(func() {
		if d.stop != nil {
			close(d.stop)
		}
		select {
		case <-d.done:
		default:
			close(d.done)
		}
	})
	return nil
}

func (d *tlvEmitterDevice) Done() <-chan struct{} { return d.done }

func (d *tlvEmitterDevice) Err() error { return nil }

type tlvTestTunerManager struct {
	devices *tlvEmitterRecorder
	b61     string
}

func (m *tlvTestTunerManager) NewDeviceByType(string, *config.ChannelConfig) (tuner.Device, error) {
	return m.devices.NewDevice(nil), nil
}

func (m *tlvTestTunerManager) B61DecoderCommandByType(string) string { return m.b61 }

func tlvTestManager(t *testing.T, devices *tlvEmitterRecorder, descramblers *fakeDescramblerRecorder, b61 string) *Manager {
	t.Helper()
	no := false
	factory := source.DescramblerFactory(nil)
	if descramblers != nil {
		factory = descramblers.NewDescrambler
	}
	manager := NewManager(ManagerConfig{
		Channels: config.ChannelsConfig{
			{
				Name:       "BSP4K",
				Type:       "BS4K",
				Channel:    "101",
				Transport:  config.TransportTLV,
				IsDisabled: &no,
			},
		},
		DescramblerFactory: factory,
		TunerManager:       &tlvTestTunerManager{devices: devices, b61: b61},
	})
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	return manager
}

func TestTLVChannelCreatesTLVSession(t *testing.T) {
	devices := &tlvEmitterRecorder{}
	manager := tlvTestManager(t, devices, nil, "")

	session, err := manager.GetOrCreate(context.Background(), "BS4K", "101")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := session.(*tlv.Session); !ok {
		t.Fatalf("session type = %T, want *tlv.Session", session)
	}
	if err := session.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTLVChannelStreamPassesTLVThrough(t *testing.T) {
	devices := &tlvEmitterRecorder{}
	manager := tlvTestManager(t, devices, nil, "")

	session, err := manager.GetOrCreate(context.Background(), "BS4K", "101")
	if err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := session.ChannelStream(context.Background(), false, &out); err != nil {
		t.Fatal(err)
	}
	want := append(tlvTestPacket(0x01), tlvTestPacket(0x02)...)
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("tlv stream = %x, want %x", out.Bytes(), want)
	}
	if got := devices.starts(); got != 1 {
		t.Fatalf("tuner device starts = %d, want 1", got)
	}
}

func TestTLVChannelStreamRejectsTSSource(t *testing.T) {
	devices := &fakeTunerDeviceRecorder{}
	no := false
	manager := NewManager(ManagerConfig{
		Channels: config.ChannelsConfig{
			{Name: "BSP4K", Type: "BS4K", Channel: "101", Transport: config.TransportTLV, IsDisabled: &no},
		},
		TunerManager: fakeTunerManager{devices: devices},
	})
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })

	session, err := manager.GetOrCreate(context.Background(), "BS4K", "101")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := session.(*tlv.Session); !ok {
		t.Fatalf("session type = %T, want *tlv.Session", session)
	}
	if err := session.ChannelStream(context.Background(), false, io.Discard); !errors.Is(err, tlv.ErrNotTLVStream) {
		t.Fatalf("TS-backed TLV stream error = %v, want ErrNotTLVStream", err)
	}
}

func TestTLVDecodedStreamSharesOneDescrambler(t *testing.T) {
	devices := &tlvEmitterRecorder{hang: true}
	descramblers := &fakeDescramblerRecorder{}
	manager := tlvTestManager(t, devices, descramblers, "b61-test")

	session, err := manager.GetOrCreate(context.Background(), "BS4K", "101")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var first, second lockedBuffer
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = session.ChannelStream(ctx, true, &first)
	}()
	go func() {
		defer wg.Done()
		_ = session.ChannelStream(ctx, true, &second)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for (first.Len() == 0 || second.Len() == 0) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	if first.Len() == 0 || second.Len() == 0 {
		t.Fatalf("both decoded subscribers should receive data: first=%d second=%d bytes", first.Len(), second.Len())
	}
	if got := devices.starts(); got != 1 {
		t.Fatalf("tuner device starts = %d, want 1", got)
	}
	if got := descramblers.starts(); got != 1 {
		t.Fatalf("descrambler starts = %d, want 1 shared", got)
	}
}

func TestTLVServiceStreamPassesThrough(t *testing.T) {
	devices := &tlvEmitterRecorder{}
	manager := tlvTestManager(t, devices, nil, "")

	session, err := manager.GetOrCreate(context.Background(), "BS4K", "101")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := session.ServiceStream(context.Background(), 101, false, &out); err != nil {
		t.Fatal(err)
	}
	want := append(tlvTestPacket(0x01), tlvTestPacket(0x02)...)
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("tlv service stream = %x, want %x", out.Bytes(), want)
	}
}

// lockedBuffer is a goroutine-safe bytes.Buffer for polling stream output in
// tests.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func (b *lockedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}
