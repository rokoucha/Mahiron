package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/tuner"
)

func testManager(t *testing.T, devices *fakeTunerDeviceRecorder) *StreamManager {
	t.Helper()
	return testManagerWithDescrambler(t, devices, nil)
}

func testManagerWithDescrambler(t *testing.T, devices *fakeTunerDeviceRecorder, descramblers *fakeDescramblerRecorder) *StreamManager {
	t.Helper()
	no := false
	factory := DescramblerFactory(nil)
	if descramblers != nil {
		factory = descramblers.NewDescrambler
	}
	return NewStreamManager(StreamManagerConfig{
		Channels: config.ChannelsConfig{
			{
				Name:       "NHK",
				Type:       "GR",
				Channel:    "27",
				IsDisabled: &no,
			},
			{
				Name:       "BS",
				Type:       "BS",
				Channel:    "101",
				IsDisabled: &no,
			},
		},
		DescramblerFactory: factory,
		Filter:             fakeFilter{},
		Scanner:            fakeScanner{},
		TunerManager: fakeTunerManager{
			decoderCommand: "descrambler",
			devices:        devices,
		},
	})
}

func TestManagerSharesSessionsByTypeAndChannel(t *testing.T) {
	manager := testManager(t, &fakeTunerDeviceRecorder{})

	first, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}
	other, err := manager.GetOrCreate(context.Background(), "BS", "101")
	if err != nil {
		t.Fatal(err)
	}

	if first != second {
		t.Fatal("same type+channel should return the same session")
	}
	if first == other {
		t.Fatal("different type+channel should return a different session")
	}
}

func TestManagerSelectsRouteByFreeChannelType(t *testing.T) {
	no := false
	priorityDirect := 10
	priorityCATV := 20
	manager := NewStreamManager(StreamManagerConfig{
		Channels: config.ChannelsConfig{
			{
				Name:       "NHK BS",
				Type:       "BS",
				Channel:    "101",
				IsDisabled: &no,
				Routes: []config.ChannelRouteConfig{
					{Id: "bs-direct", Type: "BS", Channel: "101", IsDisabled: &no, Priority: &priorityDirect},
					{Id: "catv-bs", Type: "CATV_BS", Channel: "C101", IsDisabled: &no, Priority: &priorityCATV},
				},
			},
		},
		TunerManager: &routeSelectingTunerManager{
			availableType: "CATV_BS",
		},
	})

	session, err := manager.GetOrCreate(context.Background(), "BS", "101")
	if err != nil {
		t.Fatal(err)
	}

	routeManager := manager.sources.tunerManager.(*routeSelectingTunerManager)
	if got, want := routeManager.channelType, "CATV_BS"; got != want {
		t.Fatalf("device channel type = %q, want %q", got, want)
	}
	if got, want := routeManager.channelID, "C101"; got != want {
		t.Fatalf("device channel = %q, want %q", got, want)
	}
	if got, want := session.typ, "BS"; got != want {
		t.Fatalf("session type = %q, want public type %q", got, want)
	}
}

func TestRawStream(t *testing.T) {
	devices := &fakeTunerDeviceRecorder{}
	manager := testManager(t, devices)
	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := session.ChannelStream(context.Background(), false, &out); err != nil {
		t.Fatal(err)
	}

	if got, want := out.String(), "ts2"; got != want {
		t.Fatalf("raw stream = %q, want %q", got, want)
	}
	if got := devices.starts(); got != 1 {
		t.Fatalf("tuner device starts = %d, want 1", got)
	}
}

func TestConcurrentRawStreamsStartOneTunerDevice(t *testing.T) {
	devices := &fakeTunerDeviceRecorder{}
	manager := testManager(t, devices)
	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var first bytes.Buffer
	var second bytes.Buffer
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := session.ChannelStream(context.Background(), false, &first); err != nil {
			t.Errorf("first stream: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := session.ChannelStream(context.Background(), false, &second); err != nil {
			t.Errorf("second stream: %v", err)
		}
	}()
	wg.Wait()

	if got := devices.starts(); got != 1 {
		t.Fatalf("tuner device starts = %d, want 1", got)
	}
	if first.String() == "" || second.String() == "" {
		t.Fatalf("both subscribers should receive data: first=%q second=%q", first.String(), second.String())
	}
}

func TestServiceStreamAndScanShareRunningSession(t *testing.T) {
	devices := &fakeTunerDeviceRecorder{}
	manager := testManager(t, devices)
	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}

	var serviceOut bytes.Buffer
	var scanOut bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := session.ServiceStream(context.Background(), 1024, false, &serviceOut); err != nil {
			t.Errorf("service stream: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := session.ScanServices(context.Background(), &scanOut); err != nil {
			t.Errorf("scan services: %v", err)
		}
	}()
	wg.Wait()

	if got := devices.starts(); got != 1 {
		t.Fatalf("tuner device starts = %d, want 1", got)
	}
	if got, want := serviceOut.String(), "filtered:ts2"; got != want {
		t.Fatalf("service stream = %q, want %q", got, want)
	}
	if got, want := scanOut.String(), "ts2"; got != want {
		t.Fatalf("scan stream = %q, want %q", got, want)
	}
}

func TestScanServicesReturnsWhenScannerCompletesBeforeLiveTuner(t *testing.T) {
	device := &fakeLiveTunerDevice{}
	manager := NewStreamManager(StreamManagerConfig{
		Channels: config.ChannelsConfig{
			{
				Name:    "NHK",
				Type:    "GR",
				Channel: "27",
			},
		},
		Scanner: fakeShortScanner{},
		TunerManager: fakeLiveTunerManager{
			device: device,
		},
	})
	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	var out bytes.Buffer
	go func() {
		done <- session.ScanServices(context.Background(), &out)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ScanServices did not return after scanner completed")
	}

	if got, want := out.String(), "scanned:ts"; got != want {
		t.Fatalf("scan output = %q, want %q", got, want)
	}
	if got := device.startCount(); got != 1 {
		t.Fatalf("tuner device starts = %d, want 1", got)
	}
}

func TestDecodePipelinesShareOneTunerDevice(t *testing.T) {
	devices := &fakeTunerDeviceRecorder{}
	descramblers := &fakeDescramblerRecorder{}
	manager := testManagerWithDescrambler(t, devices, descramblers)
	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}

	var rawOut bytes.Buffer
	var decodedOut bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := session.ChannelStream(context.Background(), false, &rawOut); err != nil {
			t.Errorf("raw stream: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := session.ChannelStream(context.Background(), true, &decodedOut); err != nil {
			t.Errorf("decoded stream: %v", err)
		}
	}()
	wg.Wait()

	if got := devices.starts(); got != 1 {
		t.Fatalf("tuner device starts = %d, want 1", got)
	}
	if got, want := rawOut.String(), "ts2"; got != want {
		t.Fatalf("raw stream = %q, want %q", got, want)
	}
	if got, want := decodedOut.String(), "decoded:ts2"; got != want {
		t.Fatalf("decoded stream = %q, want %q", got, want)
	}
	if got := descramblers.starts(); got != 1 {
		t.Fatalf("descrambler starts = %d, want 1", got)
	}
}

func TestServiceDecodePipelineOrder(t *testing.T) {
	devices := &fakeTunerDeviceRecorder{}
	descramblers := &fakeDescramblerRecorder{}
	manager := testManagerWithDescrambler(t, devices, descramblers)
	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := session.ServiceStream(context.Background(), 1024, true, &out); err != nil {
		t.Fatal(err)
	}

	if got, want := out.String(), "filtered:decoded:ts2"; got != want {
		t.Fatalf("service stream = %q, want %q", got, want)
	}
	if got := descramblers.starts(); got != 1 {
		t.Fatalf("descrambler starts = %d, want 1", got)
	}
}

func TestSharedServicePipelineStartsOneDescrambler(t *testing.T) {
	devices := &fakeTunerDeviceRecorder{}
	descramblers := &fakeDescramblerRecorder{}
	manager := testManagerWithDescrambler(t, devices, descramblers)
	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}

	var first bytes.Buffer
	var second bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := session.ServiceStream(context.Background(), 1024, true, &first); err != nil {
			t.Errorf("first service stream: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := session.ServiceStream(context.Background(), 1024, true, &second); err != nil {
			t.Errorf("second service stream: %v", err)
		}
	}()
	wg.Wait()

	if got := devices.starts(); got != 1 {
		t.Fatalf("tuner device starts = %d, want 1", got)
	}
	if got := descramblers.starts(); got != 1 {
		t.Fatalf("descrambler starts = %d, want 1", got)
	}
	if first.String() == "" || second.String() == "" {
		t.Fatalf("both subscribers should receive data: first=%q second=%q", first.String(), second.String())
	}
}

func TestPipelineConvertsProcessorPanicToError(t *testing.T) {
	pipeline := newStreamPipeline(
		PipelineKey{ChannelType: "GR", ChannelID: "27", Kind: PipelineChannelStream, Decode: true},
		[]Processor{panicProcessor{}},
		func(ctx context.Context, w io.Writer) error {
			_, err := w.Write([]byte("ts"))
			if errors.Is(err, io.ErrClosedPipe) {
				return nil
			}
			return err
		},
		func() {},
	)

	err := pipeline.Attach(context.Background(), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "stream processor panic") {
		t.Fatalf("pipeline error = %v, want processor panic", err)
	}
}

func TestDetachRawDoesNotLogExpectedClosedFileStopError(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	done := make(chan struct{})
	close(done)
	session := NewChannelSession(ChannelSessionConfig{
		Channel: "27",
		Type:    "GR",
		Broadcast: NewBroadcast(&tunerLiveSource{
			channel: &config.ChannelConfig{Type: "GR", Channel: "27"},
			device: fakeStopErrorDevice{
				done:    done,
				stopErr: &os.PathError{Op: "read", Path: "|0", Err: os.ErrClosed},
			},
		}, nil, nil),
	})

	var dst bytes.Buffer
	if err := session.broadcast.attach(&dst); err != nil {
		t.Fatal(err)
	}
	session.broadcast.detach(&dst)

	if strings.Contains(logs.String(), "failed to stop channel session") {
		t.Fatalf("unexpected stop error log: %s", logs.String())
	}
}

type fakeTunerDeviceRecorder struct {
	mu      sync.Mutex
	devices []*fakeTunerDevice
}

func (r *fakeTunerDeviceRecorder) NewDevice(*config.ChannelConfig) TunerDevice {
	r.mu.Lock()
	defer r.mu.Unlock()
	device := &fakeTunerDevice{
		done: make(chan struct{}),
	}
	r.devices = append(r.devices, device)
	return device
}

func (r *fakeTunerDeviceRecorder) starts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, device := range r.devices {
		count += device.startCount()
	}
	return count
}

type fakeTunerDevice struct {
	done   chan struct{}
	err    error
	mu     sync.Mutex
	starts int
}

type routeSelectingTunerManager struct {
	availableType string
	channelType   string
	channelID     string
}

func (m *routeSelectingTunerManager) NewDeviceByType(channelType string, channel *config.ChannelConfig) (tuner.Device, error) {
	if channelType != m.availableType {
		return nil, tuner.ErrTunerNotFound
	}
	m.channelType = channel.Type
	m.channelID = channel.Channel
	return &fakeTunerDevice{done: make(chan struct{})}, nil
}

type fakeTunerManager struct {
	decoderCommand string
	devices        *fakeTunerDeviceRecorder
}

func (m fakeTunerManager) NewDeviceByType(_ string, channel *config.ChannelConfig) (tuner.Device, error) {
	return m.devices.NewDevice(channel), nil
}

func (m fakeTunerManager) DecoderCommandByType(string) string {
	return m.decoderCommand
}

func (d *fakeTunerDevice) Start(_ context.Context, dst io.Writer) error {
	d.mu.Lock()
	d.starts++
	d.mu.Unlock()
	go func() {
		time.Sleep(10 * time.Millisecond)
		_, err := dst.Write([]byte("ts"))
		if err == nil {
			time.Sleep(20 * time.Millisecond)
			_, err = dst.Write([]byte("2"))
		}
		d.mu.Lock()
		d.err = err
		d.mu.Unlock()
		close(d.done)
	}()
	return nil
}

func (d *fakeTunerDevice) Stop(context.Context) error {
	return nil
}

func (d *fakeTunerDevice) Done() <-chan struct{} {
	return d.done
}

func (d *fakeTunerDevice) Err() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.err
}

func (d *fakeTunerDevice) startCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.starts
}

type fakeFilter struct{}

func (fakeFilter) FilterService(_ context.Context, _ uint16, src io.Reader, dst io.Writer) error {
	data, err := io.ReadAll(src)
	if err != nil {
		return err
	}
	_, err = dst.Write([]byte("filtered:" + string(data)))
	return err
}

type fakeScanner struct{}

func (fakeScanner) ScanServices(_ context.Context, src io.Reader, dst io.Writer) error {
	_, err := io.Copy(dst, src)
	return err
}

type fakeShortScanner struct{}

func (fakeShortScanner) ScanServices(_ context.Context, src io.Reader, dst io.Writer) error {
	buf := make([]byte, 2)
	if _, err := io.ReadFull(src, buf); err != nil {
		return err
	}
	_, err := dst.Write([]byte("scanned:" + string(buf)))
	return err
}

type fakeLiveTunerManager struct {
	device *fakeLiveTunerDevice
}

func (m fakeLiveTunerManager) NewDeviceByType(string, *config.ChannelConfig) (tuner.Device, error) {
	return m.device, nil
}

type fakeLiveTunerDevice struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
	mu     sync.Mutex
	starts int
}

func (d *fakeLiveTunerDevice) Start(ctx context.Context, dst io.Writer) error {
	d.mu.Lock()
	d.starts++
	deviceCtx, cancel := context.WithCancel(ctx)
	d.cancel = cancel
	d.done = make(chan struct{})
	d.mu.Unlock()

	go func() {
		defer close(d.done)
		for {
			select {
			case <-deviceCtx.Done():
				return
			default:
			}
			if _, err := dst.Write([]byte("ts")); err != nil && !errors.Is(err, io.ErrClosedPipe) {
				d.mu.Lock()
				d.err = err
				d.mu.Unlock()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	return nil
}

func (d *fakeLiveTunerDevice) Stop(context.Context) error {
	d.mu.Lock()
	cancel := d.cancel
	d.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func (d *fakeLiveTunerDevice) Done() <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.done
}

func (d *fakeLiveTunerDevice) Err() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.err
}

func (d *fakeLiveTunerDevice) startCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.starts
}

type panicProcessor struct{}

func (panicProcessor) Run(context.Context, io.Reader, io.Writer) error {
	panic("boom")
}

type fakeStopErrorDevice struct {
	done    <-chan struct{}
	stopErr error
}

func (d fakeStopErrorDevice) Start(context.Context, io.Writer) error {
	return nil
}

func (d fakeStopErrorDevice) Stop(context.Context) error {
	return d.stopErr
}

func (d fakeStopErrorDevice) Done() <-chan struct{} {
	return d.done
}

func (d fakeStopErrorDevice) Err() error {
	return nil
}

type fakeDescramblerRecorder struct {
	mu         sync.Mutex
	startCount int
}

func (r *fakeDescramblerRecorder) NewDescrambler(string) Descrambler {
	return fakeDescrambler{recorder: r}
}

func (r *fakeDescramblerRecorder) starts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.startCount
}

type fakeDescrambler struct {
	recorder *fakeDescramblerRecorder
}

func (d fakeDescrambler) Descramble(_ context.Context, src io.Reader, dst io.Writer) error {
	d.recorder.mu.Lock()
	d.recorder.startCount++
	d.recorder.mu.Unlock()

	data, err := io.ReadAll(src)
	if err != nil {
		return err
	}
	_, err = dst.Write([]byte("decoded:" + string(data)))
	return err
}
