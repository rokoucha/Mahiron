package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
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
	localSession := session.(*ChannelSession)
	if got, want := localSession.typ, "BS"; got != want {
		t.Fatalf("session type = %q, want public type %q", got, want)
	}
}

func TestManagerSharesLocalRouteAcrossLogicalChannels(t *testing.T) {
	no := false
	devices := &fakeTunerDeviceRecorder{}
	manager := NewStreamManager(StreamManagerConfig{
		Channels: config.ChannelsConfig{
			{
				Name: "NHK 1", Type: "GR", Channel: "27", IsDisabled: &no,
				Routes: []config.ChannelRouteConfig{{Id: "catv-27", Type: "CATV", Channel: "C27", IsDisabled: &no}},
			},
			{
				Name: "NHK 2", Type: "GR", Channel: "28", IsDisabled: &no,
				Routes: []config.ChannelRouteConfig{{Id: "catv-27", Type: "CATV", Channel: "C27", IsDisabled: &no}},
			},
		},
		TunerManager: fakeTunerManager{devices: devices},
	})

	first, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.GetOrCreate(context.Background(), "GR", "28")
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("different logical channels should keep distinct public sessions")
	}

	var firstOut bytes.Buffer
	var secondOut bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := first.ChannelStream(context.Background(), false, &firstOut); err != nil {
			t.Errorf("first stream: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := second.ChannelStream(context.Background(), false, &secondOut); err != nil {
			t.Errorf("second stream: %v", err)
		}
	}()
	wg.Wait()

	if got := devices.starts(); got != 1 {
		t.Fatalf("tuner device starts = %d, want 1", got)
	}
	if firstOut.String() == "" || secondOut.String() == "" {
		t.Fatalf("both logical streams should receive data: first=%q second=%q", firstOut.String(), secondOut.String())
	}
}

func TestManagerCoalescesConcurrentLocalRouteCreation(t *testing.T) {
	no := false
	tuners := &slowTunerManager{delay: 20 * time.Millisecond}
	manager := NewStreamManager(StreamManagerConfig{
		Channels: config.ChannelsConfig{
			{
				Name: "NHK 1", Type: "GR", Channel: "27", IsDisabled: &no,
				Routes: []config.ChannelRouteConfig{{Id: "catv-27", Type: "CATV", Channel: "C27", IsDisabled: &no}},
			},
			{
				Name: "NHK 2", Type: "GR", Channel: "28", IsDisabled: &no,
				Routes: []config.ChannelRouteConfig{{Id: "catv-27", Type: "CATV", Channel: "C27", IsDisabled: &no}},
			},
		},
		TunerManager: tuners,
	})

	var first Session
	var second Session
	var firstErr error
	var secondErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		first, firstErr = manager.GetOrCreate(context.Background(), "GR", "27")
	}()
	go func() {
		defer wg.Done()
		second, secondErr = manager.GetOrCreate(context.Background(), "GR", "28")
	}()
	wg.Wait()

	if firstErr != nil {
		t.Fatal(firstErr)
	}
	if secondErr != nil {
		t.Fatal(secondErr)
	}
	if first == nil || second == nil || first == second {
		t.Fatalf("sessions = %T/%T, want distinct non-nil sessions", first, second)
	}
	if got := tuners.acquires(); got != 1 {
		t.Fatalf("tuner acquires = %d, want 1", got)
	}
}

func TestManagerKeepsSharedRouteRunningUntilAllLogicalConsumersDetach(t *testing.T) {
	no := false
	device := &fakeLiveTunerDevice{}
	manager := NewStreamManager(StreamManagerConfig{
		Channels: config.ChannelsConfig{
			{
				Name: "NHK 1", Type: "GR", Channel: "27", IsDisabled: &no,
				Routes: []config.ChannelRouteConfig{{Id: "catv-27", Type: "CATV", Channel: "C27", IsDisabled: &no}},
			},
			{
				Name: "NHK 2", Type: "GR", Channel: "28", IsDisabled: &no,
				Routes: []config.ChannelRouteConfig{{Id: "catv-27", Type: "CATV", Channel: "C27", IsDisabled: &no}},
			},
		},
		TunerManager: fakeLiveTunerManager{device: device},
	})

	first, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.GetOrCreate(context.Background(), "GR", "28")
	if err != nil {
		t.Fatal(err)
	}

	firstCtx, firstCancel := context.WithCancel(context.Background())
	secondCtx, secondCancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() { firstDone <- first.ChannelStream(firstCtx, false, io.Discard) }()
	go func() { secondDone <- second.ChannelStream(secondCtx, false, io.Discard) }()

	time.Sleep(20 * time.Millisecond)
	firstCancel()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first stream did not stop")
	}
	if got := device.stopCount(); got != 0 {
		t.Fatalf("shared route stopped while second consumer was active: stops = %d", got)
	}

	secondCancel()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second stream did not stop")
	}
	if got := device.startCount(); got != 1 {
		t.Fatalf("tuner device starts = %d, want 1", got)
	}
	if !eventually(time.Second, func() bool { return device.stopCount() == 1 }) {
		t.Fatalf("tuner device stops = %d, want 1", device.stopCount())
	}
}

func TestManagerPassesTunerUserPriorityToAllocator(t *testing.T) {
	no := false
	tuners := &priorityCapturingTunerManager{}
	manager := NewStreamManager(StreamManagerConfig{
		Channels: config.ChannelsConfig{
			{Name: "NHK", Type: "GR", Channel: "27", IsDisabled: &no},
		},
		TunerManager: tuners,
	})

	ctx := tuner.WithUser(context.Background(), tuner.User{ID: "viewer", Priority: 7})
	if _, err := manager.GetOrCreate(ctx, "GR", "27"); err != nil {
		t.Fatal(err)
	}
	if got, want := tuners.priority, 7; got != want {
		t.Fatalf("allocator priority = %d, want %d", got, want)
	}
}

func TestManagerPassesBackgroundWaitToAllocator(t *testing.T) {
	no := false
	tuners := &priorityCapturingTunerManager{}
	manager := NewStreamManager(StreamManagerConfig{
		Channels:     config.ChannelsConfig{{Name: "NHK", Type: "GR", Channel: "27", IsDisabled: &no}},
		TunerManager: tuners,
	})
	if _, err := manager.GetOrCreateWait(context.Background(), "GR", "27"); err != nil {
		t.Fatal(err)
	}
	if !tuners.wait {
		t.Fatal("allocator wait = false, want true for background acquisition")
	}
}

func TestManagerSelectsRemoteRouteWhenLocalUnavailable(t *testing.T) {
	no := false
	priorityLocal := 10
	priorityRemote := 20
	previousNewRemoteClient := newRemoteClient
	t.Cleanup(func() { newRemoteClient = previousNewRemoteClient })
	newRemoteClient = func(remote config.RemoteConfig) *RemoteClient {
		client := NewRemoteClient(remote)
		client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch r.URL.Path {
			case "/api/tuners":
				return stringResponse(http.StatusOK, `[{"types":["GR"],"isAvailable":true,"isFree":true,"isFault":false}]`), nil
			case "/api/channels/GR/27/stream":
				return stringResponse(http.StatusOK, "remote-ts"), nil
			default:
				return stringResponse(http.StatusNotFound, ""), nil
			}
		})}
		return client
	}

	manager := NewStreamManager(StreamManagerConfig{
		Channels: config.ChannelsConfig{
			{
				Name: "NHK", Type: "GR", Channel: "27", IsDisabled: &no,
				Routes: []config.ChannelRouteConfig{
					{Id: "local", Type: "GR", Channel: "27", IsDisabled: &no, Priority: &priorityLocal},
					{Id: "remote", Remote: "living", Type: "GR", Channel: "27", IsDisabled: &no, Priority: &priorityRemote},
				},
			},
		},
		Remotes: config.RemotesConfig{{Name: "living", URL: "http://remote.local/api"}},
		TunerManager: &routeSelectingTunerManager{
			availableType: "BS",
		},
	})

	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := session.(*RemoteSession); !ok {
		t.Fatalf("session type = %T, want *RemoteSession", session)
	}
	var out bytes.Buffer
	if err := session.ChannelStream(context.Background(), false, &out); err != nil {
		t.Fatal(err)
	}
	if got, want := out.String(), "remote-ts"; got != want {
		t.Fatalf("remote stream = %q, want %q", got, want)
	}
}

func TestManagerSelectsRemoteRouteWhenRemoteAlreadyTunedToSameRoute(t *testing.T) {
	no := false
	previousNewRemoteClient := newRemoteClient
	t.Cleanup(func() { newRemoteClient = previousNewRemoteClient })
	newRemoteClient = func(remote config.RemoteConfig) *RemoteClient {
		client := NewRemoteClient(remote)
		client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch r.URL.Path {
			case "/api/tuners":
				return stringResponse(http.StatusOK, `[{
					"types":["CATV"],
					"isAvailable":true,
					"isFree":false,
					"isFault":false,
					"tunedChannelType":"CATV",
					"tunedChannel":"C27"
				}]`), nil
			case "/api/channels/CATV/C27/stream":
				return stringResponse(http.StatusOK, "remote-ts"), nil
			default:
				return stringResponse(http.StatusNotFound, ""), nil
			}
		})}
		return client
	}

	manager := NewStreamManager(StreamManagerConfig{
		Channels: config.ChannelsConfig{
			{
				Name: "NHK", Type: "GR", Channel: "27", IsDisabled: &no,
				Routes: []config.ChannelRouteConfig{
					{Id: "remote-catv", Remote: "living", Type: "CATV", Channel: "C27", IsDisabled: &no},
				},
			},
		},
		Remotes:      config.RemotesConfig{{Name: "living", URL: "http://remote.local/api"}},
		TunerManager: &routeSelectingTunerManager{availableType: "BS"},
	})

	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := session.ChannelStream(context.Background(), false, &out); err != nil {
		t.Fatal(err)
	}
	if got, want := out.String(), "remote-ts"; got != want {
		t.Fatalf("remote stream = %q, want %q", got, want)
	}
}

func TestManagerFallsBackWhenRemoteRouteUnavailable(t *testing.T) {
	no := false
	priorityRemote := 10
	priorityLocal := 20
	requests := make(chan string, 4)
	previousNewRemoteClient := newRemoteClient
	t.Cleanup(func() { newRemoteClient = previousNewRemoteClient })
	newRemoteClient = func(remote config.RemoteConfig) *RemoteClient {
		client := NewRemoteClient(remote)
		client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			requests <- r.URL.Path
			if r.URL.Path != "/tuners" {
				return stringResponse(http.StatusNotFound, ""), nil
			}
			return stringResponse(http.StatusOK, `[{
				"types":["GR"],
				"isAvailable":true,
				"isFree":false,
				"isFault":false,
				"tunedChannelType":"GR",
				"tunedChannel":"28"
			}]`), nil
		})}
		return client
	}

	manager := NewStreamManager(StreamManagerConfig{
		Channels: config.ChannelsConfig{
			{
				Name: "NHK", Type: "GR", Channel: "27", IsDisabled: &no,
				Routes: []config.ChannelRouteConfig{
					{Id: "remote", Remote: "living", Type: "GR", Channel: "27", IsDisabled: &no, Priority: &priorityRemote},
					{Id: "local", Type: "GR", Channel: "27", IsDisabled: &no, Priority: &priorityLocal},
				},
			},
		},
		Remotes: config.RemotesConfig{{Name: "living", URL: "http://remote.local"}},
		TunerManager: &routeSelectingTunerManager{
			availableType: "GR",
		},
	})

	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := session.(*ChannelSession); !ok {
		t.Fatalf("session type = %T, want *ChannelSession", session)
	}
	select {
	case request := <-requests:
		if request != "/tuners" {
			t.Fatalf("remote precheck request = %q, want /tuners", request)
		}
	default:
		t.Fatal("remote route was not prechecked")
	}
}

func TestManagerStartsRemoteProgramEventSyncOutsideSessionLifecycle(t *testing.T) {
	no := false
	requests := make(chan string, 2)
	previousNewRemoteClient := newRemoteClient
	t.Cleanup(func() { newRemoteClient = previousNewRemoteClient })
	newRemoteClient = func(remote config.RemoteConfig) *RemoteClient {
		client := NewRemoteClient(remote)
		client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/api/tuners" {
				return stringResponse(http.StatusOK, `[{"types":["GR"],"isAvailable":true,"isFree":true,"isFault":false}]`), nil
			}
			requests <- r.URL.Path + "?" + r.URL.RawQuery
			<-r.Context().Done()
			return nil, r.Context().Err()
		})}
		return client
	}

	manager := NewStreamManager(StreamManagerConfig{
		Channels: config.ChannelsConfig{{
			Name:       "NHK",
			Type:       "GR",
			Channel:    "27",
			IsDisabled: &no,
			Routes: []config.ChannelRouteConfig{
				{Id: "remote", Remote: "living", Type: "GR", Channel: "27", IsDisabled: &no},
			},
		}},
		ProgramUpdater: &recordingProgramUpdater{},
		Remotes:        config.RemotesConfig{{Name: "living", URL: "http://remote.local/api"}},
	})

	ctx, cancel := context.WithCancel(context.Background())
	manager.StartRemoteProgramEventSync(ctx)
	select {
	case request := <-requests:
		if request != "/api/events/stream?resource=program" {
			t.Fatalf("request = %q, want /api/events/stream?resource=program", request)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for remote program event sync request")
	}

	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := session.(*RemoteSession); !ok {
		t.Fatalf("session type = %T, want *RemoteSession", session)
	}
	select {
	case request := <-requests:
		t.Fatalf("unexpected additional event sync request after session creation: %q", request)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
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

	if got, want := out.Len(), 2*ts.PacketSize; got != want {
		t.Fatalf("raw stream bytes = %d, want %d", got, want)
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

func TestDecodedStreamSharesOneTunerDevice(t *testing.T) {
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
	if got, want := rawOut.Len(), 2*ts.PacketSize; got != want {
		t.Fatalf("raw stream bytes = %d, want %d", got, want)
	}
	if !bytes.Equal(decodedOut.Bytes(), rawOut.Bytes()) {
		t.Fatal("decoded stream does not match passthrough descrambler output")
	}
	if got := descramblers.starts(); got != 1 {
		t.Fatalf("descrambler starts = %d, want 1", got)
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
		}, nil),
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

func eventually(timeout time.Duration, ok func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return ok()
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

type priorityCapturingTunerManager struct {
	priority int
	wait     bool
}

func (m *priorityCapturingTunerManager) NewDeviceByType(string, *config.ChannelConfig) (tuner.Device, error) {
	return &fakeTunerDevice{done: make(chan struct{})}, nil
}

func (m *priorityCapturingTunerManager) AcquireDevice(ctx context.Context, _ string, _, _ *config.ChannelConfig, wait bool) (tuner.Device, string, error) {
	user, _ := tuner.UserFromContext(ctx)
	m.priority = user.Priority
	m.wait = wait
	return &fakeTunerDevice{done: make(chan struct{})}, "", nil
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

type slowTunerManager struct {
	delay time.Duration
	mu    sync.Mutex
	count int
}

func (m *slowTunerManager) NewDeviceByType(string, *config.ChannelConfig) (tuner.Device, error) {
	m.mu.Lock()
	m.count++
	m.mu.Unlock()
	time.Sleep(m.delay)
	return &fakeTunerDevice{done: make(chan struct{})}, nil
}

func (m *slowTunerManager) acquires() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.count
}

func (d *fakeTunerDevice) Start(_ context.Context, dst io.Writer) error {
	d.mu.Lock()
	d.starts++
	d.mu.Unlock()
	go func() {
		time.Sleep(10 * time.Millisecond)
		_, err := dst.Write(engineTestPacket(0x0100, 0))
		if err == nil {
			time.Sleep(20 * time.Millisecond)
			_, err = dst.Write(engineTestPacket(0x0100, 1))
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
	stops  int
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
	d.stops++
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

func (d *fakeLiveTunerDevice) stopCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stops
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

	_, err := io.Copy(dst, src)
	return err
}
