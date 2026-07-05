package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/stream/internal/streamtest"
	"github.com/21S1298001/mahiron/internal/stream/local"
	"github.com/21S1298001/mahiron/internal/stream/remote"
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

func TestEnsureUserContextUsesJobNameForInternalAgent(t *testing.T) {
	ctx := run.WithJob(context.Background(), run.JobInfo{Name: "EPG Gather NID 6"})
	user, ok := tuner.UserFromContext(ensureUserContext(ctx, "GR", "27"))
	if !ok {
		t.Fatal("user context not set")
	}
	if user.Agent != "EPG Gather NID 6" {
		t.Fatalf("agent = %q, want EPG Gather NID 6", user.Agent)
	}
}

func TestEnsureUserContextKeepsExplicitUser(t *testing.T) {
	ctx := run.WithJob(context.Background(), run.JobInfo{Name: "EPG Gather NID 6"})
	ctx = tuner.WithUser(ctx, tuner.User{ID: "viewer", Agent: "Viewer"})
	user, ok := tuner.UserFromContext(ensureUserContext(ctx, "GR", "27"))
	if !ok {
		t.Fatal("user context not set")
	}
	if user.Agent != "Viewer" {
		t.Fatalf("agent = %q, want Viewer", user.Agent)
	}
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
	routeManager := &routeSelectingTunerManager{
		availableType: "CATV_BS",
	}
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
		TunerManager: routeManager,
	})

	session, err := manager.GetOrCreate(context.Background(), "BS", "101")
	if err != nil {
		t.Fatal(err)
	}

	if got, want := routeManager.channelType, "CATV_BS"; got != want {
		t.Fatalf("device channel type = %q, want %q", got, want)
	}
	if got, want := routeManager.channelID, "C101"; got != want {
		t.Fatalf("device channel = %q, want %q", got, want)
	}
	localSession := session.(*local.Session)
	if got, want := localSession.Type(), "BS"; got != want {
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

// fakeDeadSession is a Session with a distinguishing id field so two
// instances compare unequal, letting tests verify identity-guarded eviction.
type fakeDeadSession struct{ id string }

func (fakeDeadSession) ChannelStream(context.Context, bool, io.Writer) error { return nil }
func (fakeDeadSession) ProgramStream(context.Context, *program.Program, bool, io.Writer) error {
	return nil
}
func (fakeDeadSession) ServiceStream(context.Context, uint16, bool, io.Writer) error { return nil }
func (fakeDeadSession) ScanServices(context.Context) ([]ts.ServiceInfo, error)       { return nil, nil }
func (fakeDeadSession) CollectEIT(context.Context, func(*ts.EIT) error) error        { return nil }
func (fakeDeadSession) ObserveLogos(context.Context, func(*ts.LogoImage) error) error {
	return nil
}
func (fakeDeadSession) ObserveDataBroadcast(context.Context, uint16, bool, func(DataBroadcastEvent) error) error {
	return nil
}
func (fakeDeadSession) DataBroadcastModule(uint16, byte, uint16) (DataBroadcastModule, bool) {
	return DataBroadcastModule{}, false
}
func (fakeDeadSession) Stop(context.Context) error { return nil }
func (fakeDeadSession) Alive() bool                { return false }

func TestManagerGetOrCreateRetriesDeadRegistryEntry(t *testing.T) {
	devices := &fakeTunerDeviceRecorder{}
	manager := testManager(t, devices)
	key := sessionKey{typ: "GR", channel: "27"}
	stale := fakeDeadSession{id: "stale"}
	manager.registry.mu.Lock()
	manager.registry.sessions[key] = sessionEntry{session: stale}
	manager.registry.mu.Unlock()

	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}
	if session == Session(stale) {
		t.Fatal("expected a fresh session, got the stale dead one")
	}

	manager.registry.mu.Lock()
	entry, stillPresent := manager.registry.sessions[key]
	manager.registry.mu.Unlock()
	if stillPresent && entry.session == Session(stale) {
		t.Fatal("stale session entry was not evicted from the registry")
	}
}

func TestSessionRegistryRemoveIfSameKeepsNewerSession(t *testing.T) {
	registry := newSessionRegistry()
	key := sessionKey{typ: "GR", channel: "27"}
	oldSession := fakeDeadSession{id: "old"}
	newSession := fakeDeadSession{id: "new"}

	registry.mu.Lock()
	registry.sessions[key] = sessionEntry{session: newSession}
	registry.mu.Unlock()

	registry.removeIfSame(key, oldSession)

	registry.mu.Lock()
	entry, ok := registry.sessions[key]
	registry.mu.Unlock()
	if !ok || entry.session != Session(newSession) {
		t.Fatal("removeIfSame evicted a session that did not match the stale reference")
	}
}

func TestManagerDoesNotBlockHasSessionDuringAcquire(t *testing.T) {
	no := false
	tuners := newBlockingTunerManager("27")
	manager := NewStreamManager(StreamManagerConfig{
		Channels:     config.ChannelsConfig{{Name: "NHK", Type: "GR", Channel: "27", IsDisabled: &no}},
		TunerManager: tuners,
	})

	acquireDone := make(chan error, 1)
	go func() {
		_, err := manager.GetOrCreateWait(context.Background(), "GR", "27")
		acquireDone <- err
	}()
	tuners.waitEntered(t, "27")

	hasSessionDone := make(chan bool, 1)
	go func() { hasSessionDone <- manager.HasSession("GR", "27") }()
	select {
	case ok := <-hasSessionDone:
		if ok {
			t.Fatal("HasSession = true while session is still being created")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("HasSession blocked while session source acquisition was in progress")
	}

	tuners.releaseBlocked()
	if err := <-acquireDone; err != nil {
		t.Fatal(err)
	}
}

func TestManagerAllowsDifferentSessionCreationDuringAcquire(t *testing.T) {
	no := false
	tuners := newBlockingTunerManager("27")
	manager := NewStreamManager(StreamManagerConfig{
		Channels: config.ChannelsConfig{
			{Name: "NHK 1", Type: "GR", Channel: "27", IsDisabled: &no},
			{Name: "NHK 2", Type: "GR", Channel: "28", IsDisabled: &no},
		},
		TunerManager: tuners,
	})

	firstDone := make(chan error, 1)
	go func() {
		_, err := manager.GetOrCreateWait(context.Background(), "GR", "27")
		firstDone <- err
	}()
	tuners.waitEntered(t, "27")

	secondDone := make(chan error, 1)
	go func() {
		_, err := manager.GetOrCreate(context.Background(), "GR", "28")
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("different session creation blocked while another source acquisition was in progress")
	}

	tuners.releaseBlocked()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestManagerCoalescesConcurrentSameSessionCreation(t *testing.T) {
	no := false
	tuners := newBlockingTunerManager("27")
	manager := NewStreamManager(StreamManagerConfig{
		Channels:     config.ChannelsConfig{{Name: "NHK", Type: "GR", Channel: "27", IsDisabled: &no}},
		TunerManager: tuners,
	})

	var first Session
	var second Session
	firstDone := make(chan error, 1)
	go func() {
		var err error
		first, err = manager.GetOrCreateWait(context.Background(), "GR", "27")
		firstDone <- err
	}()
	tuners.waitEntered(t, "27")

	secondDone := make(chan error, 1)
	go func() {
		var err error
		second, err = manager.GetOrCreateWait(context.Background(), "GR", "27")
		secondDone <- err
	}()

	tuners.releaseBlocked()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if first == nil || first != second {
		t.Fatalf("sessions = %T/%T, want same non-nil session", first, second)
	}
	if got := tuners.acquireCount(); got != 1 {
		t.Fatalf("tuner acquires = %d, want 1", got)
	}
}

func TestManagerShutdownWaitsForInflightSessionWithoutHoldingLock(t *testing.T) {
	no := false
	device := &fakeLiveTunerDevice{}
	tuners := newBlockingTunerManager("27")
	tuners.devices["27"] = device
	manager := NewStreamManager(StreamManagerConfig{
		Channels:     config.ChannelsConfig{{Name: "NHK", Type: "GR", Channel: "27", IsDisabled: &no}},
		TunerManager: tuners,
	})

	createDone := make(chan error, 1)
	go func() {
		_, err := manager.GetOrCreateWait(context.Background(), "GR", "27")
		createDone <- err
	}()
	tuners.waitEntered(t, "27")

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- manager.Shutdown(context.Background()) }()
	if !eventually(time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		_, err := manager.GetOrCreate(ctx, "GR", "27")
		return errors.Is(err, ErrStreamManagerShutdown)
	}) {
		t.Fatal("manager did not reject new sessions after shutdown started")
	}
	hasSessionDone := make(chan bool, 1)
	go func() { hasSessionDone <- manager.HasSession("GR", "27") }()
	select {
	case <-hasSessionDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("HasSession blocked while Shutdown waited for in-flight session creation")
	}

	tuners.releaseBlocked()
	if err := <-createDone; err != nil {
		t.Fatal(err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	if got := device.stopCount(); got != 1 {
		t.Fatalf("tuner device stops = %d, want 1", got)
	}
}

func TestManagerSelectsRemoteRouteWhenLocalUnavailable(t *testing.T) {
	no := false
	priorityLocal := 10
	priorityRemote := 20
	previousNewRemoteClient := newRemoteClient
	t.Cleanup(func() { newRemoteClient = previousNewRemoteClient })
	newRemoteClient = func(cfg config.RemoteConfig) *remote.Client {
		return remote.NewClient(cfg, remote.WithHTTPClient(&http.Client{Transport: streamtest.RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch r.URL.Path {
			case "/api/tuners":
				return streamtest.StringResponse(http.StatusOK, `[{"types":["GR"],"isAvailable":true,"isFree":true,"isFault":false}]`), nil
			case "/api/channels/GR/27/stream":
				return streamtest.StringResponse(http.StatusOK, "remote-ts"), nil
			default:
				return streamtest.StringResponse(http.StatusNotFound, ""), nil
			}
		})}))
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
	if _, ok := session.(*remote.Session); !ok {
		t.Fatalf("session type = %T, want *remote.Session", session)
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
	newRemoteClient = func(cfg config.RemoteConfig) *remote.Client {
		return remote.NewClient(cfg, remote.WithHTTPClient(&http.Client{Transport: streamtest.RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch r.URL.Path {
			case "/api/tuners":
				return streamtest.StringResponse(http.StatusOK, `[{
					"types":["CATV"],
					"isAvailable":true,
					"isFree":false,
					"isFault":false,
					"tunedChannelType":"CATV",
					"tunedChannel":"C27"
				}]`), nil
			case "/api/channels/CATV/C27/stream":
				return streamtest.StringResponse(http.StatusOK, "remote-ts"), nil
			default:
				return streamtest.StringResponse(http.StatusNotFound, ""), nil
			}
		})}))
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
	newRemoteClient = func(cfg config.RemoteConfig) *remote.Client {
		return remote.NewClient(cfg, remote.WithHTTPClient(&http.Client{Transport: streamtest.RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			requests <- r.URL.Path
			if r.URL.Path != "/tuners" {
				return streamtest.StringResponse(http.StatusNotFound, ""), nil
			}
			return streamtest.StringResponse(http.StatusOK, `[{
				"types":["GR"],
				"isAvailable":true,
				"isFree":false,
				"isFault":false,
				"tunedChannelType":"GR",
				"tunedChannel":"28"
			}]`), nil
		})}))
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
	if _, ok := session.(*local.Session); !ok {
		t.Fatalf("session type = %T, want *local.Session", session)
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
	newRemoteClient = func(cfg config.RemoteConfig) *remote.Client {
		return remote.NewClient(cfg, remote.WithHTTPClient(&http.Client{Transport: streamtest.RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/api/tuners" {
				return streamtest.StringResponse(http.StatusOK, `[{"types":["GR"],"isAvailable":true,"isFree":true,"isFault":false}]`), nil
			}
			requests <- r.URL.Path + "?" + r.URL.RawQuery
			<-r.Context().Done()
			return nil, r.Context().Err()
		})}))
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
		ProgramUpdater: &streamtest.RecordingProgramUpdater{},
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
	if _, ok := session.(*remote.Session); !ok {
		t.Fatalf("session type = %T, want *remote.Session", session)
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

func TestChannelStreamRawTS(t *testing.T) {
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

func TestChannelStreamWithoutUserContextGetsLowestPriority(t *testing.T) {
	devices := &fakeTunerDeviceRecorder{}
	manager := testManager(t, devices)
	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}

	ctx := run.WithJob(context.Background(), run.JobInfo{Name: "EPG Gather NID 6"})
	var out bytes.Buffer
	if err := session.ChannelStream(ctx, false, &out); err != nil {
		t.Fatal(err)
	}

	user := devices.lastDevice().lastUser()
	if user.Priority != -1 {
		t.Fatalf("tracked tuner user priority = %d, want -1", user.Priority)
	}
	if user.Agent != "EPG Gather NID 6" {
		t.Fatalf("tracked tuner user agent = %q, want %q", user.Agent, "EPG Gather NID 6")
	}
}

func TestConcurrentChannelStreamsStartOneTunerDevice(t *testing.T) {
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

func (r *fakeTunerDeviceRecorder) lastDevice() *fakeTunerDevice {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.devices[len(r.devices)-1]
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

type fakeTunerDevice struct {
	done   chan struct{}
	err    error
	mu     sync.Mutex
	starts int
	users  []tuner.User
}

func (d *fakeTunerDevice) AddUser(user tuner.User) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.users = append(d.users, user)
}

func (d *fakeTunerDevice) RemoveUser(string) {}

func (d *fakeTunerDevice) lastUser() tuner.User {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.users[len(d.users)-1]
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

type blockingTunerManager struct {
	blockChannel string
	devices      map[string]tuner.Device
	entered      chan string
	release      chan struct{}
	mu           sync.Mutex
	count        int
}

func newBlockingTunerManager(blockChannel string) *blockingTunerManager {
	return &blockingTunerManager{
		blockChannel: blockChannel,
		devices:      map[string]tuner.Device{},
		entered:      make(chan string, 8),
		release:      make(chan struct{}),
	}
}

func (m *blockingTunerManager) NewDeviceByType(string, *config.ChannelConfig) (tuner.Device, error) {
	return &fakeTunerDevice{done: make(chan struct{})}, nil
}

func (m *blockingTunerManager) AcquireDevice(ctx context.Context, _ string, requested, _ *config.ChannelConfig, _ bool) (tuner.Device, string, error) {
	m.mu.Lock()
	m.count++
	m.mu.Unlock()
	if requested.Channel == m.blockChannel {
		m.entered <- requested.Channel
		select {
		case <-m.release:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	if device := m.devices[requested.Channel]; device != nil {
		return device, "", nil
	}
	return &fakeTunerDevice{done: make(chan struct{})}, "", nil
}

func (m *blockingTunerManager) waitEntered(t *testing.T, channel string) {
	t.Helper()
	select {
	case got := <-m.entered:
		if got != channel {
			t.Fatalf("entered channel = %q, want %q", got, channel)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for channel %q acquisition", channel)
	}
}

func (m *blockingTunerManager) releaseBlocked() {
	close(m.release)
}

func (m *blockingTunerManager) acquireCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.count
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
		_, err := dst.Write(streamtest.TestPacket(0x0100, 0))
		if err == nil {
			time.Sleep(20 * time.Millisecond)
			_, err = dst.Write(streamtest.TestPacket(0x0100, 1))
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
