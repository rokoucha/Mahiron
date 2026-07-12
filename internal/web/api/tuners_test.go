package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/server/middleware"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/stream"
	"github.com/21S1298001/mahiron/internal/tuner"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func TestGetTunersAndGetTuner(t *testing.T) {
	handler := NewHandler(HandlerConfig{TunerManager: tuner.NewTunerManager(&tuner.TunerManagerConfig{
		TunersConfig: config.TunersConfig{{Name: "first", Types: []string{"GR"}, Command: "sleep 1"}},
	})})
	res, err := handler.GetTuners(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	items, ok := res.(*apigen.GetTunersOKApplicationJSON)
	if !ok || len(*items) != 1 {
		t.Fatalf("response = %T, items = %v", res, items)
	}
	if (*items)[0].Index != 0 || (*items)[0].Name != "first" || !(*items)[0].IsFree {
		t.Fatalf("unexpected tuner: %+v", (*items)[0])
	}

	item, err := handler.GetTuner(context.Background(), apigen.GetTunerParams{Index: 0})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := item.(*apigen.TunerDevice); !ok {
		t.Fatalf("response = %T", item)
	}
	missing, err := handler.GetTuner(context.Background(), apigen.GetTunerParams{Index: 1})
	if err != nil {
		t.Fatal(err)
	}
	if response, ok := missing.(*apigen.ErrorStatusCode); !ok || response.StatusCode != 404 {
		t.Fatalf("missing response = %#v", missing)
	}
}

func TestGetTunersIncludesRemoteTuners(t *testing.T) {
	localTuners := tuner.NewTunerManager(&tuner.TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "local-first", Types: []string{"BS"}, Command: "sleep 1"},
	}})
	streamManager := remoteTunerStatusProvider{StreamManager: stream.NewStreamManager(stream.StreamManagerConfig{TunerManager: localTuners})}
	handler := NewHandler(HandlerConfig{TunerManager: localTuners, StreamManager: streamManager})

	res, err := handler.GetTuners(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	items := *(res.(*apigen.GetTunersOKApplicationJSON))
	if len(items) != 2 {
		t.Fatalf("tuners = %+v, want local and remote", items)
	}
	got := items[1]
	if !got.IsRemote || got.Name != "living / remote-first" || !got.IsUsing || got.Index >= 0 {
		t.Fatalf("remote tuner = %+v", got)
	}
	if channel, ok := got.CurrentChannel.Get(); !ok || channel != "27" {
		t.Fatalf("currentChannel = %q, %v", channel, ok)
	}
}

type remoteTunerStatusProvider struct{ *stream.StreamManager }

func (remoteTunerStatusProvider) RemoteTunerStatuses(context.Context) []stream.RemoteTunerStatus {
	return []stream.RemoteTunerStatus{{
		Remote: "living",
		Status: tuner.Status{
			Index: 0, Name: "remote-first", Types: []string{"GR"},
			IsAvailable: true, IsUsing: true,
			CurrentChannelType: "GR", CurrentChannel: "27",
		},
	}}
}

func TestGetTunerProcess(t *testing.T) {
	handler := NewHandler(HandlerConfig{TunerManager: tuner.NewTunerManager(&tuner.TunerManagerConfig{
		TunersConfig: config.TunersConfig{{Name: "first", Types: []string{"GR"}, Command: "sleep 1"}},
	})})
	res, err := handler.GetTunerProcess(context.Background(), apigen.GetTunerProcessParams{Index: 0})
	if err != nil {
		t.Fatal(err)
	}
	process, ok := res.(*apigen.TunerProcess)
	if !ok {
		t.Fatalf("response = %T", res)
	}
	if process.Pid != 0 {
		t.Fatalf("pid = %d, want 0 for idle tuner", process.Pid)
	}

	missing, err := handler.GetTunerProcess(context.Background(), apigen.GetTunerProcessParams{Index: 1})
	if err != nil {
		t.Fatal(err)
	}
	if response, ok := missing.(*apigen.ErrorStatusCode); !ok || response.StatusCode != 404 {
		t.Fatalf("missing response = %#v", missing)
	}
}

func TestKillTunerProcess(t *testing.T) {
	handler := NewHandler(HandlerConfig{TunerManager: tuner.NewTunerManager(&tuner.TunerManagerConfig{
		TunersConfig: config.TunersConfig{{Name: "first", Types: []string{"GR"}, Command: "sleep 1"}},
	})})
	res, err := handler.KillTunerProcess(context.Background(), apigen.KillTunerProcessParams{Index: 0})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.KillTunerProcessNoContent); !ok {
		t.Fatalf("response = %T", res)
	}

	missing, err := handler.KillTunerProcess(context.Background(), apigen.KillTunerProcessParams{Index: 1})
	if err != nil {
		t.Fatal(err)
	}
	if response, ok := missing.(*apigen.ErrorStatusCode); !ok || response.StatusCode != 404 {
		t.Fatalf("missing response = %#v", missing)
	}
}

func TestApiTunerIncludesLogicalAndTunedChannels(t *testing.T) {
	no := false
	serviceID := uint32(101)
	priority := 2
	item := apiTuner(tuner.Status{
		CurrentChannelType: "BS", CurrentChannel: "101",
		TunedChannelType: "CATV", TunedChannel: "C13",
		Users: []tuner.User{{
			ID:       "viewer",
			Priority: 1,
			StreamSetting: tuner.StreamSetting{
				Channel: &config.ChannelConfig{
					Name:        "NHK",
					Type:        "GR",
					Channel:     "27",
					ServiceId:   &serviceID,
					CommandVars: map[string]any{"freq": 12345, "satellite": "SOMESAT"},
					IsDisabled:  &no,
					Routes: []config.ChannelRouteConfig{{
						Id:          "catv",
						Type:        "CATV",
						Channel:     "C27",
						ServiceId:   &serviceID,
						CommandVars: map[string]any{"freq": 23456},
						IsDisabled:  &no,
						Priority:    &priority,
					}},
				},
			},
			StreamInfo: map[string]tuner.StreamInfo{
				"BS/101": {Packet: 10, Drop: 1},
			},
		}},
	})
	if value, ok := item.CurrentChannel.Get(); !ok || value != "101" {
		t.Fatalf("currentChannel = %q, %v", value, ok)
	}
	if value, ok := item.TunedChannel.Get(); !ok || value != "C13" {
		t.Fatalf("tunedChannel = %q, %v", value, ok)
	}
	info, ok := item.Users[0].StreamInfo.Get()
	if !ok || info["BS/101"].Packet != 10 || info["BS/101"].Drop != 1 {
		t.Fatalf("streamInfo = %+v, %v", info, ok)
	}
	setting, ok := item.Users[0].StreamSetting.Get()
	if !ok {
		t.Fatal("streamSetting should be set")
	}
	if got, want := len(setting.Channel.Routes), 1; got != want {
		t.Fatalf("streamSetting.channel.routes length = %d, want %d", got, want)
	}
	if got, want := setting.Channel.Routes[0].ID.Value, "catv"; got != want {
		t.Fatalf("streamSetting.channel.routes[0].id = %q, want %q", got, want)
	}
	if got, want := setting.Channel.Routes[0].Priority.Value, 2; got != want {
		t.Fatalf("streamSetting.channel.routes[0].priority = %d, want %d", got, want)
	}
	commandVars, ok := setting.Channel.CommandVars.Get()
	if !ok {
		t.Fatal("streamSetting.channel.commandVars should be set")
	}
	var freq int
	if err := json.Unmarshal(commandVars["freq"], &freq); err != nil {
		t.Fatal(err)
	}
	if freq != 12345 {
		t.Fatalf("streamSetting.channel.commandVars.freq = %d, want 12345", freq)
	}
	routeCommandVars, ok := setting.Channel.Routes[0].CommandVars.Get()
	if !ok {
		t.Fatal("streamSetting.channel.routes[0].commandVars should be set")
	}
	if err := json.Unmarshal(routeCommandVars["freq"], &freq); err != nil {
		t.Fatal(err)
	}
	if freq != 23456 {
		t.Fatalf("streamSetting.channel.routes[0].commandVars.freq = %d, want 23456", freq)
	}
}

func TestChannelStreamReturnsTrackedTunerUserID(t *testing.T) {
	no := false
	channels := config.ChannelsConfig{{Name: "Test", Type: "GR", Channel: "27", IsDisabled: &no}}
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	tunerManager := tuner.NewTunerManager(&tuner.TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "first", Types: []string{"GR"}, Command: "sleep 10"},
	}})
	handler := NewHandler(HandlerConfig{
		TunerManager:   tunerManager,
		ServiceManager: service.NewServiceManager(service.NewSQLiteStore(database), channels),
		StreamManager: stream.NewStreamManager(stream.StreamManagerConfig{
			Channels: channels, TunerManager: tunerManager,
		}),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res, err := handler.GetChannelStream(ctx, apigen.GetChannelStreamParams{
		Type: "GR", Channel: "27", XMirakurunPriority: apigen.NewOptInt(3),
	})
	if err != nil {
		t.Fatal(err)
	}
	response, ok := res.(*apigen.GetChannelStreamOKHeaders)
	if !ok {
		t.Fatalf("response = %T", res)
	}
	userID, ok := response.XMirakurunTunerUserID.Get()
	if !ok || userID == "" {
		t.Fatal("X-Mirakurun-Tuner-User-ID is missing")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		status, _ := tunerManager.Status(0)
		if len(status.Users) == 1 {
			if status.Users[0].ID != userID || status.Users[0].Priority != 3 {
				t.Fatalf("tracked user = %+v, header = %q", status.Users[0], userID)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream user was not tracked: %+v", status)
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	if closer, ok := response.Response.Data.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		status, _ := tunerManager.Status(0)
		if len(status.Users) == 0 && status.IsFree {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream user was not released: %+v", status)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestTunerUserContextIncludesRequestMetadata(t *testing.T) {
	var tracked tuner.User
	handler := middleware.RequestInfoMiddleware().Handler(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		ctx, id := tunerUserContext(request.Context(), apigen.NewOptInt(2), false, &config.ChannelConfig{Type: "GR", Channel: "27"}, nil, nil)
		var ok bool
		tracked, ok = tuner.UserFromContext(ctx)
		if !ok || tracked.ID != id {
			t.Errorf("tracked user = %+v, id = %q", tracked, id)
		}
	}))
	request := httptest.NewRequest(http.MethodGet, "/api/channels/GR/27/stream?decode=0", nil)
	request.Header.Set("User-Agent", "test-agent")
	handler.ServeHTTP(httptest.NewRecorder(), request)
	if tracked.Agent != "192.0.2.1 test-agent" || tracked.URL != "/api/channels/GR/27/stream?decode=0" {
		t.Fatalf("request metadata = %+v", tracked)
	}
	if tracked.Priority != 2 || !tracked.DisableDecoder {
		t.Fatalf("stream options = %+v", tracked)
	}
}
