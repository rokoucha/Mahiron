package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/db"
	"github.com/21S1298001/Mahiron5/internal/server/middleware"
	"github.com/21S1298001/Mahiron5/internal/service"
	"github.com/21S1298001/Mahiron5/internal/stream"
	"github.com/21S1298001/Mahiron5/internal/tuner"
	apigen "github.com/21S1298001/Mahiron5/internal/web/api/gen"
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
	item := apiTuner(tuner.Status{
		CurrentChannelType: "BS", CurrentChannel: "101",
		TunedChannelType: "CATV", TunedChannel: "C13",
	})
	if value, ok := item.CurrentChannel.Get(); !ok || value != "101" {
		t.Fatalf("currentChannel = %q, %v", value, ok)
	}
	if value, ok := item.TunedChannel.Get(); !ok || value != "C13" {
		t.Fatalf("tunedChannel = %q, %v", value, ok)
	}
}

func TestChannelStreamReturnsTrackedTunerUserID(t *testing.T) {
	no := false
	channels := config.ChannelsConfig{{Name: "Test", Type: "GR", Channel: "27", IsDisabled: &no}}
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	tunerManager := tuner.NewTunerManager(&tuner.TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "first", Types: []string{"GR"}, Command: "sleep 10"},
	}})
	handler := NewHandler(HandlerConfig{
		TunerManager:   tunerManager,
		ServiceManager: service.NewServiceManager(service.NewSQLiteStore(database), channels),
		StreamManager: stream.NewAPIStreamAdapter(stream.NewStreamManager(stream.StreamManagerConfig{
			Channels: channels, TunerManager: tunerManager,
		})),
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
	if tracked.Agent != "test-agent" || tracked.URL != "/api/channels/GR/27/stream?decode=0" {
		t.Fatalf("request metadata = %+v", tracked)
	}
	if tracked.Priority != 2 || !tracked.DisableDecoder {
		t.Fatalf("stream options = %+v", tracked)
	}
}
