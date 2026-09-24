package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/event"
	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/mirakurun"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func TestOpenAPIDoesNotExposeContainerHostileOperations(t *testing.T) {
	data, err := os.ReadFile("api.yml")
	if err != nil {
		t.Fatal(err)
	}
	spec := string(data)
	for _, operationID := range []string{
		"getChannelsConfig",
		"updateChannelsConfig",
		"updateServerConfig",
		"getTunersConfig",
		"updateTunersConfig",
		"channelScan",
		"getChannelScanStatus",
		"stopChannelScan",
		"updateVersion",
		"restart",
	} {
		if strings.Contains(spec, "operationId: "+operationID) {
			t.Fatalf("api.yml exposes excluded operationId %q", operationID)
		}
	}
}

func TestOpenAPIExposesReadOnlyServerConfig(t *testing.T) {
	data, err := os.ReadFile("api.yml")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "operationId: getServerConfig") {
		t.Fatal("api.yml does not expose getServerConfig")
	}
}

func TestXMirakurunPriorityHeaderAcceptsNegativeOne(t *testing.T) {
	handler, _ := testStreamHeadHandler(t)
	server, err := apigen.NewServer(handler, handler)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodHead, "/channels/GR/27/stream", nil)
	req.Header.Set("X-Mirakurun-Priority", "-1")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code == http.StatusBadRequest {
		t.Fatalf("status = %d, want X-Mirakurun-Priority: -1 to pass validation, body = %s", rec.Code, rec.Body.String())
	}
}

// The official mirakurun npm client spreads the path-level parameters without a
// nil guard (`[...p.parameters, ...(p.get.parameters || [])]`), so every path
// item must carry a parameters array even when it is empty.
func TestOpenAPIPathItemsAlwaysDeclareParameters(t *testing.T) {
	res, err := GetApiDocumentation(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	docs, ok := res.(*apigen.GetApiDocumentationOK)
	if !ok {
		t.Fatalf("response type = %T, want *GetApiDocumentationOK", res)
	}
	raw, ok := (*docs)["paths"]
	if !ok {
		t.Fatal("docs has no paths")
	}

	var paths map[string]map[string]json.RawMessage
	if err := json.Unmarshal(raw, &paths); err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("docs paths is empty")
	}
	for path, item := range paths {
		params, ok := item["parameters"]
		if !ok {
			t.Errorf("path %q has no path-level parameters", path)
			continue
		}
		var decoded []any
		if err := json.Unmarshal(params, &decoded); err != nil {
			t.Errorf("path %q parameters = %s: %v", path, params, err)
		}
	}
}

// The tests below pin the Mirakurun-compatible JSON shapes that EPGStation and
// other clients depend on: which keys are omitted when a value is empty, that
// every extended item survives, and that video type/resolution appear only for
// known values. They are the regression record for the internal-model work
// (ISDB-S3 phase 3): later steps must keep these outputs byte-for-byte, except
// for deliberately changed fields such as the stream ID or unset start times.

// contractAPIProgram converts a program through the shared Mirakurun
// conversion so the contract tests pin its output, not a local copy.
func contractAPIProgram(p *program.Program) *apigen.Program {
	api := mirakurun.ProgramToAPI(&p.Event)
	return &api
}

// rawVideo is a video as the STD-B10 values Mirakurun and EIT carry.
type rawVideo struct {
	StreamContent int
	ComponentType int
}

// rawVideoProgram decodes a raw video the way a remote program is decoded.
func rawVideoProgram(v *rawVideo) *program.Program {
	event := mirakurun.EventFromAPI(&apigen.Program{Video: apigen.NewOptProgramVideo(apigen.ProgramVideo{
		StreamContent: apigen.NewOptInt(v.StreamContent),
		ComponentType: apigen.NewOptInt(v.ComponentType),
	})})
	return &program.Program{Event: event}
}

// wantRawVideo is what the API writes back: values outside the STD-B10
// tables are not kept and come out as 0.
func wantRawVideo(v *rawVideo) rawVideo {
	var out rawVideo
	if _, ok := isdb.VideoCodecForTSStreamContent(byte(v.StreamContent)); ok {
		out.StreamContent = v.StreamContent
	}
	if _, ok := isdb.ParseVideoComponentType(byte(v.ComponentType)); ok {
		out.ComponentType = v.ComponentType
	}
	return out
}

// contractFullProgram exercises every optional program field.
func contractFullProgram() *program.Program {
	expiresAt := int64(1788609060000)
	return &program.Program{ID: program.ProgramID(1, 101, 7), Event: model.Event{
		Key: model.ServiceKey{NetworkID: 1, ServiceID: 101}, EventID: 7,
		StartAt: testPtr[int64](1788609060000), DurationMS: testPtr[int](1800000),
		Name: "大河ドラマ", Description: "解説文",
		Genres: []model.Genre{{Lv1: 3, Lv2: 2, Un1: 15, Un2: 15}, {Lv1: 0, Lv2: 1, Un1: 15, Un2: 15}},
		Videos: []model.VideoComponent{{Codec: model.VideoCodecH264, Resolution: model.VideoResolution1080i, Aspect: model.VideoAspect16x9NoPanVector}},
		Audios: []model.AudioComponent{
			{ComponentType: 3, Tag: 16, Main: true, SamplingHz: 48000, Languages: []string{"jpn", "eng"}},
			{ComponentType: 2, Languages: []string{}},
		},
		Extended: []model.ExtendedBlock{{Items: []model.ExtendedItem{
			{Name: "番組内容", Text: "本文"},
			{Name: "出演者", Text: "Foo"},
			{Name: "原作・脚本", Text: "　【作】八津弘幸"},
		}}},
		Related: []model.RelatedEvent{{GroupType: model.EventGroupShared, ServiceID: 101, EventID: 9}},
		Series:  &model.Series{ID: 5, Repeat: 0, Pattern: testPtr(1), ExpiresAt: &expiresAt, Episode: 1, LastEpisode: 12, Name: "series"},
	}}
}

// contractMinimalProgram carries no optional information at all.
func contractMinimalProgram() *program.Program {
	return &program.Program{ID: program.ProgramID(1, 101, 8), Event: model.Event{Key: model.ServiceKey{ServiceID: 101, NetworkID: 1}, EventID: 8, StartAt: testPtr[int64](1788609060000), DurationMS: testPtr[int](1800000), FreeCA: true}}
}

func contractJSONKeys(t *testing.T, raw []byte) map[string]json.RawMessage {
	t.Helper()
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func requireKeys(t *testing.T, decoded map[string]json.RawMessage, wantPresent, wantAbsent []string) {
	t.Helper()
	for _, key := range wantPresent {
		if _, ok := decoded[key]; !ok {
			t.Errorf("key %q absent, want present (keys: %v)", key, reflect.ValueOf(decoded).MapKeys())
		}
	}
	for _, key := range wantAbsent {
		if value, ok := decoded[key]; ok {
			t.Errorf("key %q = %s, want absent", key, value)
		}
	}
}

func TestProgramContractOmitsEmptyKeys(t *testing.T) {
	raw, err := contractAPIProgram(contractMinimalProgram()).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	requireKeys(t, contractJSONKeys(t, raw),
		[]string{"id", "eventId", "serviceId", "networkId", "startAt", "duration", "isFree", "audios", "relatedItems"},
		[]string{"name", "description", "genres", "video", "extended", "series"},
	)
	// audios and relatedItems stay as empty arrays so clients can range over
	// them without a nil check.
	var minimal struct {
		Audios       []any `json:"audios"`
		RelatedItems []any `json:"relatedItems"`
	}
	if err := json.Unmarshal(raw, &minimal); err != nil {
		t.Fatal(err)
	}
	if minimal.Audios == nil || minimal.RelatedItems == nil {
		t.Errorf("audios/relatedItems = %#v/%#v, want non-nil empty arrays", minimal.Audios, minimal.RelatedItems)
	}

	raw, err = contractAPIProgram(contractFullProgram()).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	requireKeys(t, contractJSONKeys(t, raw),
		[]string{"id", "eventId", "serviceId", "networkId", "startAt", "duration", "isFree",
			"name", "description", "genres", "video", "audios", "extended", "relatedItems", "series"},
		nil,
	)
	var full struct {
		Audios []map[string]json.RawMessage `json:"audios"`
		Series map[string]json.RawMessage   `json:"series"`
	}
	if err := json.Unmarshal(raw, &full); err != nil {
		t.Fatal(err)
	}
	if len(full.Audios) != 2 {
		t.Fatalf("audios length = %d, want 2", len(full.Audios))
	}
	requireKeys(t, full.Audios[0],
		[]string{"componentType", "componentTag", "isMain", "samplingRate", "langs"}, nil)
	// langs stays as an empty array even when the audio has no language codes.
	// componentTag and isMain are always written: every EIT audio component
	// carries them. An unknown sampling rate stays absent.
	requireKeys(t, full.Audios[1], []string{"componentType", "componentTag", "isMain", "langs"},
		[]string{"samplingRate"})
	if string(full.Audios[1]["langs"]) != "[]" {
		t.Errorf("langs = %s, want []", full.Audios[1]["langs"])
	}
	requireKeys(t, full.Series,
		[]string{"id", "repeat", "pattern", "expiresAt", "episode", "lastEpisode", "name"}, nil)

	bare := *contractFullProgram()
	bare.Series = &model.Series{ID: 5}
	raw, err = contractAPIProgram(&bare).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var bareSeries struct {
		Series map[string]json.RawMessage `json:"series"`
	}
	if err := json.Unmarshal(raw, &bareSeries); err != nil {
		t.Fatal(err)
	}
	requireKeys(t, bareSeries.Series, []string{"id"},
		[]string{"expiresAt", "name"})
}

func TestProgramContractVideoTypeAndResolution(t *testing.T) {
	for _, tt := range []struct {
		name       string
		video      *rawVideo
		wantType   string
		wantRes    string
		wantAbsent []string
	}{
		{name: "h264 1080i", video: &rawVideo{StreamContent: 0x5, ComponentType: 0xB3}, wantType: "h.264", wantRes: "1080i"},
		{name: "mpeg2 480i", video: &rawVideo{StreamContent: 0x1, ComponentType: 0x01}, wantType: "mpeg2", wantRes: "480i"},
		{name: "h265 2160p", video: &rawVideo{StreamContent: 0x9, ComponentType: 0x91}, wantType: "h.265", wantRes: "2160p"},
		{name: "unknown values become 0", video: &rawVideo{StreamContent: 0xF, ComponentType: 0xF1}, wantAbsent: []string{"type", "resolution"}},
		{name: "known type with unknown resolution", video: &rawVideo{StreamContent: 0x5, ComponentType: 0xF1}, wantType: "h.264", wantAbsent: []string{"resolution"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := contractAPIProgram(rawVideoProgram(tt.video)).MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}
			var decoded struct {
				Video map[string]json.RawMessage `json:"video"`
			}
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded.Video == nil {
				t.Fatal("video key absent, want present")
			}
			var streamContent, componentType int
			if err := json.Unmarshal(decoded.Video["streamContent"], &streamContent); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(decoded.Video["componentType"], &componentType); err != nil {
				t.Fatal(err)
			}
			if want := wantRawVideo(tt.video); streamContent != want.StreamContent || componentType != want.ComponentType {
				t.Errorf("raw video = %d/%d, want %d/%d",
					streamContent, componentType, want.StreamContent, want.ComponentType)
			}
			if tt.wantType != "" {
				var videoType string
				if err := json.Unmarshal(decoded.Video["type"], &videoType); err != nil {
					t.Fatalf("type: %v (video = %s)", err, decoded.Video)
				}
				if videoType != tt.wantType {
					t.Errorf("type = %q, want %q", videoType, tt.wantType)
				}
			}
			if tt.wantRes != "" {
				var resolution string
				if err := json.Unmarshal(decoded.Video["resolution"], &resolution); err != nil {
					t.Fatalf("resolution: %v (video = %s)", err, decoded.Video)
				}
				if resolution != tt.wantRes {
					t.Errorf("resolution = %q, want %q", resolution, tt.wantRes)
				}
			}
			for _, key := range tt.wantAbsent {
				if value, ok := decoded.Video[key]; ok {
					t.Errorf("video key %q = %s, want absent", key, value)
				}
			}
		})
	}
}

// TestProgramContractExtendedKeepsEveryItem pins that extended items survive
// the database round trip and both JSON encodings as a set, and that the
// streaming encoding writes them sorted by key so the output is stable.
func TestProgramContractExtendedKeepsEveryItem(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	pm := program.NewManager(program.NewSQLiteStore(database))
	full := contractFullProgram()
	if err := pm.UpsertPrograms(ctx, []*program.Program{full}); err != nil {
		t.Fatal(err)
	}
	stored, ok, err := pm.Get(ctx, full.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("stored program not found")
	}
	if !reflect.DeepEqual(stored.Extended, full.Extended) {
		t.Fatalf("stored extended = %#v, want %#v", stored.Extended, full.Extended)
	}
	if !reflect.DeepEqual(stored.Genres, full.Genres) {
		t.Fatalf("stored genres = %#v, want %#v", stored.Genres, full.Genres)
	}

	decodeExtended := func(t *testing.T, raw []byte) map[string]string {
		t.Helper()
		var decoded struct {
			Extended map[string]string `json:"extended"`
		}
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		return decoded.Extended
	}
	raw, err := contractAPIProgram(stored).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	want := contractExtendedMap(full)
	if got := decodeExtended(t, raw); !reflect.DeepEqual(got, want) {
		t.Fatalf("encoded extended = %#v, want %#v", got, want)
	}
	// Every encoding keeps the broadcast order of the items: the generated
	// one, and mirakurun.MarshalProgram used by /api/programs and /api/events.
	extendedJSON := `"extended":{` + `"番組内容":"本文","出演者":"Foo","原作・脚本":"　【作】八津弘幸"}`
	streamed := mirakurun.MarshalProgram(contractAPIProgram(stored))
	for _, encoded := range [][]byte{raw, streamed} {
		if !strings.Contains(string(encoded), extendedJSON) {
			t.Errorf("extended not in broadcast order: %s", encoded)
		}
	}
}

// TestProgramEventsShareAPIEncoding pins that /api/events program payloads
// are the shared conversion's bytes, so /api/events and /api/programs cannot
// drift apart on key omission or on video type/resolution.
func TestProgramEventsShareAPIEncoding(t *testing.T) {
	for _, p := range []*program.Program{contractMinimalProgram(), contractFullProgram()} {
		hub := event.New()
		mirakurun.NewEventPublisher(hub).PublishProgramEvent(event.TypeCreate, &p.Event)
		events := hub.Log()
		if len(events) != 1 {
			t.Fatalf("events length = %d, want 1", len(events))
		}
		api := contractAPIProgram(p)
		if want := mirakurun.MarshalProgram(api); string(events[0].Data) != string(want) {
			t.Errorf("eventId=%d event data = %s, want %s", p.EventID, events[0].Data, want)
		}
		// /api/events must stay decodable as apigen.EventData.
		if _, err := apiEventData(events[0].Data); err != nil {
			t.Fatal(err)
		}
	}
	// Removals carry only the program ID.
	hub := event.New()
	mirakurun.NewEventPublisher(hub).PublishProgramRemove(event.TypeRemove, 42)
	if got := string(hub.Log()[0].Data); got != `{"id":42}` {
		t.Errorf("remove data = %s, want %s", got, `{"id":42}`)
	}
}

func contractServiceHandler(t *testing.T, channels config.ChannelsConfig) *Handler {
	t.Helper()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store := service.NewSQLiteStore(database)
	if err := store.ReplaceChannelServices(context.Background(), "GR", "27", []*service.Service{
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, TransportStreamId: 10, Name: "NHK", Type: 1, ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	return NewHandler(HandlerConfig{
		ServiceManager: service.NewManager(store, channels),
	})
}

func TestServiceContractOmitsEmptyKeys(t *testing.T) {
	handler := contractServiceHandler(t, config.ChannelsConfig{{Name: "NHK", Type: "GR", Channel: "27"}})

	bare := &service.Service{Id: "0000100101", ServiceId: 101, NetworkId: 1, TransportStreamId: 10,
		Name: "NHK", Type: 1, ChannelType: "GR", ChannelId: "27"}
	raw, err := apiService(handler, bare, true).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	decoded := contractJSONKeys(t, raw)
	requireKeys(t, decoded,
		[]string{"id", "serviceId", "networkId", "transportStreamId", "name", "type",
			"eitScheduleFlag", "eitPresentFollowing", "hasLogoData", "remoteControlKeyId",
			"epgReady", "channel"},
		[]string{"logoId", "epgUpdatedAt", "epgLastAttemptAt", "epgLastError"})
	var epgReady bool
	if err := json.Unmarshal(decoded["epgReady"], &epgReady); err != nil {
		t.Fatal(err)
	}
	if epgReady {
		t.Error("epgReady = true, want false before any EPG attempt")
	}

	logoID := int64(42)
	attemptAt, successAt := int64(2000), int64(3000)
	full := &service.Service{Id: "0000100101", ServiceId: 101, NetworkId: 1, TransportStreamId: 10,
		Name: "NHK", Type: 1, LogoId: &logoID, HasLogoData: true, RemoteControlKeyId: 3,
		ChannelType: "GR", ChannelId: "27",
		EPG: service.EPGStatus{LastAttemptAt: &attemptAt, LastSuccessAt: &successAt, LastError: "boom"}}
	raw, err = apiService(handler, full, true).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	decoded = contractJSONKeys(t, raw)
	requireKeys(t, decoded,
		[]string{"logoId", "epgReady", "epgUpdatedAt", "epgLastAttemptAt", "epgLastError", "channel"}, nil)

	// A service whose channel is not configured carries no channel key.
	orphan := &service.Service{Id: "0000200102", ServiceId: 102, NetworkId: 2, TransportStreamId: 20,
		Name: "orphan", Type: 1, ChannelType: "BS", ChannelId: "101"}
	raw, err = apiService(handler, orphan, true).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := contractJSONKeys(t, raw)["channel"]; ok {
		t.Errorf("channel = %s, want absent for unconfigured channel", value)
	}
	raw, err = apiService(handler, bare, false).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := contractJSONKeys(t, raw)["channel"]; ok {
		t.Errorf("channel = %s, want absent when not requested", value)
	}
}

// TestServiceEventsShareAPIEncoding pins that /api/events service payloads
// are the shared conversion's bytes, so /api/events and /api/services cannot
// drift apart on the optional logo and EPG keys.
func TestServiceEventsShareAPIEncoding(t *testing.T) {
	handler := contractServiceHandler(t, config.ChannelsConfig{{Name: "NHK", Type: "GR", Channel: "27"}})
	channel := handler.serviceManager.GetChannel("GR", "27")
	logoID := int64(42)
	attemptAt, successAt := int64(2000), int64(3000)
	for _, svc := range []*service.Service{
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, TransportStreamId: 10, Name: "NHK", Type: 1, ChannelType: "GR", ChannelId: "27"},
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, TransportStreamId: 10, Name: "NHK", Type: 1,
			LogoId: &logoID, HasLogoData: true, ChannelType: "GR", ChannelId: "27",
			EPG: service.EPGStatus{LastAttemptAt: &attemptAt, LastSuccessAt: &successAt, LastError: "boom"}},
	} {
		hub := event.New()
		mirakurun.NewEventPublisher(hub).PublishServiceEvent(event.TypeUpdate, svc, channel)
		events := hub.Log()
		if len(events) != 1 {
			t.Fatalf("events length = %d, want 1", len(events))
		}
		want := mirakurun.MarshalService(apiService(handler, svc, true))
		if string(events[0].Data) != string(want) {
			t.Errorf("service %q event data = %s, want %s", svc.Id, events[0].Data, want)
		}
		if _, err := apiEventData(events[0].Data); err != nil {
			t.Fatal(err)
		}
	}
}

// TestMirakurunOutputsShareOneFixture records the Mirakurun-compatible outputs
// (/api/services, /api/programs, /api/events, IPTV playlist and XMLTV) from a
// single edge-case fixture. Later internal-model steps diff against these
// outputs; only deliberate changes (stream ID, unset times) may move them.
func TestMirakurunOutputsShareOneFixture(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	serviceStore := service.NewSQLiteStore(database)
	services := []*service.Service{
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, TransportStreamId: 10,
			Name: "NHK \"総合\"", Type: 1, RemoteControlKeyId: 1, ChannelType: "GR", ChannelId: "27"},
		{Id: "0000200102", ServiceId: 102, NetworkId: 2, TransportStreamId: 20,
			Name: "BS Service", Type: 1, ChannelType: "BS", ChannelId: "101"},
	}
	if err := serviceStore.ReplaceChannelServices(ctx, "GR", "27", services[:1]); err != nil {
		t.Fatal(err)
	}
	if err := serviceStore.ReplaceChannelServices(ctx, "BS", "101", services[1:]); err != nil {
		t.Fatal(err)
	}
	programStore := program.NewSQLiteStore(database)
	pm := program.NewManager(programStore)
	full, minimal := contractFullProgram(), contractMinimalProgram()
	orphan := &program.Program{ID: program.ProgramID(9, 109, 1), Event: model.Event{Key: model.ServiceKey{ServiceID: 109, NetworkID: 9}, EventID: 1, StartAt: testPtr[int64](1788609060000), DurationMS: testPtr[int](1800000)}}
	if err := pm.UpsertPrograms(ctx, []*program.Program{full, minimal, orphan}); err != nil {
		t.Fatal(err)
	}

	hub := event.New()
	mirakurun.NewEventPublisher(hub).PublishProgramEvent(event.TypeCreate, &full.Event)
	mirakurun.NewEventPublisher(hub).PublishServiceEvent(event.TypeUpdate, services[0], nil)
	handler := NewHandler(HandlerConfig{
		ProgramManager: pm,
		ServiceManager: service.NewManager(serviceStore, config.ChannelsConfig{
			{Name: "Terrestrial", Type: "GR", Channel: "27"},
			{Name: "Satellite", Type: "BS", Channel: "101"},
		}),
		EventHub: hub,
	})

	t.Run("services", func(t *testing.T) {
		res, err := handler.GetServices(ctx, apigen.GetServicesParams{})
		if err != nil {
			t.Fatal(err)
		}
		items, ok := res.(*apigen.GetServicesOKApplicationJSON)
		if !ok {
			t.Fatalf("response type = %T", res)
		}
		if len(*items) != 2 {
			t.Fatalf("services length = %d, want 2", len(*items))
		}
		raw, err := (*items)[0].MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		decoded := contractJSONKeys(t, raw)
		var name string
		if err := json.Unmarshal(decoded["name"], &name); err != nil {
			t.Fatal(err)
		}
		if name != `NHK "総合"` {
			t.Errorf("name = %q", name)
		}
		if _, ok := decoded["logoId"]; ok {
			t.Errorf("logoId = %s, want absent", decoded["logoId"])
		}
	})

	t.Run("programs", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/programs", nil)
		rec := httptest.NewRecorder()
		handler.WriteProgramsJSON(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var items []map[string]json.RawMessage
		if err := json.Unmarshal(rec.Body.Bytes(), &items); err != nil {
			t.Fatal(err)
		}
		if len(items) != 3 {
			t.Fatalf("programs length = %d, want 3", len(items))
		}
		byID := map[int64]map[string]json.RawMessage{}
		for _, item := range items {
			var id int64
			if err := json.Unmarshal(item["id"], &id); err != nil {
				t.Fatal(err)
			}
			byID[id] = item
		}
		fullKeys := byID[full.ID]
		requireKeys(t, fullKeys, []string{"name", "genres", "video", "extended", "series"}, nil)
		var extended map[string]string
		if err := json.Unmarshal(fullKeys["extended"], &extended); err != nil {
			t.Fatal(err)
		}
		if want := contractExtendedMap(full); !reflect.DeepEqual(extended, want) {
			t.Errorf("extended = %#v, want %#v", extended, want)
		}
		requireKeys(t, byID[minimal.ID],
			[]string{"id", "audios", "relatedItems"},
			[]string{"name", "description", "genres", "video", "extended", "series"})
	})

	t.Run("events", func(t *testing.T) {
		res, err := handler.GetEvents(ctx)
		if err != nil {
			t.Fatal(err)
		}
		events, ok := res.(*apigen.GetEventsOKApplicationJSON)
		if !ok {
			t.Fatalf("response type = %T", res)
		}
		if len(*events) != 2 {
			t.Fatalf("events length = %d, want 2", len(*events))
		}
		raw, err := json.Marshal((*events)[0].Data)
		if err != nil {
			t.Fatal(err)
		}
		programData := contractJSONKeys(t, raw)
		requireKeys(t, programData, []string{"genres", "video", "extended"}, nil)
		raw, err = json.Marshal((*events)[1].Data)
		if err != nil {
			t.Fatal(err)
		}
		serviceData := contractJSONKeys(t, raw)
		requireKeys(t, serviceData, []string{"id", "name"}, []string{"logoId", "channel"})
	})

	t.Run("playlist", func(t *testing.T) {
		playlistCtx := requestContext(t, "http://localhost:40772/api/iptv/playlist", nil)
		res, err := handler.IptvPlaylistGet(playlistCtx)
		if err != nil {
			t.Fatal(err)
		}
		playlist, ok := res.(*apigen.IptvPlaylistGetOK)
		if !ok {
			t.Fatalf("response type = %T", res)
		}
		raw, err := io.ReadAll(playlist.Data)
		if err != nil {
			t.Fatal(err)
		}
		body := string(raw)
		// RemoteControlKeyId 0 falls back to the guide ID for tvg-chno.
		for _, want := range []string{
			`tvg-id="100101"`,
			`tvg-name="NHK \"総合\""`,
			`tvg-chno="1"`,
			`tvg-chno="200102"`,
			`group-title="Terrestrial"`,
			`group-title="Satellite"`,
			"http://localhost:40772/api/services/100101/stream?decode=1",
		} {
			if !strings.Contains(body, want) {
				t.Errorf("playlist missing %q:\n%s", want, body)
			}
		}
	})

	t.Run("xmltv", func(t *testing.T) {
		res, err := handler.IptvXmltvGet(ctx)
		if err != nil {
			t.Fatal(err)
		}
		xmltvRes, ok := res.(*apigen.IptvXmltvGetOK)
		if !ok {
			t.Fatalf("response type = %T", res)
		}
		raw, err := io.ReadAll(xmltvRes.Data)
		if err != nil {
			t.Fatal(err)
		}
		body := string(raw)
		for _, want := range []string{
			`<display-name>NHK &#34;総合&#34;</display-name>`,
			`<title>大河ドラマ</title>`,
			`<desc>解説文</desc>`,
			`<category>3/2</category>`,
			`<category>0/1</category>`,
			`<title>No Title</title>`,
		} {
			if !strings.Contains(body, want) {
				t.Errorf("xmltv missing %q:\n%s", want, body)
			}
		}
		if strings.Count(body, "<category>") != 2 {
			t.Errorf("category count = %d, want 2 (none for the program without genres)",
				strings.Count(body, "<category>"))
		}
	})
}

// contractExtendedMap lists a program's extended items as the object the API
// writes.
func contractExtendedMap(p *program.Program) map[string]string {
	items := map[string]string{}
	for _, block := range p.Extended {
		for _, item := range block.Items {
			items[item.Name] = item.Text
		}
	}
	return items
}
