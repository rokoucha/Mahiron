package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/server/middleware"
	"github.com/21S1298001/mahiron/internal/service"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func testIPTVHandler(t *testing.T) *Handler {
	t.Helper()
	ctx := context.Background()
	no := false
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	serviceStore := service.NewSQLiteStore(database)
	services := []*service.Service{
		{
			Id:                 "0000100101",
			ServiceId:          101,
			NetworkId:          1,
			TransportStreamId:  10,
			Name:               "NHK & News",
			Type:               1,
			RemoteControlKeyId: 3,
			ChannelType:        "GR",
			ChannelId:          "27",
		},
		{
			Id:                 "0000200102",
			ServiceId:          102,
			NetworkId:          2,
			TransportStreamId:  20,
			Name:               "BS Service",
			Type:               1,
			RemoteControlKeyId: 4,
			ChannelType:        "BS",
			ChannelId:          "101",
		},
	}
	if err := serviceStore.ReplaceChannelServices(ctx, "GR", "27", []*service.Service{services[0]}); err != nil {
		t.Fatal(err)
	}
	if err := serviceStore.ReplaceChannelServices(ctx, "BS", "101", []*service.Service{services[1]}); err != nil {
		t.Fatal(err)
	}

	programManager := program.NewProgramManager(program.NewSQLiteStore(database))
	if err := programManager.UpsertPrograms(ctx, []*program.Program{
		{
			ID:          program.ProgramID(1, 101, 501),
			EventID:     501,
			ServiceID:   101,
			NetworkID:   1,
			StartAt:     time.Date(2026, 6, 21, 12, 30, 0, 0, time.Local).UnixMilli(),
			Duration:    int((30 * time.Minute).Milliseconds()),
			IsFree:      true,
			Name:        `Morning "News" & Weather`,
			Description: "Headlines <and> forecast",
			Genres:      []program.Genre{{Lv1: 0, Lv2: 1}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	return NewHandler(HandlerConfig{
		ProgramManager: programManager,
		ServiceManager: service.NewServiceManager(serviceStore, config.ChannelsConfig{
			{Name: "Terrestrial", Type: "GR", Channel: "27", IsDisabled: &no},
			{Name: "Satellite", Type: "BS", Channel: "101", IsDisabled: &no},
		}),
	})
}

func requestContext(t *testing.T, target string, headers map[string]string) context.Context {
	t.Helper()
	var ctx context.Context
	handler := middleware.RequestInfoMiddleware().Handler(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		ctx = r.Context()
	}))
	request := httptest.NewRequest(http.MethodGet, target, nil)
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	handler.ServeHTTP(httptest.NewRecorder(), request)
	if ctx == nil {
		t.Fatal("request context was not captured")
	}
	return ctx
}

func TestIPTVDiscoverUsesRequestHost(t *testing.T) {
	handler := testIPTVHandler(t)
	ctx := requestContext(t, "http://internal.example/api/iptv/discover.json", map[string]string{
		"X-Forwarded-Proto": "https",
		"X-Forwarded-Host":  "tv.example.test",
	})

	res, err := handler.IptvDiscoverJSONGet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	discover, ok := res.(*apigen.IptvDiscover)
	if !ok {
		t.Fatalf("response type = %T, want *IptvDiscover", res)
	}
	if got, want := discover.BaseURL, "https://tv.example.test/api"; got != want {
		t.Fatalf("BaseURL = %q, want %q", got, want)
	}
	if got, want := discover.LineupURL, "https://tv.example.test/api/iptv/lineup.json"; got != want {
		t.Fatalf("LineupURL = %q, want %q", got, want)
	}
	if got, want := discover.TunerCount, 2; got != want {
		t.Fatalf("TunerCount = %d, want %d", got, want)
	}
	if discover.DeviceAuth == "" {
		t.Fatal("DeviceAuth should be set")
	}
}

func TestIPTVLineupReturnsServiceStreams(t *testing.T) {
	handler := testIPTVHandler(t)
	ctx := requestContext(t, "http://localhost:40772/api/iptv/lineup.json", nil)

	res, err := handler.IptvLineupJSONGet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lineup, ok := res.(*apigen.IptvLineupJSONGetOKApplicationJSON)
	if !ok {
		t.Fatalf("response type = %T, want *IptvLineupJSONGetOKApplicationJSON", res)
	}
	if got, want := len(*lineup), 2; got != want {
		t.Fatalf("lineup length = %d, want %d", got, want)
	}
	if got, want := (*lineup)[0].GuideNumber, "100101"; got != want {
		t.Fatalf("GuideNumber = %q, want %q", got, want)
	}
	if got, want := (*lineup)[0].GuideName, "NHK & News"; got != want {
		t.Fatalf("GuideName = %q, want %q", got, want)
	}
	if got, want := (*lineup)[0].URL, "http://localhost:40772/api/services/100101/stream?decode=1"; got != want {
		t.Fatalf("URL = %q, want %q", got, want)
	}
}

func TestIPTVLineupStatus(t *testing.T) {
	handler := testIPTVHandler(t)

	res, err := handler.IptvLineupStatusJSONGet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	status, ok := res.(*apigen.IptvLineupStatus)
	if !ok {
		t.Fatalf("response type = %T, want *IptvLineupStatus", res)
	}
	if status.ScanInProgress != 0 || status.ScanPossible != 1 || status.Source != "Cable" {
		t.Fatalf("status = %+v, want idle Cable source", status)
	}
	if len(status.SourceList) != 1 || status.SourceList[0] != "Cable" {
		t.Fatalf("SourceList = %#v, want Cable", status.SourceList)
	}
}

func TestIPTVPlaylist(t *testing.T) {
	handler := testIPTVHandler(t)
	ctx := requestContext(t, "http://localhost:40772/api/iptv/playlist", nil)

	res, err := handler.IptvPlaylistGet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	playlistRes, ok := res.(*apigen.IptvPlaylistGetOK)
	if !ok {
		t.Fatalf("response type = %T, want *IptvPlaylistGetOK", res)
	}
	data, err := io.ReadAll(playlistRes.Data)
	if err != nil {
		t.Fatal(err)
	}
	playlist := string(data)
	for _, want := range []string{
		`#EXTM3U x-tvg-url="http://localhost:40772/api/iptv/xmltv"`,
		`tvg-id="100101"`,
		`tvg-name="NHK & News"`,
		`channel-id="100101"`,
		`group-title="Terrestrial"`,
		`tvg-chno="3"`,
		`http://localhost:40772/api/services/100101/stream?decode=1`,
	} {
		if !strings.Contains(playlist, want) {
			t.Fatalf("playlist missing %q:\n%s", want, playlist)
		}
	}
}

func TestIPTVXMLTV(t *testing.T) {
	handler := testIPTVHandler(t)

	res, err := handler.IptvXmltvGet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	xmltvRes, ok := res.(*apigen.IptvXmltvGetOK)
	if !ok {
		t.Fatalf("response type = %T, want *IptvXmltvGetOK", res)
	}
	data, err := io.ReadAll(xmltvRes.Data)
	if err != nil {
		t.Fatal(err)
	}
	xmltv := string(data)
	start := time.Date(2026, 6, 21, 12, 30, 0, 0, time.Local).Format("20060102150405 -0700")
	stop := time.Date(2026, 6, 21, 13, 0, 0, 0, time.Local).Format("20060102150405 -0700")
	for _, want := range []string{
		`<tv source-info-name="mahiron">`,
		`<channel id="100101">`,
		`<display-name>NHK &amp; News</display-name>`,
		`<programme start="` + start + `" stop="` + stop + `" channel="100101">`,
		`<title>Morning &#34;News&#34; &amp; Weather</title>`,
		`<desc>Headlines &lt;and&gt; forecast</desc>`,
		`<category>0/1</category>`,
	} {
		if !strings.Contains(xmltv, want) {
			t.Fatalf("xmltv missing %q:\n%s", want, xmltv)
		}
	}
}
