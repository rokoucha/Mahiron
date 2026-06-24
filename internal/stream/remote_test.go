package stream

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
)

func TestRemoteClientCheckAvailableAndBasicAuth(t *testing.T) {
	var auth string
	var hasDeadline bool
	client := NewRemoteClient(config.RemoteConfig{
		URL:       "http://remote.local/api",
		BasicAuth: &config.BasicAuthConfig{Username: "user", Password: "pass"},
	})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		auth = r.Header.Get("Authorization")
		_, hasDeadline = r.Context().Deadline()
		if r.URL.Path != "/api/tuners" {
			t.Fatalf("path = %s, want /api/tuners", r.URL.Path)
		}
		return stringResponse(http.StatusOK, `[{"types":["GR"],"isAvailable":true,"isFree":true,"isFault":false}]`), nil
	})}
	if err := client.CheckAvailable(context.Background(), "GR"); err != nil {
		t.Fatal(err)
	}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass"))
	if auth != wantAuth {
		t.Fatalf("Authorization = %q, want %q", auth, wantAuth)
	}
	if !hasDeadline {
		t.Fatal("remote availability request context has no deadline")
	}
}

func TestRemoteClientNoAuthAndUnavailable(t *testing.T) {
	var auth string
	client := NewRemoteClient(config.RemoteConfig{URL: "http://remote.local"})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		auth = r.Header.Get("Authorization")
		return stringResponse(http.StatusOK, `[{"types":["GR"],"isAvailable":true,"isFree":false,"isFault":false}]`), nil
	})}
	if err := client.CheckAvailable(context.Background(), "GR"); err != ErrTunerUnavailable {
		t.Fatalf("CheckAvailable error = %v, want ErrTunerUnavailable", err)
	}
	if auth != "" {
		t.Fatalf("Authorization = %q, want empty", auth)
	}
}

func TestRemoteClientCheckAvailableForActiveSameRoute(t *testing.T) {
	client := NewRemoteClient(config.RemoteConfig{URL: "http://remote.local"})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return stringResponse(http.StatusOK, `[{
			"types":["GR"],
			"isAvailable":true,
			"isFree":false,
			"isFault":false,
			"tunedChannelType":"GR",
			"tunedChannel":"27"
		}]`), nil
	})}
	if err := client.CheckAvailableForRoute(context.Background(), "GR", "27"); err != nil {
		t.Fatalf("CheckAvailableForRoute error = %v, want nil", err)
	}
}

func TestRemoteClientCheckAvailableForActiveCurrentRoute(t *testing.T) {
	client := NewRemoteClient(config.RemoteConfig{URL: "http://remote.local"})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return stringResponse(http.StatusOK, `[{
			"types":["CATV"],
			"isAvailable":true,
			"isFree":false,
			"isFault":false,
			"currentChannelType":"CATV",
			"currentChannel":"C27"
		}]`), nil
	})}
	if err := client.CheckAvailableForRoute(context.Background(), "CATV", "C27"); err != nil {
		t.Fatalf("CheckAvailableForRoute error = %v, want nil", err)
	}
}

func TestRemoteClientCheckAvailableForBusyDifferentOrUnknownRoute(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "different route",
			body: `[{
				"types":["GR"],
				"isAvailable":true,
				"isFree":false,
				"isFault":false,
				"tunedChannelType":"GR",
				"tunedChannel":"28"
			}]`,
		},
		{
			name: "unknown route",
			body: `[{
				"types":["GR"],
				"isAvailable":true,
				"isFree":false,
				"isFault":false
			}]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewRemoteClient(config.RemoteConfig{URL: "http://remote.local"})
			client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return stringResponse(http.StatusOK, tt.body), nil
			})}
			if err := client.CheckAvailableForRoute(context.Background(), "GR", "27"); err != ErrTunerUnavailable {
				t.Fatalf("CheckAvailableForRoute error = %v, want ErrTunerUnavailable", err)
			}
		})
	}
}

func TestRemoteSessionStreamsChannelAndService(t *testing.T) {
	paths := []string{}
	queries := []string{}
	client := NewRemoteClient(config.RemoteConfig{URL: "http://remote.local/api"})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		paths = append(paths, r.URL.Path)
		queries = append(queries, r.URL.RawQuery)
		switch r.URL.Path {
		case "/api/channels/GR/27/stream":
			return stringResponse(http.StatusOK, "channel-ts"), nil
		case "/api/channels/GR/27/services/1024/stream":
			return stringResponse(http.StatusOK, "service-ts"), nil
		default:
			return stringResponse(http.StatusNotFound, ""), nil
		}
	})}

	session := NewRemoteSession(RemoteSessionConfig{
		Client:       client,
		RouteChannel: &config.ChannelConfig{Type: "GR", Channel: "27"},
	})

	var channelOut bytes.Buffer
	if err := session.ChannelStream(context.Background(), false, &channelOut); err != nil {
		t.Fatal(err)
	}
	var serviceOut bytes.Buffer
	if err := session.ServiceStream(context.Background(), 1024, true, &serviceOut); err != nil {
		t.Fatal(err)
	}
	if channelOut.String() != "channel-ts" || serviceOut.String() != "service-ts" {
		t.Fatalf("streams = %q/%q", channelOut.String(), serviceOut.String())
	}
	if len(paths) != 2 || paths[0] != "/api/channels/GR/27/stream" || paths[1] != "/api/channels/GR/27/services/1024/stream" {
		t.Fatalf("paths = %#v", paths)
	}
	if len(queries) != 2 || queries[0] != "" || queries[1] != "decode=1" {
		t.Fatalf("queries = %#v", queries)
	}
}

func TestRemoteSessionStreamsProgram(t *testing.T) {
	var path string
	var query string
	client := NewRemoteClient(config.RemoteConfig{URL: "http://remote.local/api"})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		path = r.URL.Path
		query = r.URL.RawQuery
		if r.URL.Path != "/api/programs/10100009/stream" {
			return stringResponse(http.StatusNotFound, ""), nil
		}
		return stringResponse(http.StatusOK, "program-ts"), nil
	})}
	session := NewRemoteSession(RemoteSessionConfig{Client: client})

	var out bytes.Buffer
	if err := session.ProgramStream(context.Background(), &program.Program{ID: 10100009}, true, &out); err != nil {
		t.Fatal(err)
	}
	if path != "/api/programs/10100009/stream" || query != "decode=1" {
		t.Fatalf("request = %s?%s", path, query)
	}
	if out.String() != "program-ts" {
		t.Fatalf("program stream = %q, want program-ts", out.String())
	}
}

func TestRemoteProgramStreamMapsStatusErrors(t *testing.T) {
	client := NewRemoteClient(config.RemoteConfig{URL: "http://remote.local/api"})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return stringResponse(http.StatusNotFound, ""), nil
	})}
	if err := client.ProgramStream(context.Background(), 1, false, io.Discard); err != ErrChannelNotFound {
		t.Fatalf("ProgramStream 404 error = %v, want ErrChannelNotFound", err)
	}

	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return stringResponse(http.StatusServiceUnavailable, ""), nil
	})}
	if err := client.ProgramStream(context.Background(), 1, false, io.Discard); err != ErrTunerUnavailable {
		t.Fatalf("ProgramStream 503 error = %v, want ErrTunerUnavailable", err)
	}

	for _, status := range []int{http.StatusConflict, http.StatusLocked} {
		client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return stringResponse(status, ""), nil
		})}
		if err := client.ProgramStream(context.Background(), 1, false, io.Discard); err != ErrTunerUnavailable {
			t.Fatalf("ProgramStream %d error = %v, want ErrTunerUnavailable", status, err)
		}
	}
}

func TestRemoteStreamForwardsPriorityHeader(t *testing.T) {
	var priority string
	client := NewRemoteClient(config.RemoteConfig{URL: "http://remote.local/api"})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		priority = r.Header.Get("X-Mirakurun-Priority")
		return stringResponse(http.StatusOK, "ts"), nil
	})}

	ctx := tuner.WithUser(context.Background(), tuner.User{ID: "viewer", Priority: 7})
	if err := client.ChannelStream(ctx, "GR", "27", false, io.Discard); err != nil {
		t.Fatal(err)
	}
	if priority != "7" {
		t.Fatalf("X-Mirakurun-Priority = %q, want 7", priority)
	}
}

func TestRemoteSessionScanServicesUsesRemoteAPI(t *testing.T) {
	var auth string
	var path string
	client := NewRemoteClient(config.RemoteConfig{
		URL:       "http://remote.local/api",
		BasicAuth: &config.BasicAuthConfig{Username: "user", Password: "pass"},
	})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		auth = r.Header.Get("Authorization")
		path = r.URL.Path
		if r.URL.Path != "/api/channels/GR/27/services" {
			t.Fatalf("path = %s, want /api/channels/GR/27/services", r.URL.Path)
		}
		return stringResponse(http.StatusOK, `[{
			"id": 327361024,
			"serviceId": 1024,
			"networkId": 32736,
			"transportStreamId": 32736,
			"name": "remote service",
			"type": 1,
			"logoId": 12,
			"hasLogoData": true,
			"remoteControlKeyId": 5
		}]`), nil
	})}
	session := NewRemoteSession(RemoteSessionConfig{
		Client:       client,
		RouteChannel: &config.ChannelConfig{Type: "GR", Channel: "27"},
	})

	got, err := session.ScanServices(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if path != "/api/channels/GR/27/services" {
		t.Fatalf("path = %s", path)
	}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass"))
	if auth != wantAuth {
		t.Fatalf("Authorization = %q, want %q", auth, wantAuth)
	}
	if len(got) != 1 || got[0].Nid != 32736 || got[0].Sid != 1024 || got[0].Tsid != 32736 || got[0].Name != "remote service" || got[0].RemoteControlKeyId == nil || *got[0].RemoteControlKeyId != 5 {
		t.Fatalf("services = %#v", got)
	}
	if got[0].LogoId != 12 || got[0].LogoVersion == nil || *got[0].LogoVersion != 0 || got[0].LogoDownloadDataId == nil || *got[0].LogoDownloadDataId != 1024 {
		t.Fatalf("logo metadata = %#v", got[0])
	}
}

func TestRemoteClientScanServicesReturnsStatusError(t *testing.T) {
	client := NewRemoteClient(config.RemoteConfig{URL: "http://remote.local/api"})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return stringResponse(http.StatusServiceUnavailable, ""), nil
	})}
	if _, err := client.ScanServices(context.Background(), "GR", "27"); err == nil {
		t.Fatal("ScanServices error = nil, want status error")
	}
}

func TestRemoteSessionObserveLogosUsesRemoteAPI(t *testing.T) {
	var paths []string
	client := NewRemoteClient(config.RemoteConfig{URL: "http://remote.local/api"})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		paths = append(paths, r.URL.Path)
		switch r.URL.Path {
		case "/api/channels/GR/27/services":
			return stringResponse(http.StatusOK, `[{
				"serviceId": 101,
				"networkId": 4,
				"transportStreamId": 4,
				"name": "remote service",
				"type": 1,
				"logoId": 12,
				"hasLogoData": true
			}, {
				"serviceId": 102,
				"networkId": 4,
				"transportStreamId": 4,
				"name": "remote service without logo data",
				"type": 1,
				"logoId": 13,
				"hasLogoData": false
			}]`), nil
		case "/api/services/400101/logo":
			return stringResponse(http.StatusOK, "png"), nil
		default:
			return stringResponse(http.StatusNotFound, ""), nil
		}
	})}
	session := NewRemoteSession(RemoteSessionConfig{
		Client:       client,
		RouteChannel: &config.ChannelConfig{Type: "GR", Channel: "27"},
	})

	var observed int
	err := session.ObserveLogos(context.Background(), func(image *ts.LogoImage) error {
		observed++
		if image.OriginalNetworkID != 4 || image.LogoID != 12 || image.LogoVersion != 0 || image.DownloadDataID != 101 || string(image.Data) != "png" {
			t.Fatalf("image = %#v", image)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed != 1 {
		t.Fatalf("observed logos = %d, want 1", observed)
	}
	if len(paths) != 2 || paths[0] != "/api/channels/GR/27/services" || paths[1] != "/api/services/400101/logo" {
		t.Fatalf("paths = %#v", paths)
	}
}

func TestRemoteClientListServicePrograms(t *testing.T) {
	var path string
	var query string
	client := NewRemoteClient(config.RemoteConfig{URL: "http://remote.local/api"})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		path = r.URL.Path
		query = r.URL.RawQuery
		if r.URL.Path != "/api/programs" {
			t.Fatalf("path = %s, want /api/programs", r.URL.Path)
		}
		return stringResponse(http.StatusOK, `[{
			"id": 101001,
			"eventId": 1,
			"serviceId": 101,
			"networkId": 4,
			"startAt": 1000,
			"duration": 1800000,
			"isFree": true,
			"name": "news",
			"description": "desc",
			"genres": [{"lv1": 0, "lv2": 1, "un1": 15, "un2": 15}],
			"video": {"streamContent": 1, "componentType": 179},
			"audios": [{"componentType": 2, "componentTag": 16, "isMain": true, "samplingRate": 48000, "langs": ["jpn"]}],
			"extended": {"key": "value"},
			"relatedItems": [{"type": "shared", "networkId": 4, "serviceId": 102, "eventId": 2}],
			"series": {"id": 7, "repeat": 1, "pattern": 2, "expiresAt": 3000, "episode": 3, "lastEpisode": 12, "name": "series"}
		}]`), nil
	})}

	programs, err := client.ListServicePrograms(context.Background(), 4, 101)
	if err != nil {
		t.Fatal(err)
	}
	if path != "/api/programs" || query != "networkId=4&serviceId=101" {
		t.Fatalf("request = %s?%s", path, query)
	}
	if len(programs) != 1 {
		t.Fatalf("len(programs) = %d", len(programs))
	}
	p := programs[0]
	if p.ID != 101001 || p.EventID != 1 || p.ServiceID != 101 || p.NetworkID != 4 || p.Name != "news" || !p.IsFree {
		t.Fatalf("program = %#v", p)
	}
	if len(p.Genres) != 1 || p.Genres[0].Lv1 != 0 || p.Genres[0].Lv2 != 1 || p.Genres[0].Un1 != 15 {
		t.Fatalf("genres = %#v", p.Genres)
	}
	if p.Video == nil || p.Video.StreamContent != 1 || p.Video.ComponentType != 179 {
		t.Fatalf("video = %#v", p.Video)
	}
	if len(p.Audios) != 1 || p.Audios[0].SamplingRate == nil || *p.Audios[0].SamplingRate != 48000 || len(p.Audios[0].Langs) != 1 || p.Audios[0].Langs[0] != "jpn" {
		t.Fatalf("audios = %#v", p.Audios)
	}
	if p.Extended["key"] != "value" {
		t.Fatalf("extended = %#v", p.Extended)
	}
	if len(p.RelatedItems) != 1 || p.RelatedItems[0].Type != "shared" || p.RelatedItems[0].NetworkID == nil || *p.RelatedItems[0].NetworkID != 4 {
		t.Fatalf("related = %#v", p.RelatedItems)
	}
	if p.Series == nil || p.Series.ID != 7 || p.Series.Pattern != 2 || p.Series.ExpiresAt == nil || *p.Series.ExpiresAt != 3000 {
		t.Fatalf("series = %#v", p.Series)
	}
}

func TestRemoteClientStreamProgramEventsUsesRemoteAPI(t *testing.T) {
	var auth string
	var path string
	var query string
	client := NewRemoteClient(config.RemoteConfig{
		URL:       "http://remote.local/api",
		BasicAuth: &config.BasicAuthConfig{Username: "user", Password: "pass"},
	})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		auth = r.Header.Get("Authorization")
		path = r.URL.Path
		query = r.URL.RawQuery
		return stringResponse(http.StatusOK, "[\n"), nil
	})}

	if err := client.StreamProgramEvents(context.Background(), &recordingProgramUpdater{}); err != nil {
		t.Fatal(err)
	}
	if path != "/api/events/stream" || query != "resource=program" {
		t.Fatalf("request = %s?%s", path, query)
	}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass"))
	if auth != wantAuth {
		t.Fatalf("Authorization = %q, want %q", auth, wantAuth)
	}
}

func TestReadRemoteProgramEventsUpsertsProgramUpdates(t *testing.T) {
	src := strings.NewReader(`[
{"resource":"program","type":"update","data":{"id":401010001,"eventId":1,"serviceId":101,"networkId":4,"startAt":1000,"duration":1800000,"isFree":true,"name":"updated"}}
,
{"resource":"program","type":"create","data":{"id":401010002,"eventId":2,"serviceId":101,"networkId":4,"startAt":2000,"duration":1800000,"isFree":false,"name":"next"}},
`)
	updater := &recordingProgramUpdater{}

	if err := readRemoteProgramEvents(context.Background(), src, updater); err != nil {
		t.Fatal(err)
	}
	if got, want := len(updater.programs), 2; got != want {
		t.Fatalf("upserted programs = %d, want %d", got, want)
	}
	if updater.programs[0].ID != 401010001 || updater.programs[0].Name != "updated" || updater.programs[1].EventID != 2 {
		t.Fatalf("programs = %#v", updater.programs)
	}
}

func TestReadRemoteProgramEventsIgnoresMalformedAndFilteredEvents(t *testing.T) {
	src := strings.NewReader(`[
not-json
{"resource":"service","type":"update","data":{"id":1}}
{"resource":"program","type":"remove","data":{"id":401010001,"eventId":1,"serviceId":101,"networkId":4}}
{"resource":"program","type":"update","data":{"id":401010002,"eventId":2,"serviceId":101,"networkId":4,"name":"kept"}}
{"resource":"program","type":"update","data":}
`)
	updater := &recordingProgramUpdater{}

	if err := readRemoteProgramEvents(context.Background(), src, updater); err != nil {
		t.Fatal(err)
	}
	if got, want := len(updater.programs), 1; got != want {
		t.Fatalf("upserted programs = %d, want %d", got, want)
	}
	if updater.programs[0].ID != 401010002 || updater.programs[0].Name != "kept" {
		t.Fatalf("program = %#v", updater.programs[0])
	}
}

func TestReadRemoteProgramEventsStopsCleanlyOnCanceledContextAndEOF(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	updater := &recordingProgramUpdater{}

	err := readRemoteProgramEvents(ctx, strings.NewReader(`{"resource":"program","type":"update","data":{"id":1}}`), updater)
	if err != nil {
		t.Fatalf("canceled read error = %v, want nil", err)
	}
	if len(updater.programs) != 0 {
		t.Fatalf("upserted after cancellation = %#v", updater.programs)
	}

	if err := readRemoteProgramEvents(context.Background(), strings.NewReader("[\n"), updater); err != nil {
		t.Fatalf("EOF read error = %v, want nil", err)
	}
}

func TestKnownServiceProgramUpdaterFiltersUnknownServices(t *testing.T) {
	inner := &recordingProgramUpdater{}
	lister := &recordingServiceLister{
		services: []*service.Service{{NetworkId: 4, ServiceId: 101}},
	}
	updater := newKnownServiceProgramUpdater(inner, lister)

	err := updater.UpsertPrograms(context.Background(), []*program.Program{
		{ID: 401010001, NetworkID: 4, ServiceID: 101, EventID: 1},
		{ID: 401020001, NetworkID: 4, ServiceID: 102, EventID: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(inner.programs), 1; got != want {
		t.Fatalf("upserted programs = %d, want %d", got, want)
	}
	if inner.programs[0].ServiceID != 101 {
		t.Fatalf("program = %#v, want known service", inner.programs[0])
	}
	if got, want := lister.calls, 2; got != want {
		t.Fatalf("service list calls = %d, want %d (initial load and unknown refresh)", got, want)
	}
}

func TestKnownServiceProgramUpdaterRefreshesUnknownOnce(t *testing.T) {
	inner := &recordingProgramUpdater{}
	lister := &recordingServiceLister{
		services: []*service.Service{{NetworkId: 4, ServiceId: 101}},
		refreshServices: []*service.Service{
			{NetworkId: 4, ServiceId: 101},
			{NetworkId: 4, ServiceId: 102},
		},
	}
	updater := newKnownServiceProgramUpdater(inner, lister)

	err := updater.UpsertPrograms(context.Background(), []*program.Program{
		{ID: 401020001, NetworkID: 4, ServiceID: 102, EventID: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(inner.programs), 1; got != want {
		t.Fatalf("upserted programs = %d, want %d", got, want)
	}
	if inner.programs[0].ServiceID != 102 {
		t.Fatalf("program = %#v, want refreshed service", inner.programs[0])
	}
	if got, want := lister.calls, 2; got != want {
		t.Fatalf("service list calls = %d, want %d (initial load and unknown refresh)", got, want)
	}
}

type recordingProgramUpdater struct {
	programs []*program.Program
}

func (u *recordingProgramUpdater) UpsertPrograms(_ context.Context, programs []*program.Program) error {
	u.programs = append(u.programs, programs...)
	return nil
}

type recordingServiceLister struct {
	calls           int
	services        []*service.Service
	refreshServices []*service.Service
}

func (l *recordingServiceLister) GetServices(context.Context) ([]*service.Service, error) {
	l.calls++
	if l.calls > 1 && l.refreshServices != nil {
		return l.refreshServices, nil
	}
	return l.services, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func stringResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}
