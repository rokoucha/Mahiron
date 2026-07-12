package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/event"
	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/stream"
	"github.com/21S1298001/mahiron/internal/tuner"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
	"github.com/ogen-go/ogen/otelogen"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestHTTPContractRoundTripsThroughGeneratedClientAndSQLite(t *testing.T) {
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	disabled := false
	channels := config.ChannelsConfig{{Name: "NHK", Type: "GR", Channel: "27", IsDisabled: &disabled}}
	hub := event.New()
	services := service.NewServiceManager(service.NewSQLiteStore(database), channels, hub)
	programs := program.NewProgramManager(program.NewSQLiteStore(database), hub)
	tuners := tuner.NewTunerManager(&tuner.TunerManagerConfig{})
	jobs, err := job.NewManager(job.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = jobs.Shutdown(context.Background()) })

	handler, err := NewWeb(WebConfig{
		ServiceManager: services,
		ProgramManager: programs,
		StreamManager:  testStreamManager{},
		TunerManager:   tuners,
		JobManager:     jobs,
		LogStore:       observability.NewLogStore(16),
		EventHub:       hub,
		EpgStaleAfter:  7_200_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := apigen.NewClient("http://mahiron.test/api", apigen.WithClient(handlerClient{handler: handler}))
	if err != nil {
		t.Fatal(err)
	}

	version, err := client.CheckVersion(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := version.(*apigen.Version); !ok {
		t.Fatalf("CheckVersion response = %T, want *apigen.Version", version)
	}
	gotChannels, err := client.GetChannels(t.Context(), apigen.GetChannelsParams{})
	if err != nil {
		t.Fatal(err)
	}
	list, ok := gotChannels.(*apigen.GetChannelsOKApplicationJSON)
	if !ok || len(*list) != 1 || (*list)[0].Channel != "27" {
		t.Fatalf("GetChannels response = %#v", gotChannels)
	}
	status, err := client.GetStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := status.(*apigen.Status); !ok {
		t.Fatalf("GetStatus response = %T, want *apigen.Status", status)
	}
}

type handlerClient struct{ handler http.Handler }

func (c handlerClient) Do(request *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	c.handler.ServeHTTP(recorder, request)
	return recorder.Result(), nil
}

func TestNewWebServesWebUIRoutesAndAPI(t *testing.T) {
	handler, err := NewWeb(WebConfig{
		ServiceManager: testServiceManager{},
		StreamManager:  testStreamManager{},
	})
	if err != nil {
		t.Fatalf("NewWeb() = %v", err)
	}

	index := httptest.NewRecorder()
	handler.ServeHTTP(index, httptest.NewRequest(http.MethodGet, "/", nil))
	if index.Code != http.StatusOK {
		t.Fatalf("GET / = %d, want 200", index.Code)
	}
	if contentType := index.Header().Get("Content-Type"); !strings.Contains(contentType, "text/html") {
		t.Fatalf("GET / Content-Type = %q, want text/html", contentType)
	}
	if cache := index.Header().Get("Cache-Control"); cache != "no-cache" {
		t.Fatalf("GET / Cache-Control = %q, want no-cache", cache)
	}

	epg := httptest.NewRecorder()
	handler.ServeHTTP(epg, httptest.NewRequest(http.MethodGet, "/epg", nil))
	if epg.Code != http.StatusOK {
		t.Fatalf("GET /epg = %d, want 200", epg.Code)
	}
	if epg.Body.String() != index.Body.String() {
		t.Fatal("GET /epg did not serve the SPA index")
	}

	match := regexp.MustCompile(`"/assets/([^"]+\.js)"`).FindStringSubmatch(index.Body.String())
	if len(match) == 2 {
		asset := httptest.NewRecorder()
		handler.ServeHTTP(asset, httptest.NewRequest(http.MethodGet, "/assets/"+match[1], nil))
		if asset.Code != http.StatusOK {
			t.Fatalf("GET /assets/%s = %d, want 200", match[1], asset.Code)
		}
		if cache := asset.Header().Get("Cache-Control"); !strings.Contains(cache, "immutable") {
			t.Fatalf("asset Cache-Control = %q, want immutable", cache)
		}
	}

	apiStatus := httptest.NewRecorder()
	handler.ServeHTTP(apiStatus, httptest.NewRequest(http.MethodGet, "/api/status", nil))
	if apiStatus.Code != http.StatusOK {
		t.Fatalf("GET /api/status = %d, want 200", apiStatus.Code)
	}
	if contentType := apiStatus.Header().Get("Content-Type"); !strings.Contains(contentType, "application/json") {
		t.Fatalf("GET /api/status Content-Type = %q, want application/json", contentType)
	}
}

func TestNewWebFiltersStreamHTTPSpans(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	handler, err := NewWeb(WebConfig{
		ServiceManager: testServiceManager{},
		StreamManager:  testStreamManager{},
		TracerProvider: provider,
	})
	if err != nil {
		t.Fatalf("NewWeb() = %v", err)
	}

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/status", nil))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/channels/GR/27/stream", nil))

	var names []string
	for _, span := range recorder.Ended() {
		names = append(names, span.Name())
	}
	if !contains(names, "GetStatus") {
		t.Fatalf("ended spans = %v, want GetStatus", names)
	}
	if contains(names, "GetChannelStream") {
		t.Fatalf("ended spans = %v, want no GetChannelStream", names)
	}
}

func TestNewWebUsesConfiguredMeterProvider(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	handler, err := NewWeb(WebConfig{
		ServiceManager: testServiceManager{},
		StreamManager:  testStreamManager{},
		MeterProvider:  provider,
	})
	if err != nil {
		t.Fatalf("NewWeb() = %v", err)
	}

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/status", nil))

	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	if got := int64MetricSum(data, otelogen.ServerRequestCount); got != 1 {
		t.Fatalf("%s = %d, want 1", otelogen.ServerRequestCount, got)
	}
	if !hasMetric(data, otelogen.ServerDuration) {
		t.Fatalf("collected metrics missing %s: %#v", otelogen.ServerDuration, data.ScopeMetrics)
	}
}

type testStreamManager struct{}

func (testStreamManager) GetOrCreate(context.Context, string, string) (stream.Session, error) {
	return nil, stream.ErrChannelNotFound
}

func (testStreamManager) GetExisting(string, string) (stream.Session, bool) {
	return nil, false
}

func (testStreamManager) ActiveSessionCount() int { return 0 }

type testServiceManager struct{}

func (testServiceManager) EPGSummary(context.Context, int64, int64) (int, int, *int64, error) {
	return 0, 0, nil, nil
}

func (testServiceManager) GetChannel(channelType, channel string) *config.ChannelConfig {
	return &config.ChannelConfig{Type: channelType, Channel: channel}
}

func (testServiceManager) GetChannels() config.ChannelsConfig { return nil }

func (testServiceManager) GetServiceByChannelAndId(context.Context, string, string, string) (*service.Service, error) {
	return nil, nil
}

func (testServiceManager) GetServiceById(context.Context, string) (*service.Service, error) {
	return nil, nil
}

func (testServiceManager) GetServiceByItemID(context.Context, int64) (*service.Service, error) {
	return nil, nil
}

func (testServiceManager) GetLogoByServiceItemID(context.Context, int64) ([]byte, error) {
	return nil, nil
}

func (testServiceManager) GetServices(context.Context) ([]*service.Service, error) {
	return nil, nil
}

func (testServiceManager) GetServicesByChannel(context.Context, string, string) ([]*service.Service, error) {
	return nil, nil
}

func contains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func int64MetricSum(data metricdata.ResourceMetrics, name string) int64 {
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			if item.Name != name {
				continue
			}
			sum, ok := item.Data.(metricdata.Sum[int64])
			if !ok {
				return 0
			}
			var total int64
			for _, point := range sum.DataPoints {
				total += point.Value
			}
			return total
		}
	}
	return 0
}

func hasMetric(data metricdata.ResourceMetrics, name string) bool {
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			if item.Name == name {
				return true
			}
		}
	}
	return false
}
