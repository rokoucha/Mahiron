package web

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/internal/service"
	"github.com/21S1298001/Mahiron5/internal/stream"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

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

type testStreamManager struct{}

func (testStreamManager) GetOrCreate(context.Context, string, string) (interface {
	ChannelStream(context.Context, bool, io.Writer) error
	ProgramStream(context.Context, *program.Program, bool, io.Writer) error
	ServiceStream(context.Context, uint16, bool, io.Writer) error
}, error) {
	return nil, stream.ErrChannelNotFound
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
