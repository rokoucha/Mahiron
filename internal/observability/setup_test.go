package observability

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
)

func TestOtlpTarget(t *testing.T) {
	tests := []struct {
		name         string
		endpoint     string
		signalPath   string
		wantHost     string
		wantPath     string
		wantInsecure bool
	}{
		{
			name:         "http without path",
			endpoint:     "http://localhost:4318",
			signalPath:   "v1/logs",
			wantHost:     "localhost:4318",
			wantPath:     "/v1/logs",
			wantInsecure: true,
		},
		{
			name:         "https without path",
			endpoint:     "https://otel.example.net",
			signalPath:   "v1/traces",
			wantHost:     "otel.example.net",
			wantPath:     "/v1/traces",
			wantInsecure: false,
		},
		{
			name:         "trailing slash",
			endpoint:     "https://otel.example.net/",
			signalPath:   "v1/metrics",
			wantHost:     "otel.example.net",
			wantPath:     "/v1/metrics",
			wantInsecure: false,
		},
		{
			name:         "base path prefix",
			endpoint:     "https://example.net/otel",
			signalPath:   "v1/logs",
			wantHost:     "example.net",
			wantPath:     "/otel/v1/logs",
			wantInsecure: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, urlPath, insecure, err := otlpTarget(tt.endpoint, tt.signalPath)
			if err != nil {
				t.Fatal(err)
			}
			if host != tt.wantHost {
				t.Errorf("host = %q, want %q", host, tt.wantHost)
			}
			if urlPath != tt.wantPath {
				t.Errorf("urlPath = %q, want %q", urlPath, tt.wantPath)
			}
			if insecure != tt.wantInsecure {
				t.Errorf("insecure = %v, want %v", insecure, tt.wantInsecure)
			}
		})
	}
}

// Regression test: otlploghttp.WithEndpointURL keeps an empty URL path
// instead of falling back to /v1/logs, so exports were rejected with 404
// when the configured endpoint had no path.
func TestNewOTelLogHandlerPostsToSignalPath(t *testing.T) {
	paths := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case paths <- r.URL.Path:
		default:
		}
	}))
	defer srv.Close()

	ctx := context.Background()
	handler, shutdown, err := newOTelLogHandler(ctx, config.ObservabilityConfig{
		ServiceName: "test",
		Endpoint:    srv.URL,
	})
	if err != nil {
		t.Fatal(err)
	}

	slog.New(handler).InfoContext(ctx, "hello")
	if err := shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-paths:
		if got != "/v1/logs" {
			t.Errorf("request path = %q, want %q", got, "/v1/logs")
		}
	default:
		t.Fatal("no export request received")
	}
}
