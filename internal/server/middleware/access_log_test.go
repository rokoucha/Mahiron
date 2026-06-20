package middleware

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAccessLogMiddlewareLogsRequestAndResponse(t *testing.T) {
	logs := captureAccessLogs(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("missing"))
	})

	for _, want := range []string{
		"HTTP request completed",
		"method=GET",
		"path=/channels/GR/27",
		"query=\"decode=1\"",
		"status=404",
		"bytes=7",
		"remoteAddr=",
		"userAgent=mahiron-test",
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs = %q, want %q", logs, want)
		}
	}
}

func TestAccessLogMiddlewareDefaultsStatusToOK(t *testing.T) {
	logs := captureAccessLogs(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	if !strings.Contains(logs, "status=200") {
		t.Fatalf("logs = %q, want status=200", logs)
	}
	if !strings.Contains(logs, "bytes=2") {
		t.Fatalf("logs = %q, want bytes=2", logs)
	}
}

func TestAccessLogMiddlewareLogsPanicBeforeRethrow(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(previous)

	handler := AccessLogMiddleware().Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))
	req := httptest.NewRequest(http.MethodGet, "/panic", nil)

	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("panic was not rethrown")
			}
		}()
		handler.ServeHTTP(httptest.NewRecorder(), req)
	}()

	if !strings.Contains(buf.String(), "path=/panic") {
		t.Fatalf("logs = %q, want panic request log", buf.String())
	}
	if !strings.Contains(buf.String(), "status=500") {
		t.Fatalf("logs = %q, want status=500", buf.String())
	}
}

func captureAccessLogs(t *testing.T, fn http.HandlerFunc) string {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(previous)

	handler := AccessLogMiddleware().Handler(fn)
	req := httptest.NewRequest(http.MethodGet, "/channels/GR/27?decode=1", nil)
	req.RemoteAddr = "192.0.2.1:12345"
	req.Header.Set("User-Agent", "mahiron-test")
	handler.ServeHTTP(httptest.NewRecorder(), req)
	return buf.String()
}
