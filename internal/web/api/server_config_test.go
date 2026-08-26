package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func TestGetServerConfig(t *testing.T) {
	handler := NewHandler(HandlerConfig{})
	server, err := apigen.NewServer(handler, handler)
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/config/server", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != "{}" {
		t.Fatalf("body = %q, want {}", got)
	}
}
