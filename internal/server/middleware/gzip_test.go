package middleware

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGzipMiddlewareCompressesLargeJSON(t *testing.T) {
	body := strings.Repeat("x", 2048)
	handler := GzipMiddleware().Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/programs", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", got)
	}
	if got := rec.Header().Get("Vary"); got != "Accept-Encoding" {
		t.Fatalf("Vary = %q, want Accept-Encoding", got)
	}
	if got := rec.Header().Get("Content-Length"); got != "" {
		t.Fatalf("Content-Length = %q, want empty", got)
	}

	gz, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	decoded, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("read gzip body: %v", err)
	}
	if string(decoded) != body {
		t.Fatalf("decoded body mismatch: got %d bytes, want %d bytes", len(decoded), len(body))
	}
}

func TestGzipMiddlewarePassesThroughStreamPaths(t *testing.T) {
	handler := GzipMiddleware().Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(strings.Repeat("x", 2048)))
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/events/stream", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding = %q, want empty", got)
	}
	if got := rec.Body.Len(); got != 2048 {
		t.Fatalf("body length = %d, want 2048 (uncompressed)", got)
	}
}

func TestGzipMiddlewarePassesThroughWithoutAcceptEncoding(t *testing.T) {
	handler := GzipMiddleware().Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(strings.Repeat("x", 2048)))
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/programs", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding = %q, want empty", got)
	}
	if got := rec.Body.Len(); got != 2048 {
		t.Fatalf("body length = %d, want 2048 (uncompressed)", got)
	}
}

func TestGzipMiddlewarePassesThroughSmallResponses(t *testing.T) {
	body := "ok"
	handler := GzipMiddleware().Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/services", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding = %q, want empty", got)
	}
	if got := rec.Body.String(); got != body {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

func TestGzipMiddlewarePassesThroughNonJSON(t *testing.T) {
	body := strings.Repeat("x", 2048)
	handler := GzipMiddleware().Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte(body))
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/services/1/logo", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding = %q, want empty", got)
	}
	if got := rec.Body.String(); got != body {
		t.Fatalf("body mismatch")
	}
}

func TestGzipMiddlewareFlusherWorks(t *testing.T) {
	body := strings.Repeat("x", 2048)
	handler := GzipMiddleware().Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("ResponseWriter does not implement http.Flusher")
		}
		flusher.Flush()
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/programs", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if !rec.Flushed {
		t.Fatal("underlying ResponseRecorder was not flushed")
	}
	if got := rec.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", got)
	}

	gz, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	decoded, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("read gzip body: %v", err)
	}
	if string(decoded) != body {
		t.Fatal("decoded body mismatch after Flush")
	}
}

func TestGzipMiddlewareBufferReusePreservesIndependentBodies(t *testing.T) {
	// Guards against a pooled gzip.Writer leaking state across requests.
	handler := GzipMiddleware().Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(r.URL.Query().Get("body")))
	}))

	for _, body := range []string{strings.Repeat("a", 2000), strings.Repeat("b", 3000)} {
		req := httptest.NewRequest(http.MethodGet, "/api/programs?body="+body, nil)
		req.Header.Set("Accept-Encoding", "gzip")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		gz, err := gzip.NewReader(rec.Body)
		if err != nil {
			t.Fatalf("gzip.NewReader: %v", err)
		}
		decoded, err := io.ReadAll(gz)
		if err != nil {
			t.Fatalf("read gzip body: %v", err)
		}
		if string(decoded) != body {
			t.Fatalf("decoded body mismatch")
		}
	}
}
