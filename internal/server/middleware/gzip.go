package middleware

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
)

// gzipMinSize is the smallest response body GzipMiddleware will compress.
// Below it, gzip's per-response overhead (header, checksum, flate framing)
// tends to cost more than it saves.
const gzipMinSize = 1024

var gzipWriterPool = sync.Pool{
	New: func() any { return gzip.NewWriter(nil) },
}

// GzipMiddleware compresses JSON API responses with gzip. It never touches
// streaming endpoints (paths ending in "/stream", and anything under
// /data-broadcast/, which serve MPEG-TS or Server-Sent Events): those are
// skipped before wrapping the response writer, so their Flusher/Hijacker
// support reaches the handler untouched. Everything else is wrapped and only
// compressed once the response is confirmed to be application/json and at
// least gzipMinSize bytes; anything smaller or of another content type is
// written through unmodified.
func GzipMiddleware() *Middleware {
	return &Middleware{
		Name: "Gzip",
		Handler: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !acceptsGzip(r) || excludedFromGzip(r.URL.Path) {
					next.ServeHTTP(w, r)
					return
				}
				gw := &gzipResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
				defer gw.Close()
				next.ServeHTTP(gw, r)
			})
		},
	}
}

func acceptsGzip(r *http.Request) bool {
	for _, enc := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		if strings.EqualFold(strings.TrimSpace(enc), "gzip") {
			return true
		}
	}
	return false
}

func excludedFromGzip(path string) bool {
	return strings.HasSuffix(path, "/stream") || strings.Contains(path, "/data-broadcast/")
}

type gzipResponseWriter struct {
	http.ResponseWriter

	statusCode  int
	wroteHeader bool

	eligibilityChecked bool
	eligible           bool

	decided  bool
	compress bool
	buf      bytes.Buffer
	gz       *gzip.Writer

	closed bool
}

func (w *gzipResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.statusCode = status
}

func (w *gzipResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}

	if !w.decided && !w.eligibilityChecked {
		w.eligibilityChecked = true
		w.eligible = strings.HasPrefix(w.Header().Get("Content-Type"), "application/json")
		if !w.eligible {
			w.finalize(false)
		}
	}

	if !w.decided {
		w.buf.Write(p)
		if w.buf.Len() >= gzipMinSize {
			w.finalize(true)
		}
		return len(p), nil
	}

	if w.compress {
		return w.gz.Write(p)
	}
	return w.ResponseWriter.Write(p)
}

// finalize locks in the compress/pass-through decision, writes the status
// line and headers, and flushes anything buffered so far.
func (w *gzipResponseWriter) finalize(compress bool) {
	w.decided = true
	w.compress = compress
	if compress {
		w.Header().Del("Content-Length")
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Add("Vary", "Accept-Encoding")
	}
	w.ResponseWriter.WriteHeader(w.statusCode)
	if w.buf.Len() == 0 {
		return
	}
	buffered := w.buf.Bytes()
	if compress {
		w.gz = gzipWriterPool.Get().(*gzip.Writer)
		w.gz.Reset(w.ResponseWriter)
		_, _ = w.gz.Write(buffered)
	} else {
		_, _ = w.ResponseWriter.Write(buffered)
	}
	w.buf.Reset()
}

func (w *gzipResponseWriter) Flush() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if !w.decided {
		w.finalize(w.buf.Len() >= gzipMinSize)
	}
	if w.compress && w.gz != nil {
		_ = w.gz.Flush()
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *gzipResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("underlying response writer does not support hijacking")
	}
	return hijacker.Hijack()
}

func (w *gzipResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// Close finalizes a response that never hit the compression threshold
// (including empty bodies) and releases the pooled gzip.Writer, if any.
func (w *gzipResponseWriter) Close() {
	if w.closed {
		return
	}
	w.closed = true
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if !w.decided {
		w.finalize(false)
	}
	if w.gz != nil {
		_ = w.gz.Close()
		gzipWriterPool.Put(w.gz)
		w.gz = nil
	}
}
