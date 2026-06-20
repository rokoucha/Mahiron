package middleware

import (
	"bufio"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
)

type accessLogResponseWriter struct {
	http.ResponseWriter
	bytes       int
	status      int
	wroteHeader bool
}

func AccessLogMiddleware() *Middleware {
	return &Middleware{
		Name: "AccessLog",
		Handler: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				startedAt := time.Now()
				recorder := &accessLogResponseWriter{ResponseWriter: w, status: http.StatusOK}
				defer func() {
					recovered := recover()
					if recovered != nil && !recorder.wroteHeader {
						recorder.status = http.StatusInternalServerError
					}
					slog.Info("HTTP request completed",
						"method", r.Method,
						"path", r.URL.Path,
						"query", r.URL.RawQuery,
						"status", recorder.status,
						"bytes", recorder.bytes,
						"duration", time.Since(startedAt),
						"remoteAddr", r.RemoteAddr,
						"userAgent", r.UserAgent(),
					)
					if recovered != nil {
						panic(recovered)
					}
				}()

				next.ServeHTTP(recorder, r)
			})
		},
	}
}

func (w *accessLogResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *accessLogResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.wroteHeader = true
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += n
	return n, err
}

func (w *accessLogResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *accessLogResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("underlying response writer does not support hijacking")
	}
	return hijacker.Hijack()
}

func (w *accessLogResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
