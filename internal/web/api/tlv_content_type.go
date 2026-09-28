package api

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
)

// ChannelLookup resolves the channel of stream requests.
type ChannelLookup interface {
	GetChannel(string, string) *config.ChannelConfig
	GetServiceById(context.Context, string) (*service.Service, error)
}

// ProgramLookup resolves the program of a program stream request.
type ProgramLookup interface {
	Get(context.Context, int64) (*program.Program, bool, error)
}

// TLVStreamContentType rewrites the generated server's video/mp2t to
// application/octet-stream for TLV (ISDB-S3) channels, their services and
// their programs, since a TLV payload is not MPEG-2 TS. Other responses
// (JSON errors, HEAD) pass through untouched.
func TLVStreamContentType(next http.Handler, channels ChannelLookup, programs ProgramLookup) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		channelType, channelID, ok := streamChannel(r.Context(), r.URL.Path, channels, programs)
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		channel := channels.GetChannel(channelType, channelID)
		if channel == nil || !config.IsTLVTransport(*channel) {
			next.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(&tlvContentTypeWriter{ResponseWriter: w}, r)
	})
}

// streamChannel resolves the channel a stream path is served from: the
// channel in /channels/... paths, or the service's channel in
// /services/{id}/stream and /programs/{id}/stream.
func streamChannel(ctx context.Context, path string, channels ChannelLookup, programs ProgramLookup) (string, string, bool) {
	if isChannelStreamPath(path) {
		return channelStreamTarget(path)
	}
	segments := strings.Split(strings.Trim(path, "/"), "/")
	if len(segments) != 3 || segments[2] != "stream" {
		return "", "", false
	}
	serviceID := segments[1]
	switch segments[0] {
	case "services":
	case "programs":
		id, err := strconv.ParseInt(segments[1], 10, 64)
		if err != nil {
			return "", "", false
		}
		p, ok, err := programs.Get(ctx, id)
		if err != nil || !ok {
			return "", "", false
		}
		serviceID = strconv.FormatInt(int64(p.Key.NetworkID)*100000+int64(p.Key.ServiceID), 10)
	default:
		return "", "", false
	}
	svc, err := channels.GetServiceById(ctx, serviceID)
	if err != nil || svc == nil {
		return "", "", false
	}
	return svc.ChannelType, svc.ChannelId, true
}

func isChannelStreamPath(path string) bool {
	if !strings.HasPrefix(path, "/channels/") || !strings.HasSuffix(path, "/stream") {
		return false
	}
	return true
}

// channelStreamTarget extracts the channel type and ID from
// /channels/{type}/{channel}/stream and
// /channels/{type}/{channel}/services/{id}/stream.
func channelStreamTarget(path string) (string, string, bool) {
	trimmed := strings.Trim(path, "/")
	segments := strings.Split(trimmed, "/")
	if len(segments) < 4 || segments[0] != "channels" {
		return "", "", false
	}
	if segments[len(segments)-1] != "stream" {
		return "", "", false
	}
	channelType, err := url.PathUnescape(segments[1])
	if err != nil {
		return "", "", false
	}
	channelID, err := url.PathUnescape(segments[2])
	if err != nil {
		return "", "", false
	}
	return channelType, channelID, true
}

type tlvContentTypeWriter struct {
	http.ResponseWriter
	wroteHeader bool
}

func (w *tlvContentTypeWriter) WriteHeader(code int) {
	w.rewrite()
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *tlvContentTypeWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.rewrite()
		w.wroteHeader = true
	}
	return w.ResponseWriter.Write(p)
}

// Flush forwards to the underlying writer so streaming responses keep the
// same flushing behavior with the wrapper in place.
func (w *tlvContentTypeWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *tlvContentTypeWriter) rewrite() {
	if w.Header().Get("Content-Type") == "video/mp2t" {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
}
