package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
)

type stubChannelLookup struct {
	channels map[string]*config.ChannelConfig
	services map[string]*service.Service
}

func (s stubChannelLookup) GetChannel(channelType, channel string) *config.ChannelConfig {
	return s.channels[channelType+"\x00"+channel]
}

func (s stubChannelLookup) GetServiceById(_ context.Context, id string) (*service.Service, error) {
	return s.services[id], nil
}

// stubProgramLookup knows program 7 of service 101 of network 11.
type stubProgramLookup struct{}

func (stubProgramLookup) Get(_ context.Context, id int64) (*program.Program, bool, error) {
	if id != 7 {
		return nil, false, nil
	}
	return &program.Program{Event: model.Event{Key: model.ServiceKey{NetworkID: 11, ServiceID: 101}}}, true, nil
}

func videoHandler(body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	})
}

func TestTLVStreamContentTypeRewritesTLVChannel(t *testing.T) {
	lookup := stubChannelLookup{channels: map[string]*config.ChannelConfig{
		"BS4K\x00101": {Type: "BS4K", Channel: "101", Transport: config.TransportTLV},
		"GR\x0027":    {Type: "GR", Channel: "27", Transport: config.TransportTS},
	}, services: map[string]*service.Service{
		"1100101": {ChannelType: "BS4K", ChannelId: "101"},
	}}
	handler := TLVStreamContentType(videoHandler("tlv"), lookup, stubProgramLookup{})

	for _, path := range []string{
		"/channels/BS4K/101/stream",
		"/channels/BS4K/101/services/101/stream",
		"/services/1100101/stream",
		"/programs/7/stream",
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if got := rec.Header().Get("Content-Type"); got != "application/octet-stream" {
			t.Fatalf("GET %s Content-Type = %q, want application/octet-stream", path, got)
		}
		if rec.Body.String() != "tlv" {
			t.Fatalf("GET %s body = %q, want tlv", path, rec.Body.String())
		}
	}
}

func TestTLVStreamContentTypeKeepsTSChannel(t *testing.T) {
	lookup := stubChannelLookup{channels: map[string]*config.ChannelConfig{
		"GR\x0027": {Type: "GR", Channel: "27", Transport: config.TransportTS},
	}, services: map[string]*service.Service{
		"102400": {ChannelType: "GR", ChannelId: "27"},
	}}
	handler := TLVStreamContentType(videoHandler("ts"), lookup, stubProgramLookup{})

	for _, path := range []string{"/channels/GR/27/stream", "/services/102400/stream", "/services/404/stream"} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if got := rec.Header().Get("Content-Type"); got != "video/mp2t" {
			t.Fatalf("GET %s Content-Type = %q, want video/mp2t", path, got)
		}
	}
}

func TestTLVStreamContentTypePassesNonStreamThrough(t *testing.T) {
	lookup := stubChannelLookup{channels: map[string]*config.ChannelConfig{
		"BS4K\x00101": {Type: "BS4K", Channel: "101", Transport: config.TransportTLV},
	}}
	jsonHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
	})
	handler := TLVStreamContentType(jsonHandler, lookup, stubProgramLookup{})

	for _, path := range []string{
		"/channels/BS4K/101",
		"/tuners",
		"/channels/BS4K/101/stream/extra",
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if got := rec.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
			t.Fatalf("GET %s Content-Type = %q, want passthrough", path, got)
		}
	}
}

func TestChannelStreamTargetParsesPaths(t *testing.T) {
	channelType, channelID, ok := channelStreamTarget("/channels/BS4K/101/stream")
	if !ok || channelType != "BS4K" || channelID != "101" {
		t.Fatalf("target = %q %q %v", channelType, channelID, ok)
	}
	channelType, channelID, ok = channelStreamTarget("/channels/BS4K/101/services/102/stream")
	if !ok || channelType != "BS4K" || channelID != "101" {
		t.Fatalf("service target = %q %q %v", channelType, channelID, ok)
	}
	if _, _, ok := channelStreamTarget("/channels/BS4K/101"); ok {
		t.Fatal("non-stream path parsed as stream")
	}
}
