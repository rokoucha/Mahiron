package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/stream"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func TestGetServiceDataBroadcastEventsWritesSnapshot(t *testing.T) {
	handler := testProgramHandler(t)
	handler.streamManager = fakeDataBroadcastStreamManager{session: fakeDataBroadcastSession{}}
	rec := httptest.NewRecorder()
	err := handler.GetServiceDataBroadcastEvents(context.Background(), apigen.GetServiceDataBroadcastEventsParams{
		ID: 100101,
	}, rec)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("Content-Type = %q", got)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: snapshot") || !strings.Contains(body, `"type":"snapshot"`) {
		t.Fatalf("SSE body = %q, want snapshot event", body)
	}
}

func TestGetServiceDataBroadcastModuleReturnsETagAndNotModified(t *testing.T) {
	handler := testProgramHandler(t)
	session := fakeDataBroadcastSession{module: stream.DataBroadcastModule{
		ComponentTag: 0x40,
		ModuleID:     2,
		ETag:         `"dsmcc-test"`,
		Data:         []byte("module"),
	}}
	handler.streamManager = fakeDataBroadcastStreamManager{session: session, existing: true}

	rec := httptest.NewRecorder()
	err := handler.GetServiceDataBroadcastModule(context.Background(), apigen.GetServiceDataBroadcastModuleParams{
		ID:           100101,
		ComponentTag: 0x40,
		ModuleId:     2,
	}, rec)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("ETag"); got != `"dsmcc-test"` {
		t.Fatalf("ETag = %q", got)
	}
	if got := rec.Body.String(); got != "module" {
		t.Fatalf("body = %q", got)
	}

	rec = httptest.NewRecorder()
	err = handler.GetServiceDataBroadcastModule(context.Background(), apigen.GetServiceDataBroadcastModuleParams{
		ID:           100101,
		ComponentTag: 0x40,
		ModuleId:     2,
		IfNoneMatch:  apigen.NewOptString(`"dsmcc-test"`),
	}, rec)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusNotModified {
		t.Fatalf("status = %d, want 304", rec.Code)
	}
}

func TestGetServiceDataBroadcastModuleWithoutLiveSessionReturnsNotFound(t *testing.T) {
	handler := testProgramHandler(t)
	handler.streamManager = fakeDataBroadcastStreamManager{}
	rec := httptest.NewRecorder()
	err := handler.GetServiceDataBroadcastModule(context.Background(), apigen.GetServiceDataBroadcastModuleParams{
		ID:           100101,
		ComponentTag: 0x40,
		ModuleId:     2,
	}, rec)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

type fakeDataBroadcastStreamManager struct {
	err      error
	existing bool
	session  fakeDataBroadcastSession
}

func (m fakeDataBroadcastStreamManager) GetOrCreate(context.Context, string, string) (interface {
	ChannelStream(context.Context, bool, io.Writer) error
	ProgramStream(context.Context, *program.Program, bool, io.Writer) error
	ServiceStream(context.Context, uint16, bool, io.Writer) error
	ObserveDataBroadcast(context.Context, uint16, bool, func(stream.DataBroadcastEvent) error) error
	DataBroadcastModule(uint16, byte, uint16) (stream.DataBroadcastModule, bool)
}, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.session, nil
}

func (m fakeDataBroadcastStreamManager) GetExisting(string, string) (interface {
	ChannelStream(context.Context, bool, io.Writer) error
	ProgramStream(context.Context, *program.Program, bool, io.Writer) error
	ServiceStream(context.Context, uint16, bool, io.Writer) error
	ObserveDataBroadcast(context.Context, uint16, bool, func(stream.DataBroadcastEvent) error) error
	DataBroadcastModule(uint16, byte, uint16) (stream.DataBroadcastModule, bool)
}, bool) {
	return m.session, m.existing
}

func (m fakeDataBroadcastStreamManager) ActiveSessionCount() int { return 0 }

type fakeDataBroadcastSession struct {
	module stream.DataBroadcastModule
}

func (s fakeDataBroadcastSession) ChannelStream(context.Context, bool, io.Writer) error {
	return errors.New("unexpected ChannelStream call")
}

func (s fakeDataBroadcastSession) ProgramStream(context.Context, *program.Program, bool, io.Writer) error {
	return errors.New("unexpected ProgramStream call")
}

func (s fakeDataBroadcastSession) ServiceStream(context.Context, uint16, bool, io.Writer) error {
	return errors.New("unexpected ServiceStream call")
}

func (s fakeDataBroadcastSession) ObserveDataBroadcast(_ context.Context, serviceID uint16, _ bool, observe func(stream.DataBroadcastEvent) error) error {
	return observe(stream.DataBroadcastEvent{
		Type: "snapshot",
		Snapshot: stream.DataBroadcastSnapshot{
			ServiceID: serviceID,
		},
	})
}

func (s fakeDataBroadcastSession) DataBroadcastModule(_ uint16, componentTag byte, moduleID uint16) (stream.DataBroadcastModule, bool) {
	if s.module.ComponentTag != componentTag || s.module.ModuleID != moduleID {
		return stream.DataBroadcastModule{}, false
	}
	return s.module, true
}
