package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/stream"
	"github.com/21S1298001/mahiron/internal/stream/databroadcast"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
	"github.com/21S1298001/mahiron/ts"
)

func TestAPIDataBroadcastBITUsesWebBMLFieldNames(t *testing.T) {
	name := "局"
	payload := apiDataBroadcastEvent(1, databroadcast.DataBroadcastEvent{Type: "bit", BIT: &databroadcast.DataBroadcastBIT{OriginalNetworkID: 0x7fe0, Broadcasters: []databroadcast.DataBroadcastBroadcaster{{BroadcasterID: 0xff, BroadcasterName: &name, Affiliations: []byte{1, 2}, Services: []databroadcast.DataBroadcastService{{ServiceID: 101, ServiceType: 1}}}}}})
	bit, ok := payload["bit"].(map[string]any)
	if !ok || bit["originalNetworkId"] != uint16(0x7fe0) {
		t.Fatalf("bit = %#v", payload["bit"])
	}
	broadcasters := bit["broadcasters"].([]map[string]any)
	if len(broadcasters) != 1 || broadcasters[0]["broadcasterId"] != byte(0xff) || broadcasters[0]["affiliations"] == nil {
		t.Fatalf("broadcasters = %#v", broadcasters)
	}
}

func TestAPIDataBroadcastPCRAndNPTUseWebBMLFieldNames(t *testing.T) {
	npt := uint64(0x112345678)
	event := apiDataBroadcastEvent(1, databroadcast.DataBroadcastEvent{Type: "esEventUpdated", ESEvent: &databroadcast.DataBroadcastESEvent{ComponentTag: 0x40, DataEventID: 3, Events: []databroadcast.DataBroadcastGeneralEvent{{Type: "nptEvent", TimeMode: 2, EventMessageNPT: &npt}}}})
	es := event["esEvent"].(map[string]any)
	if es["componentId"] != byte(0x40) || es["dataEventId"] != byte(3) {
		t.Fatalf("esEvent = %#v", es)
	}
	events := es["events"].([]map[string]any)
	if len(events) != 1 || events[0]["eventMessageNPT"] != npt {
		t.Fatalf("events = %#v", events)
	}
	pcr := apiDataBroadcastEvent(1, databroadcast.DataBroadcastEvent{Type: "pcr", PCR: &databroadcast.DataBroadcastPCR{PCRBase: 10, PCRExtension: 20}})["pcr"].(map[string]any)
	if pcr["pcrBase"] != uint64(10) || pcr["pcrExtension"] != uint16(20) {
		t.Fatalf("pcr = %#v", pcr)
	}
}

func TestAPIDataBroadcastByteFieldsEncodeAsNumberArrays(t *testing.T) {
	payloads := []map[string]any{
		apiDataBroadcastEvent(1, databroadcast.DataBroadcastEvent{Type: "bit", BIT: &databroadcast.DataBroadcastBIT{Broadcasters: []databroadcast.DataBroadcastBroadcaster{{Affiliations: []byte{1, 128, 255}}}}}),
		apiDataBroadcastEvent(1, databroadcast.DataBroadcastEvent{Type: "esEventUpdated", ESEvent: &databroadcast.DataBroadcastESEvent{Events: []databroadcast.DataBroadcastGeneralEvent{{Type: "immediateEvent", PrivateData: []byte{0, 127, 255}}}}}),
	}
	for _, payload := range payloads {
		encoded, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		if bit, ok := decoded["bit"].(map[string]any); ok {
			affiliations := bit["broadcasters"].([]any)[0].(map[string]any)["affiliations"]
			if _, ok := affiliations.([]any); !ok {
				t.Fatalf("affiliations encoded as %T: %s", affiliations, encoded)
			}
		}
		if es, ok := decoded["esEvent"].(map[string]any); ok {
			privateData := es["events"].([]any)[0].(map[string]any)["privateDataByte"]
			if _, ok := privateData.([]any); !ok {
				t.Fatalf("privateDataByte encoded as %T: %s", privateData, encoded)
			}
		}
	}
}

func TestAPIDataBroadcastModuleExposesParsedMetadata(t *testing.T) {
	priority := byte(80)
	payload := apiDataBroadcastModule(100101, &databroadcast.DataBroadcastModule{
		Metadata: &ts.DSMCCModuleMetadata{Name: "index.bml", Type: "text/bml", CachingPriority: &priority},
	})
	metadata := payload["metadata"].(map[string]any)
	if metadata["name"] != "index.bml" || metadata["type"] != "text/bml" || metadata["cachingPriority"] != &priority {
		t.Fatalf("metadata = %#v", metadata)
	}
}

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
	session := fakeDataBroadcastSession{module: databroadcast.DataBroadcastModule{
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
	if got := rec.Header().Get("Cache-Control"); got != "private, no-cache" {
		t.Fatalf("Cache-Control = %q", got)
	}
	if got := rec.Body.String(); got != "module" {
		t.Fatalf("body = %q", got)
	}

	rec = httptest.NewRecorder()
	err = handler.GetServiceDataBroadcastModule(context.Background(), apigen.GetServiceDataBroadcastModuleParams{
		ID:           100101,
		ComponentTag: 0x40,
		ModuleId:     2,
		IfNoneMatch:  apigen.NewOptString(`W/"other", W/"dsmcc-test"`),
	}, rec)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusNotModified {
		t.Fatalf("status = %d, want 304", rec.Code)
	}
	if got := rec.Header().Get("ETag"); got != `"dsmcc-test"` {
		t.Fatalf("304 ETag = %q", got)
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

func (m fakeDataBroadcastStreamManager) GetOrCreate(context.Context, string, string) (stream.Session, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.session, nil
}

func (m fakeDataBroadcastStreamManager) GetExisting(string, string) (stream.Session, bool) {
	return m.session, m.existing
}

func (m fakeDataBroadcastStreamManager) ActiveSessionCount() int { return 0 }

type fakeDataBroadcastSession struct {
	stream.Session
	module databroadcast.DataBroadcastModule
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

func (s fakeDataBroadcastSession) ObserveDataBroadcast(_ context.Context, serviceID uint16, _ bool, observe func(databroadcast.DataBroadcastEvent) error) error {
	return observe(databroadcast.DataBroadcastEvent{
		Type: "snapshot",
		Snapshot: databroadcast.DataBroadcastSnapshot{
			ServiceID: serviceID,
		},
	})
}

func (s fakeDataBroadcastSession) DataBroadcastModule(_ uint16, componentTag byte, moduleID uint16) (databroadcast.DataBroadcastModule, bool) {
	if s.module.ComponentTag != componentTag || s.module.ModuleID != moduleID {
		return databroadcast.DataBroadcastModule{}, false
	}
	return s.module, true
}
