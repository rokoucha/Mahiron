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

func TestAPIDataBroadcastProgramInfoAndCurrentTimeUseLowerCamelCase(t *testing.T) {
	programPayload, err := json.Marshal(apiDataBroadcastEvent(1, databroadcast.DataBroadcastEvent{Type: "programInfo", ProgramInfo: &databroadcast.DataBroadcastProgramInfo{ServiceID: 101, EventIDs: []uint16{1}, RawSectionHex: "00"}}))
	if err != nil {
		t.Fatal(err)
	}
	currentPayload, err := json.Marshal(apiDataBroadcastEvent(1, databroadcast.DataBroadcastEvent{Type: "currentTime", CurrentTime: &databroadcast.DataBroadcastCurrentTime{JSTTimeUnixMilli: 123}}))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(programPayload); !strings.Contains(got, `"serviceId":101`) || !strings.Contains(got, `"eventIds":[1]`) || !strings.Contains(got, `"rawSectionHex":"00"`) || strings.Contains(got, "ServiceID") {
		t.Fatalf("programInfo = %s", got)
	}
	if got := string(currentPayload); !strings.Contains(got, `"jstTimeUnixMilli":123`) || strings.Contains(got, "JSTTimeUnixMilli") {
		t.Fatalf("currentTime = %s", got)
	}
}

func TestAPIDataBroadcastDirectModuleHasNullContentLocation(t *testing.T) {
	manifest := apiDataBroadcastModuleManifest(1, databroadcast.DataBroadcastModule{}, []databroadcast.ModuleResource{{ID: "0", ContentType: "text/bml"}})
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"contentLocation":null`) {
		t.Fatalf("manifest = %s", encoded)
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
	if !strings.Contains(body, "event: snapshot\nid: 0") || !strings.Contains(body, `"revision":42`) || !strings.Contains(body, `"type":"snapshot"`) {
		t.Fatalf("SSE body = %q, want snapshot event", body)
	}
}

func TestGetServiceDataBroadcastStateReturnsAuthoritativeSnapshot(t *testing.T) {
	handler := testProgramHandler(t)
	handler.streamManager = fakeDataBroadcastStreamManager{session: fakeDataBroadcastSession{}, existing: true}
	rec := httptest.NewRecorder()
	err := handler.GetServiceDataBroadcastState(context.Background(), apigen.GetServiceDataBroadcastStateParams{ID: 100101}, rec)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"revision":42`) {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestGetServiceDataBroadcastModuleVersionUsesImmutableURL(t *testing.T) {
	handler := testProgramHandler(t)
	module := databroadcast.DataBroadcastModule{ComponentTag: 0x40, DownloadID: 7, ModuleID: 2, Version: 3, ETag: `"dsmcc-test"`, Data: []byte("Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Location: startup.bml\r\nContent-Type: text/bml\r\n\r\nmodule\r\n--x--\r\n")}
	handler.streamManager = fakeDataBroadcastStreamManager{session: fakeDataBroadcastSession{module: module}, existing: true}
	rec := httptest.NewRecorder()
	err := handler.GetServiceDataBroadcastModuleVersion(context.Background(), apigen.GetServiceDataBroadcastModuleVersionParams{ID: 100101, ComponentTag: 0x40, DownloadId: 7, ModuleId: 2, ModuleVersion: 3}, rec)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || rec.Header().Get("Cache-Control") != "private, max-age=31536000, immutable" || !strings.Contains(rec.Body.String(), `"contentLocation":"startup.bml"`) {
		t.Fatalf("status = %d, cache-control = %q, body = %q", rec.Code, rec.Header().Get("Cache-Control"), rec.Body.String())
	}
}

func TestGetServiceDataBroadcastModuleRawReturnsNotModified(t *testing.T) {
	handler := testProgramHandler(t)
	module := databroadcast.DataBroadcastModule{ComponentTag: 0x40, DownloadID: 7, ModuleID: 2, Version: 3, ETag: `"dsmcc-test"`, Data: []byte("module")}
	handler.streamManager = fakeDataBroadcastStreamManager{session: fakeDataBroadcastSession{module: module}, existing: true}
	params := apigen.GetServiceDataBroadcastModuleRawParams{ID: 100101, ComponentTag: 0x40, DownloadId: 7, ModuleId: 2, ModuleVersion: 3}
	params.IfNoneMatch.SetTo(`"dsmcc-test"`)
	rec := httptest.NewRecorder()
	if err := handler.GetServiceDataBroadcastModuleRaw(context.Background(), params, rec); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusNotModified || rec.Body.Len() != 0 {
		t.Fatalf("status = %d, body = %q", rec.Code, rec.Body.String())
	}
}

func TestGetServiceDataBroadcastModuleRawUsesRetainedModuleWithoutSession(t *testing.T) {
	handler := testProgramHandler(t)
	module := databroadcast.DataBroadcastModule{ComponentTag: 0x40, DownloadID: 7, ModuleID: 2, Version: 3, ETag: `"dsmcc-test"`, Data: []byte("retained")}
	handler.streamManager = fakeDataBroadcastStreamManager{cachedModule: module}
	rec := httptest.NewRecorder()
	err := handler.GetServiceDataBroadcastModuleRaw(context.Background(), apigen.GetServiceDataBroadcastModuleRawParams{ID: 100101, ComponentTag: 0x40, DownloadId: 7, ModuleId: 2, ModuleVersion: 3}, rec)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || rec.Body.String() != "retained" {
		t.Fatalf("status = %d, body = %q", rec.Code, rec.Body.String())
	}
}

func TestGetServiceDataBroadcastModuleResourceServesDecodedPart(t *testing.T) {
	handler := testProgramHandler(t)
	module := databroadcast.DataBroadcastModule{ComponentTag: 0x40, DownloadID: 7, ModuleID: 2, Version: 3, ETag: `"dsmcc-test"`, Data: []byte("Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Location: startup.bml\r\nContent-Type: text/bml\r\n\r\nmodule\r\n--x--\r\n")}
	handler.streamManager = fakeDataBroadcastStreamManager{session: fakeDataBroadcastSession{module: module}, existing: true}
	rec := httptest.NewRecorder()
	err := handler.GetServiceDataBroadcastModuleResource(context.Background(), apigen.GetServiceDataBroadcastModuleResourceParams{ID: 100101, ComponentTag: 0x40, DownloadId: 7, ModuleId: 2, ModuleVersion: 3, ResourceId: "0"}, rec)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "text/bml" || rec.Body.String() != "module" {
		t.Fatalf("status = %d, content-type = %q, body = %q", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
}

func TestGetServiceDataBroadcastModuleVersionRejectsMalformedEntity(t *testing.T) {
	handler := testProgramHandler(t)
	module := databroadcast.DataBroadcastModule{ComponentTag: 0x40, DownloadID: 7, ModuleID: 2, Version: 3, Data: []byte("not a MIME entity")}
	handler.streamManager = fakeDataBroadcastStreamManager{session: fakeDataBroadcastSession{module: module}, existing: true}
	rec := httptest.NewRecorder()
	err := handler.GetServiceDataBroadcastModuleVersion(context.Background(), apigen.GetServiceDataBroadcastModuleVersionParams{ID: 100101, ComponentTag: 0x40, DownloadId: 7, ModuleId: 2, ModuleVersion: 3}, rec)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}
}

func TestGetServiceDataBroadcastModuleRawReportsEvictedGeneration(t *testing.T) {
	handler := testProgramHandler(t)
	handler.streamManager = fakeDataBroadcastStreamManager{evicted: true}
	rec := httptest.NewRecorder()
	err := handler.GetServiceDataBroadcastModuleRaw(context.Background(), apigen.GetServiceDataBroadcastModuleRawParams{ID: 100101, ComponentTag: 0x40, DownloadId: 7, ModuleId: 2, ModuleVersion: 3}, rec)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusGone {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusGone)
	}
}

func TestGetServiceDataBroadcastModuleVersionUsesCachedResources(t *testing.T) {
	handler := testProgramHandler(t)
	module := databroadcast.DataBroadcastModule{ComponentTag: 0x40, DownloadID: 7, ModuleID: 2, Version: 3, ETag: `"dsmcc-test"`, Data: []byte("not a MIME entity")}
	contentLocation := "index.bml"
	handler.streamManager = fakeDataBroadcastStreamManager{
		session:         fakeDataBroadcastSession{module: module},
		existing:        true,
		cachedResources: []databroadcast.ModuleResource{{ID: "0", ContentLocation: &contentLocation, ContentType: "text/bml", Data: []byte("cached")}},
	}
	rec := httptest.NewRecorder()
	err := handler.GetServiceDataBroadcastModuleVersion(context.Background(), apigen.GetServiceDataBroadcastModuleVersionParams{ID: 100101, ComponentTag: 0x40, DownloadId: 7, ModuleId: 2, ModuleVersion: 3}, rec)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"contentLocation":"index.bml"`) {
		t.Fatalf("status = %d, body = %q", rec.Code, rec.Body.String())
	}
}

func TestGetServiceDataBroadcastModuleVersionRejectsResourceLimit(t *testing.T) {
	handler := testProgramHandler(t)
	module := databroadcast.DataBroadcastModule{ComponentTag: 0x40, DownloadID: 7, ModuleID: 2, Version: 3, Data: []byte("Content-Type: text/bml\r\n\r\n" + strings.Repeat("x", 8*1024*1024+1))}
	handler.streamManager = fakeDataBroadcastStreamManager{session: fakeDataBroadcastSession{module: module}, existing: true}
	rec := httptest.NewRecorder()
	err := handler.GetServiceDataBroadcastModuleVersion(context.Background(), apigen.GetServiceDataBroadcastModuleVersionParams{ID: 100101, ComponentTag: 0x40, DownloadId: 7, ModuleId: 2, ModuleVersion: 3}, rec)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusInsufficientStorage {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInsufficientStorage)
	}
}

type fakeDataBroadcastStreamManager struct {
	err             error
	existing        bool
	session         fakeDataBroadcastSession
	cachedModule    databroadcast.DataBroadcastModule
	cachedResources []databroadcast.ModuleResource
	evicted         bool
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

func (m fakeDataBroadcastStreamManager) DataBroadcastCachedModule(_ string, _ string, _ uint16, componentTag byte, downloadID uint32, moduleID uint16, version byte) (databroadcast.DataBroadcastModule, bool) {
	module := m.cachedModule
	if module.ComponentTag != componentTag || module.DownloadID != downloadID || module.ModuleID != moduleID || module.Version != version {
		return databroadcast.DataBroadcastModule{}, false
	}
	return module, true
}

func (m fakeDataBroadcastStreamManager) DataBroadcastModuleWasEvicted(string, string, uint16, byte, uint32, uint16, byte) bool {
	return m.evicted
}

func (m fakeDataBroadcastStreamManager) DataBroadcastCachedResources(string, string, uint16, byte, uint32, uint16, byte) ([]databroadcast.ModuleResource, bool) {
	return m.cachedResources, len(m.cachedResources) != 0
}

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
		Type:     "snapshot",
		Revision: 42,
		Snapshot: databroadcast.DataBroadcastSnapshot{
			ServiceID: serviceID,
			Revision:  42,
		},
	})
}

func (s fakeDataBroadcastSession) DataBroadcastModule(_ uint16, componentTag byte, moduleID uint16) (databroadcast.DataBroadcastModule, bool) {
	if s.module.ComponentTag != componentTag || s.module.ModuleID != moduleID {
		return databroadcast.DataBroadcastModule{}, false
	}
	return s.module, true
}

func (s fakeDataBroadcastSession) DataBroadcastSnapshot(serviceID uint16) databroadcast.DataBroadcastSnapshot {
	snapshot := databroadcast.DataBroadcastSnapshot{ServiceID: serviceID, Revision: 42}
	if s.module.ModuleID != 0 {
		snapshot.Components = []databroadcast.DataBroadcastComponent{{ComponentTag: s.module.ComponentTag, Modules: []databroadcast.DataBroadcastModule{s.module}}}
	}
	return snapshot
}

func (s fakeDataBroadcastSession) DataBroadcastModuleVersion(_ uint16, componentTag byte, downloadID uint32, moduleID uint16, version byte) (databroadcast.DataBroadcastModule, bool) {
	if s.module.ComponentTag != componentTag || s.module.DownloadID != downloadID || s.module.ModuleID != moduleID || s.module.Version != version {
		return databroadcast.DataBroadcastModule{}, false
	}
	return s.module, true
}
