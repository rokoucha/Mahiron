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

	"github.com/21S1298001/mahiron/internal/bml"
	"github.com/21S1298001/mahiron/internal/bml/cache"
	"github.com/21S1298001/mahiron/internal/bml/resource"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/stream"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
	"github.com/21S1298001/mahiron/ts"
)

func TestAPIDataBroadcastBITUsesWebBMLFieldNames(t *testing.T) {
	name := "局"
	payload := apiDataBroadcastEvent(1, bml.Event{Type: "bit", BIT: &bml.BIT{OriginalNetworkID: 0x7fe0, Broadcasters: []bml.Broadcaster{{BroadcasterID: 0xff, BroadcasterName: &name, Affiliations: []byte{1, 2}, Services: []bml.Service{{ServiceID: 101, ServiceType: 1}}}}}})
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
	event := apiDataBroadcastEvent(1, bml.Event{Type: "esEventUpdated", ESEvent: &bml.ESEvent{ComponentTag: 0x40, DataEventID: 3, Events: []bml.GeneralEvent{{Type: "nptEvent", TimeMode: 2, EventMessageNPT: &npt}}}})
	es := event["esEvent"].(map[string]any)
	if es["componentId"] != byte(0x40) || es["dataEventId"] != byte(3) {
		t.Fatalf("esEvent = %#v", es)
	}
	events := es["events"].([]map[string]any)
	if len(events) != 1 || events[0]["eventMessageNPT"] != npt {
		t.Fatalf("events = %#v", events)
	}
	pcr := apiDataBroadcastEvent(1, bml.Event{Type: "pcr", PCR: &bml.PCR{PCRBase: 10, PCRExtension: 20}})["pcr"].(map[string]any)
	if pcr["pcrBase"] != uint64(10) || pcr["pcrExtension"] != uint16(20) {
		t.Fatalf("pcr = %#v", pcr)
	}
}

func TestAPIDataBroadcastProgramInfoAndCurrentTimeUseLowerCamelCase(t *testing.T) {
	programPayload, err := json.Marshal(apiDataBroadcastEvent(1, bml.Event{Type: "programInfo", ProgramInfo: &bml.ProgramInfo{ServiceID: 101, EventIDs: []uint16{1}, RawSectionHex: "00"}}))
	if err != nil {
		t.Fatal(err)
	}
	currentPayload, err := json.Marshal(apiDataBroadcastEvent(1, bml.Event{Type: "currentTime", CurrentTime: &bml.CurrentTime{JSTTimeUnixMilli: 123}}))
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
	manifest := apiDataBroadcastModuleManifest(1, bml.Module{}, []resource.ModuleResource{{ID: "0", ContentType: "text/bml"}})
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
		apiDataBroadcastEvent(1, bml.Event{Type: "bit", BIT: &bml.BIT{Broadcasters: []bml.Broadcaster{{Affiliations: []byte{1, 128, 255}}}}}),
		apiDataBroadcastEvent(1, bml.Event{Type: "esEventUpdated", ESEvent: &bml.ESEvent{Events: []bml.GeneralEvent{{Type: "immediateEvent", PrivateData: []byte{0, 127, 255}}}}}),
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
	payload := apiDataBroadcastModule(100101, &bml.Module{
		Metadata: &bml.ModuleMetadata{Name: "index.bml", Type: "text/bml", CachingPriority: &priority},
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

func TestDataBroadcastAPIDisabledReturnsNotFoundBeforeUsingDependencies(t *testing.T) {
	handler := NewHandler(HandlerConfig{DataBroadcastDisabled: true})
	tests := []struct {
		name string
		call func(http.ResponseWriter) error
	}{
		{"events", func(w http.ResponseWriter) error {
			return handler.GetServiceDataBroadcastEvents(context.Background(), apigen.GetServiceDataBroadcastEventsParams{ID: 100101}, w)
		}},
		{"state", func(w http.ResponseWriter) error {
			return handler.GetServiceDataBroadcastState(context.Background(), apigen.GetServiceDataBroadcastStateParams{ID: 100101}, w)
		}},
		{"module version", func(w http.ResponseWriter) error {
			return handler.GetServiceDataBroadcastModuleVersion(context.Background(), apigen.GetServiceDataBroadcastModuleVersionParams{ID: 100101}, w)
		}},
		{"module raw", func(w http.ResponseWriter) error {
			return handler.GetServiceDataBroadcastModuleRaw(context.Background(), apigen.GetServiceDataBroadcastModuleRawParams{ID: 100101}, w)
		}},
		{"module resource", func(w http.ResponseWriter) error {
			return handler.GetServiceDataBroadcastModuleResource(context.Background(), apigen.GetServiceDataBroadcastModuleResourceParams{ID: 100101}, w)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			if err := tt.call(recorder); err != nil {
				t.Fatal(err)
			}
			if recorder.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
			}
		})
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

func TestGetServiceDataBroadcastStateFallsBackToProvisionalSnapshot(t *testing.T) {
	handler := testProgramHandler(t)
	handler.streamManager = fakeDataBroadcastStreamManager{}
	handler.bmlSnapshotStore = stubBMLSnapshotStore{
		service: bml.PersistedService{ServiceID: 101, PMTSection: testBMLPMTSection(101, 0x0200, 0x40), StoredAt: 1700000000},
		found:   true,
	}
	rec := httptest.NewRecorder()
	err := handler.GetServiceDataBroadcastState(context.Background(), apigen.GetServiceDataBroadcastStateParams{ID: 100101}, rec)
	if err != nil {
		t.Fatal(err)
	}
	body := rec.Body.String()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	if !strings.Contains(body, `"origin":"cache"`) || !strings.Contains(body, `"storedAt":1700000000000`) {
		t.Fatalf("body = %s, want cache origin with millisecond storedAt", body)
	}
}

func TestGetServiceDataBroadcastStateReturns404WithoutLiveOrCacheSnapshot(t *testing.T) {
	handler := testProgramHandler(t)
	handler.streamManager = fakeDataBroadcastStreamManager{}
	handler.bmlSnapshotStore = stubBMLSnapshotStore{}
	rec := httptest.NewRecorder()
	err := handler.GetServiceDataBroadcastState(context.Background(), apigen.GetServiceDataBroadcastStateParams{ID: 100101}, rec)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestGetServiceDataBroadcastStateAllowCacheZeroSkipsProvisionalSnapshot(t *testing.T) {
	handler := testProgramHandler(t)
	handler.streamManager = fakeDataBroadcastStreamManager{}
	handler.bmlSnapshotStore = stubBMLSnapshotStore{
		service: bml.PersistedService{ServiceID: 101, PMTSection: testBMLPMTSection(101, 0x0200, 0x40), StoredAt: 1700000000},
		found:   true,
	}
	params := apigen.GetServiceDataBroadcastStateParams{ID: 100101}
	params.AllowCache.SetTo(0)
	rec := httptest.NewRecorder()
	err := handler.GetServiceDataBroadcastState(context.Background(), params, rec)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 when allowCache=0 forbids the cache fallback", rec.Code)
	}
}

func TestGetServiceDataBroadcastStateLiveSessionTakesPriorityOverCache(t *testing.T) {
	handler := testProgramHandler(t)
	handler.streamManager = fakeDataBroadcastStreamManager{
		session:  fakeDataBroadcastSession{},
		existing: true,
	}
	handler.bmlSnapshotStore = stubBMLSnapshotStore{
		service: bml.PersistedService{ServiceID: 101, PMTSection: testBMLPMTSection(101, 0x0200, 0x40), StoredAt: 1700000000},
		found:   true,
	}
	rec := httptest.NewRecorder()
	err := handler.GetServiceDataBroadcastState(context.Background(), apigen.GetServiceDataBroadcastStateParams{ID: 100101}, rec)
	if err != nil {
		t.Fatal(err)
	}
	body := rec.Body.String()
	if rec.Code != http.StatusOK || !strings.Contains(body, `"origin":"live"`) || strings.Contains(body, `"origin":"cache"`) {
		t.Fatalf("status = %d, body = %s, want live origin when a session exists", rec.Code, body)
	}
}

func TestGetServiceDataBroadcastModuleVersionUsesImmutableURL(t *testing.T) {
	handler := testProgramHandler(t)
	module := bml.Module{ComponentTag: 0x40, DownloadID: 7, ModuleID: 2, Version: 3, ETag: `"dsmcc-test"`, Data: []byte("Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Location: startup.bml\r\nContent-Type: text/bml\r\n\r\nmodule\r\n--x--\r\n")}
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
	module := bml.Module{ComponentTag: 0x40, DownloadID: 7, ModuleID: 2, Version: 3, ETag: `"dsmcc-test"`, Data: []byte("module")}
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
	handler.streamManager = fakeDataBroadcastStreamManager{}
	store := cache.NewModuleCache(1024)
	key := bml.ModuleCacheKey{ChannelType: "GR", ChannelID: "27", ServiceID: 101, ComponentTag: 0x40, DownloadID: 7, ModuleID: 2, Version: 3, Size: 8}
	store.Put(key, ts.DSMCCModule{DownloadID: 7, ModuleID: 2, Version: 3, Size: 8, Data: []byte("retained")})
	handler.bmlStore = store
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
	module := bml.Module{ComponentTag: 0x40, DownloadID: 7, ModuleID: 2, Version: 3, ETag: `"dsmcc-test"`, Data: []byte("Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Location: startup.bml\r\nContent-Type: text/bml\r\n\r\nmodule\r\n--x--\r\n")}
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
	module := bml.Module{ComponentTag: 0x40, DownloadID: 7, ModuleID: 2, Version: 3, Data: []byte("not a MIME entity")}
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
	handler.streamManager = fakeDataBroadcastStreamManager{}
	handler.bmlStore = stubEvictedModuleStore{ModuleStore: cache.NewModuleCache(1024), evicted: true}
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
	module := bml.Module{ComponentTag: 0x40, DownloadID: 7, ModuleID: 2, Version: 3, ETag: `"dsmcc-test"`, Data: []byte("not a MIME entity")}
	contentLocation := "index.bml"
	handler.streamManager = fakeDataBroadcastStreamManager{
		session:  fakeDataBroadcastSession{module: module},
		existing: true,
	}
	handler.bmlStore = stubDecodedModuleStore{
		ModuleStore: cache.NewModuleCache(1024),
		resources:   []resource.ModuleResource{{ID: "0", ContentLocation: &contentLocation, ContentType: "text/bml", Data: []byte("cached")}},
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
	module := bml.Module{ComponentTag: 0x40, DownloadID: 7, ModuleID: 2, Version: 3, Data: []byte("Content-Type: text/bml\r\n\r\n" + strings.Repeat("x", 8*1024*1024+1))}
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

// stubBMLSnapshotStore serves a canned persisted PMT/DII state for
// provisional-snapshot tests. The handler replays it through bml.Hub, the
// same path production uses.
type stubBMLSnapshotStore struct {
	service bml.PersistedService
	found   bool
}

func (s stubBMLSnapshotStore) PutSnapshot(_, _ string, _ bml.PersistedService) error { return nil }

func (s stubBMLSnapshotStore) GetSnapshot(_, _ string, _ uint16) (bml.PersistedService, bool) {
	return s.service, s.found
}

// stubEvictedModuleStore reports a canned eviction verdict while delegating
// module reads to an embedded store.
type stubEvictedModuleStore struct {
	bml.ModuleStore
	evicted bool
}

func (s stubEvictedModuleStore) WasEvicted(bml.ModuleVersionKey) bool { return s.evicted }

// stubDecodedModuleStore serves canned decoded resources while delegating
// module reads to an embedded store.
type stubDecodedModuleStore struct {
	bml.ModuleStore
	resources []resource.ModuleResource
}

func (s stubDecodedModuleStore) GetDecodedResources(bml.ModuleVersionKey) ([]resource.ModuleResource, bool) {
	return s.resources, len(s.resources) != 0
}

// testBMLPMTSection builds a minimal valid PMT section carrying one
// data-carousel component, enough for RestoreSnapshot to replay.
func testBMLPMTSection(serviceID, carouselPID uint16, componentTag byte) []byte {
	esInfo := []byte{0x52, 0x01, componentTag}
	length := 9 + 5 + len(esInfo) + 4
	s := make([]byte, 3+length)
	s[0] = ts.TableIDPMT
	s[1] = 0xb0 | byte(length>>8)
	s[2] = byte(length)
	s[3] = byte(serviceID >> 8)
	s[4] = byte(serviceID)
	s[5] = 0xc1
	s[8] = 0x1f
	s[9] = 0xff
	off := 12
	s[off] = ts.StreamTypeDSMCCDataCarousel
	s[off+1] = 0xe0 | byte(carouselPID>>8)
	s[off+2] = byte(carouselPID)
	s[off+3] = 0xf0 | byte(len(esInfo)>>8)
	s[off+4] = byte(len(esInfo))
	copy(s[off+5:], esInfo)
	var crc uint32 = 0xffffffff
	for _, b := range s[:len(s)-4] {
		crc ^= uint32(b) << 24
		for range 8 {
			if crc&0x80000000 != 0 {
				crc = (crc << 1) ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
	}
	s[len(s)-4] = byte(crc >> 24)
	s[len(s)-3] = byte(crc >> 16)
	s[len(s)-2] = byte(crc >> 8)
	s[len(s)-1] = byte(crc)
	return s
}

type fakeDataBroadcastSession struct {
	stream.Session
	module bml.Module
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

func (s fakeDataBroadcastSession) ObserveDataBroadcast(_ context.Context, serviceID uint16, _ bool, observe func(bml.Event) error) error {
	return observe(bml.Event{
		Type:     "snapshot",
		Revision: 42,
		Snapshot: bml.Snapshot{
			ServiceID: serviceID,
			Revision:  42,
		},
	})
}

func (s fakeDataBroadcastSession) DataBroadcastModule(_ uint16, componentTag byte, moduleID uint16) (bml.Module, bool) {
	if s.module.ComponentTag != componentTag || s.module.ModuleID != moduleID {
		return bml.Module{}, false
	}
	return s.module, true
}

func (s fakeDataBroadcastSession) DataBroadcastSnapshot(serviceID uint16) bml.Snapshot {
	snapshot := bml.Snapshot{ServiceID: serviceID, Revision: 42}
	if s.module.ModuleID != 0 {
		snapshot.Components = []bml.Component{{ComponentTag: s.module.ComponentTag, Modules: []bml.Module{s.module}}}
	}
	return snapshot
}

func (s fakeDataBroadcastSession) DataBroadcastModuleVersion(_ uint16, componentTag byte, downloadID uint32, moduleID uint16, version byte) (bml.Module, bool) {
	if s.module.ComponentTag != componentTag || s.module.DownloadID != downloadID || s.module.ModuleID != moduleID || s.module.Version != version {
		return bml.Module{}, false
	}
	return s.module, true
}
