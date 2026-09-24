package bml

import (
	"context"
	"testing"

	"github.com/21S1298001/mahiron/ts"
)

// testModuleStore is a minimal in-memory ModuleStore for Hub white-box
// tests. The persistent SQLite behavior lives in bml/cache tests; here only
// the Hub's persistent-release and eviction-recovery paths matter.
type testModuleStore struct {
	modules map[ModuleCacheKey]ts.DSMCCModule
}

func newTestModuleStore() *testModuleStore {
	return &testModuleStore{modules: map[ModuleCacheKey]ts.DSMCCModule{}}
}

func (s *testModuleStore) Get(key ModuleCacheKey) (ts.DSMCCModule, bool) {
	module, ok := s.modules[key]
	return module, ok
}

func (s *testModuleStore) Has(key ModuleCacheKey) bool {
	_, ok := s.modules[key]
	return ok
}

func (s *testModuleStore) GetVersion(key ModuleVersionKey) (ts.DSMCCModule, bool) {
	var found ts.DSMCCModule
	ok := false
	for candidateKey, candidate := range s.modules {
		if candidateKey.VersionKey() != key {
			continue
		}
		if ok && candidate.Size != found.Size {
			return ts.DSMCCModule{}, false
		}
		found, ok = candidate, true
	}
	return found, ok
}

func (s *testModuleStore) Put(key ModuleCacheKey, module ts.DSMCCModule) bool {
	module.Info = append([]byte(nil), module.Info...)
	module.Data = append([]byte(nil), module.Data...)
	s.modules[key] = module
	return true
}

func (s *testModuleStore) PersistsCompletedModules() {}

func (s *testModuleStore) evict(key ModuleCacheKey) {
	delete(s.modules, key)
}

func TestSubscriberOverflowClosesConnectionForSnapshotReconnect(t *testing.T) {
	hub := NewHub()
	_, events, unsubscribe := hub.Subscribe(context.Background(), 101)
	defer unsubscribe()

	hub.mu.Lock()
	for range dataBroadcastSubscriberBuffer + 1 {
		hub.broadcastLocked(101, Event{Type: "currentTime"})
	}
	// The event after the full buffer closes the subscriber. A future EventSource
	// connection starts from a fresh snapshot rather than applying a gap.
	hub.broadcastLocked(101, Event{Type: "pcr"})
	hub.mu.Unlock()

	for range events {
	}

	snapshot, _, unsubscribeSnapshot := hub.Subscribe(context.Background(), 101)
	defer unsubscribeSnapshot()
	if snapshot.Revision != dataBroadcastSubscriberBuffer+1 {
		t.Fatalf("snapshot revision = %d, want %d", snapshot.Revision, dataBroadcastSubscriberBuffer+1)
	}
}

// TestSharedCarouselPIDDeliversToAllServices reproduces the TOKYO MX layout
// where sibling services (MX1/MX2) reference the same data-carousel ES PID in
// their PMTs. Every DII/DDB section on that PID must reach every referencing
// service's carousel: delivering each section to just one arbitrary service
// splits the block stream between carousels, so no service ever assembles a
// complete module.
func TestSharedCarouselPIDDeliversToAllServices(t *testing.T) {
	hub := NewHub()
	const carouselPID uint16 = 2400
	const componentTag byte = 0x60
	serviceIDs := []uint16{23608, 23610}
	hub.Observe(ts.PIDSection{PID: 0x101, Section: ts.Section(provisionalTestBuildPMT(serviceIDs[0], carouselPID, componentTag))})
	hub.Observe(ts.PIDSection{PID: 0x103, Section: ts.Section(provisionalTestBuildPMT(serviceIDs[1], carouselPID, componentTag))})
	hub.Observe(ts.PIDSection{PID: carouselPID, Section: ts.Section(provisionalTestBuildDII(1, 4, 2, 4, 3, nil))})
	hub.Observe(ts.PIDSection{PID: carouselPID, Section: ts.Section(sharedPIDTestBuildDDB(1, 2, 3, 0, []byte("data")))})
	for _, serviceID := range serviceIDs {
		got, ok := hub.ModuleVersion(serviceID, componentTag, 1, 2, 3)
		if !ok || string(got.Data) != "data" {
			t.Fatalf("service %d: module = %#v, found = %v, want completed module", serviceID, got, ok)
		}
	}
}

func sharedPIDTestBuildDDB(downloadID uint32, moduleID uint16, version byte, blockNumber uint16, data []byte) []byte {
	body := []byte{byte(moduleID >> 8), byte(moduleID), version, 0, byte(blockNumber >> 8), byte(blockNumber)}
	body = append(body, data...)
	return provisionalTestBuildDSMCCSection(ts.TableIDDSMCCDDB, 0x1003, downloadID, body)
}

func TestDIIReturnToEntry(t *testing.T) {
	if value := diiReturnToEntry([]byte{0xf0, 1, 0x80}); value == nil || !*value {
		t.Fatalf("return-to-entry = %v, want true", value)
	}
	if value := diiReturnToEntry([]byte{0xf0, 1, 0}); value == nil || *value {
		t.Fatalf("return-to-entry = %v, want false", value)
	}
	if value := diiReturnToEntry([]byte{0xf0, 2, 0x80}); value != nil {
		t.Fatalf("malformed descriptor = %v, want nil", value)
	}
}

func TestModuleVersionReadsPersistentModuleAfterLivePayloadRelease(t *testing.T) {
	store := newTestModuleStore()

	hub := NewHub().WithModuleStore(store)
	const serviceID uint16 = 101
	const componentTag byte = 0x40
	carousel := ts.NewDSMCCCarousel(ts.DSMCCCarouselLimits{})
	carousel.ObserveDII(&ts.DSMCCDII{DownloadID: 1, BlockSize: 4, Modules: []ts.DSMCCModuleInfo{{ModuleID: 2, ModuleSize: 4, Version: 3}}})
	module, complete, err := carousel.ObserveDDB(&ts.DSMCCDDB{DownloadID: 1, ModuleID: 2, ModuleVersion: 3, Data: []byte("data")})
	if err != nil || !complete {
		t.Fatalf("complete = %v, err = %v", complete, err)
	}
	key := hub.moduleCacheKey(serviceID, componentTag, module.DownloadID, module.ModuleID, module.Version, module.Size)
	if !store.Put(key, *module) || !carousel.ReleaseCompletedPayload(module.ModuleID) {
		t.Fatal("did not persist and release module")
	}

	hub.mu.Lock()
	service := hub.serviceLocked(serviceID)
	service.carousels[componentTag] = carousel
	hub.mu.Unlock()

	got, ok := hub.ModuleVersion(serviceID, componentTag, 1, 2, 3)
	if !ok || string(got.Data) != "data" {
		t.Fatalf("module = %#v, found = %v", got, ok)
	}
}

// TestModuleVersionRecoversAfterPersistentStoreEviction reproduces the
// production data-broadcast hang: a completed module's live payload is
// released after a persistent Put, then the store evicts that generation
// (byte budget, prune, corruption recovery, ...) before any client fetches
// it. Without recovery, the module stays reported as "complete" forever
// while every fetch 404s/425s. ModuleVersion must instead reset carousel
// assembly so the broadcaster's continuing DDB retransmissions rebuild it.
func TestModuleVersionRecoversAfterPersistentStoreEviction(t *testing.T) {
	store := newTestModuleStore()

	hub := NewHub().WithModuleStore(store)
	const serviceID uint16 = 101
	const componentTag byte = 0x40
	carousel := ts.NewDSMCCCarousel(ts.DSMCCCarouselLimits{})
	carousel.ObserveDII(&ts.DSMCCDII{DownloadID: 1, BlockSize: 4, Modules: []ts.DSMCCModuleInfo{{ModuleID: 2, ModuleSize: 4, Version: 3}}})
	module, complete, err := carousel.ObserveDDB(&ts.DSMCCDDB{DownloadID: 1, ModuleID: 2, ModuleVersion: 3, Data: []byte("data")})
	if err != nil || !complete {
		t.Fatalf("complete = %v, err = %v", complete, err)
	}
	key := hub.moduleCacheKey(serviceID, componentTag, module.DownloadID, module.ModuleID, module.Version, module.Size)
	if !store.Put(key, *module) || !carousel.ReleaseCompletedPayload(module.ModuleID) {
		t.Fatal("did not persist and release module")
	}

	hub.mu.Lock()
	service := hub.serviceLocked(serviceID)
	service.carousels[componentTag] = carousel
	hub.mu.Unlock()

	// Simulate the store evicting the only retained generation before a
	// client ever fetched it (byte budget, age prune, ...).
	store.evict(key)
	if store.Has(key) {
		t.Fatal("evict failed")
	}

	if _, ok := hub.ModuleVersion(serviceID, componentTag, 1, 2, 3); ok {
		t.Fatal("module found despite release and eviction")
	}
	announcements := carousel.Announcements()
	if len(announcements) != 1 || announcements[0].Complete {
		t.Fatalf("announcements after recovery reset = %#v, want reset to incomplete", announcements)
	}

	// The broadcaster's continuing retransmission of the same blocks must now
	// rebuild the module instead of being ignored as "already complete".
	if _, complete, err := carousel.ObserveDDB(&ts.DSMCCDDB{DownloadID: 1, ModuleID: 2, ModuleVersion: 3, Data: []byte("data")}); err != nil || !complete {
		t.Fatalf("rebuild after invalidate: complete = %v, err = %v", complete, err)
	}
	got, ok := hub.ModuleVersion(serviceID, componentTag, 1, 2, 3)
	if !ok || string(got.Data) != "data" {
		t.Fatalf("module after rebuild = %#v, found = %v", got, ok)
	}
}

// provisionalTestBuildPMT and provisionalTestBuildDII build minimal, valid
// section bytes. They intentionally duplicate the small builders in
// bml/cache's tests: each is a self-contained fixture tightly coupled to
// its own package's scenarios.
func provisionalTestBuildPMT(serviceID, carouselPID uint16, componentTag byte) []byte {
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
	provisionalTestWriteCRC(s)
	return s
}

func provisionalTestBuildDII(downloadID uint32, blockSize, moduleID uint16, moduleSize uint32, moduleVersion byte, moduleInfo []byte) []byte {
	body := []byte{
		byte(downloadID >> 24), byte(downloadID >> 16), byte(downloadID >> 8), byte(downloadID),
		byte(blockSize >> 8), byte(blockSize),
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0,
		0, 1,
		byte(moduleID >> 8), byte(moduleID),
		byte(moduleSize >> 24), byte(moduleSize >> 16), byte(moduleSize >> 8), byte(moduleSize),
		moduleVersion,
		byte(len(moduleInfo)),
	}
	body = append(body, moduleInfo...)
	return provisionalTestBuildDSMCCSection(ts.TableIDDSMCCDII, 0x1002, 1, body)
}

func provisionalTestBuildDSMCCSection(tableID byte, messageID uint16, headerID uint32, body []byte) []byte {
	message := []byte{0x11, 0x03, byte(messageID >> 8), byte(messageID), byte(headerID >> 24), byte(headerID >> 16), byte(headerID >> 8), byte(headerID), 0xff, 0}
	message = append(message, byte(len(body)>>8), byte(len(body)))
	message = append(message, body...)
	length := 5 + len(message) + 4
	s := make([]byte, 3+length)
	s[0] = tableID
	s[1] = 0xb0 | byte(length>>8)
	s[2] = byte(length)
	s[3] = 0
	s[4] = 1
	s[5] = 0xc1
	copy(s[8:], message)
	provisionalTestWriteCRC(s)
	return s
}

func provisionalTestWriteCRC(s []byte) {
	crc := provisionalTestCRC32MPEG2(s[:len(s)-4])
	s[len(s)-4] = byte(crc >> 24)
	s[len(s)-3] = byte(crc >> 16)
	s[len(s)-2] = byte(crc >> 8)
	s[len(s)-1] = byte(crc)
}

func provisionalTestCRC32MPEG2(data []byte) uint32 {
	var crc uint32 = 0xffffffff
	for _, b := range data {
		crc ^= uint32(b) << 24
		for range 8 {
			if crc&0x80000000 != 0 {
				crc = (crc << 1) ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
