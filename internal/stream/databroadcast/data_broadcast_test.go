package databroadcast

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/21S1298001/mahiron/ts"
)

func TestSubscriberOverflowClosesConnectionForSnapshotReconnect(t *testing.T) {
	hub := NewDataBroadcastHub()
	_, events, unsubscribe := hub.Subscribe(context.Background(), 101)
	defer unsubscribe()

	hub.mu.Lock()
	for range dataBroadcastSubscriberBuffer + 1 {
		hub.broadcastLocked(101, DataBroadcastEvent{Type: "currentTime"})
	}
	// The event after the full buffer closes the subscriber. A future EventSource
	// connection starts from a fresh snapshot rather than applying a gap.
	hub.broadcastLocked(101, DataBroadcastEvent{Type: "pcr"})
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
	hub := NewDataBroadcastHub()
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
	store, err := NewSQLiteModuleStore(filepath.Join(t.TempDir(), "cache.sqlite3"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	hub := NewDataBroadcastHub().WithModuleStore(store)
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
	store, err := NewSQLiteModuleStore(filepath.Join(t.TempDir(), "cache.sqlite3"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	hub := NewDataBroadcastHub().WithModuleStore(store)
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
	store.maxBytes = 0
	store.prune()
	if store.Has(key) {
		t.Fatal("prune failed")
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
