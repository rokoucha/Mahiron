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
