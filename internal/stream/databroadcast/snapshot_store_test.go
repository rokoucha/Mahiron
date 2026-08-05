package databroadcast

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/ts"
)

func TestSQLiteModuleStorePersistsAndRestoresSnapshot(t *testing.T) {
	store, err := NewSQLiteModuleStore(filepath.Join(t.TempDir(), "cache.sqlite3"), 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	service := PersistedService{
		ServiceID:  101,
		PMTSection: []byte("pmt-bytes"),
		Carousels: []PersistedCarousel{
			{ComponentTag: 0x40, PID: 0x0200, DIISection: []byte("dii-40")},
			{ComponentTag: 0x41, PID: 0x0201, DIISection: []byte("dii-41")},
		},
	}
	if err := store.PutSnapshot("GR", "27", service); err != nil {
		t.Fatal(err)
	}

	got, ok := store.GetSnapshot("GR", "27", 101)
	if !ok {
		t.Fatal("snapshot not found")
	}
	if string(got.PMTSection) != "pmt-bytes" {
		t.Fatalf("PMTSection = %q", got.PMTSection)
	}
	if got.StoredAt == 0 {
		t.Fatal("StoredAt was not populated")
	}
	if len(got.Carousels) != 2 {
		t.Fatalf("Carousels = %#v, want 2", got.Carousels)
	}
	if got.Carousels[0].ComponentTag != 0x40 || got.Carousels[0].PID != 0x0200 || string(got.Carousels[0].DIISection) != "dii-40" {
		t.Fatalf("Carousels[0] = %#v", got.Carousels[0])
	}
	if got.Carousels[1].ComponentTag != 0x41 || got.Carousels[1].PID != 0x0201 || string(got.Carousels[1].DIISection) != "dii-41" {
		t.Fatalf("Carousels[1] = %#v", got.Carousels[1])
	}
}

func TestSQLiteModuleStoreGetSnapshotMissingReturnsFalse(t *testing.T) {
	store, err := NewSQLiteModuleStore(filepath.Join(t.TempDir(), "cache.sqlite3"), 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if _, ok := store.GetSnapshot("GR", "27", 101); ok {
		t.Fatal("expected no snapshot to be found")
	}
}

func TestSQLiteModuleStoreRemovesDroppedSnapshotCarousel(t *testing.T) {
	store, err := NewSQLiteModuleStore(filepath.Join(t.TempDir(), "cache.sqlite3"), 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	service := PersistedService{
		ServiceID:  101,
		PMTSection: []byte("pmt-1"),
		Carousels: []PersistedCarousel{
			{ComponentTag: 0x40, PID: 0x0200, DIISection: []byte("dii-40")},
			{ComponentTag: 0x41, PID: 0x0201, DIISection: []byte("dii-41")},
		},
	}
	if err := store.PutSnapshot("GR", "27", service); err != nil {
		t.Fatal(err)
	}

	// Component 0x41 is no longer present in the PMT.
	service.PMTSection = []byte("pmt-2")
	service.Carousels = []PersistedCarousel{{ComponentTag: 0x40, PID: 0x0200, DIISection: []byte("dii-40")}}
	if err := store.PutSnapshot("GR", "27", service); err != nil {
		t.Fatal(err)
	}

	got, ok := store.GetSnapshot("GR", "27", 101)
	if !ok {
		t.Fatal("snapshot not found")
	}
	if len(got.Carousels) != 1 || got.Carousels[0].ComponentTag != 0x40 {
		t.Fatalf("Carousels = %#v, want only component 0x40", got.Carousels)
	}
}

func TestSQLiteModuleStorePrunesExpiredSnapshots(t *testing.T) {
	store, err := NewSQLiteModuleStoreWithOptions(filepath.Join(t.TempDir(), "cache.sqlite3"), SQLiteModuleStoreOptions{MaxBytes: 1024, SnapshotMaxAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	service := PersistedService{ServiceID: 101, PMTSection: []byte("pmt"), Carousels: []PersistedCarousel{{ComponentTag: 0x40, PID: 0x0200, DIISection: []byte("dii")}}}
	if err := store.PutSnapshot("GR", "27", service); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec("UPDATE data_broadcast_snapshots SET stored_at = 0"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec("UPDATE data_broadcast_snapshot_carousels SET stored_at = 0"); err != nil {
		t.Fatal(err)
	}

	store.pruneSnapshots()

	if _, ok := store.GetSnapshot("GR", "27", 101); ok {
		t.Fatal("expired snapshot remained in cache")
	}
}

func TestSQLiteModuleStoreSnapshotMaxAgeZeroKeepsSnapshotsIndefinitely(t *testing.T) {
	store, err := NewSQLiteModuleStoreWithOptions(filepath.Join(t.TempDir(), "cache.sqlite3"), SQLiteModuleStoreOptions{MaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	service := PersistedService{ServiceID: 101, PMTSection: []byte("pmt"), Carousels: []PersistedCarousel{{ComponentTag: 0x40, PID: 0x0200, DIISection: []byte("dii")}}}
	if err := store.PutSnapshot("GR", "27", service); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec("UPDATE data_broadcast_snapshots SET stored_at = 0"); err != nil {
		t.Fatal(err)
	}

	store.pruneSnapshots()

	if _, ok := store.GetSnapshot("GR", "27", 101); !ok {
		t.Fatal("snapshot was pruned despite SnapshotMaxAge = 0 (unlimited)")
	}
}

func TestModuleCacheHasReportsPresenceWithoutPayload(t *testing.T) {
	cache := NewModuleCache(10)
	key := ModuleCacheKey{ModuleID: 1}
	if cache.Has(key) {
		t.Fatal("empty cache reported a hit")
	}
	cache.Put(key, ts.DSMCCModule{Data: []byte("data")})
	if !cache.Has(key) {
		t.Fatal("cache did not report presence after Put")
	}
}

func TestSQLiteModuleStoreHasReportsPresenceWithoutPayload(t *testing.T) {
	store, err := NewSQLiteModuleStore(filepath.Join(t.TempDir(), "cache.sqlite3"), 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	key := ModuleCacheKey{ChannelType: "GR", ChannelID: "27", ServiceID: 101, ComponentTag: 0x40, DownloadID: 1, ModuleID: 2, Version: 3, Size: 4}
	if store.Has(key) {
		t.Fatal("empty store reported a hit")
	}
	store.Put(key, ts.DSMCCModule{Data: []byte("data")})
	if !store.Has(key) {
		t.Fatal("store did not report presence after Put")
	}
}
