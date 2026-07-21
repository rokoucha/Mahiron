package databroadcast

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/stream/databroadcast/cachedb"
	"github.com/21S1298001/mahiron/ts"
)

func TestModuleCacheEvictsLeastRecentlyUsed(t *testing.T) {
	cache := NewModuleCache(6)
	key1 := ModuleCacheKey{ModuleID: 1}
	key2 := ModuleCacheKey{ModuleID: 2}
	key3 := ModuleCacheKey{ModuleID: 3}
	cache.Put(key1, ts.DSMCCModule{ModuleID: 1, Data: []byte("aaa")})
	cache.Put(key2, ts.DSMCCModule{ModuleID: 2, Data: []byte("bbb")})
	if _, ok := cache.Get(key1); !ok {
		t.Fatal("recent module missing")
	}
	cache.Put(key3, ts.DSMCCModule{ModuleID: 3, Data: []byte("ccc")})
	if _, ok := cache.Get(key2); ok {
		t.Fatal("least recently used module was not evicted")
	}
	if module, ok := cache.Get(key1); !ok || string(module.Data) != "aaa" {
		t.Fatalf("module 1 = %#v, %v", module, ok)
	}
}

func TestSQLiteModuleStorePersistsCompletedModule(t *testing.T) {
	store, err := NewSQLiteModuleStore(filepath.Join(t.TempDir(), "cache.sqlite3"), 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	key := ModuleCacheKey{ChannelType: "GR", ChannelID: "27", ServiceID: 101, ComponentTag: 0x40, DownloadID: 1, ModuleID: 2, Version: 3, Size: 4}
	store.Put(key, ts.DSMCCModule{Info: []byte("meta"), Data: []byte("data")})
	module, ok := store.Get(key)
	if !ok || string(module.Data) != "data" || string(module.Info) != "meta" {
		t.Fatalf("module = %#v, found = %v", module, ok)
	}
}

func TestSQLiteModuleStoreRecreatesCorruptCache(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.sqlite3")
	if err := os.WriteFile(path, []byte("not a SQLite database"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := NewSQLiteModuleStore(path, 10)
	if err != nil {
		t.Fatalf("recreate corrupt cache: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	key := ModuleCacheKey{ModuleID: 1, Size: 4}
	store.Put(key, ts.DSMCCModule{Data: []byte("data")})
	if module, ok := store.Get(key); !ok || string(module.Data) != "data" {
		t.Fatalf("module = %#v, found = %v", module, ok)
	}
}

func TestSQLiteModuleStorePrunesExpiredModules(t *testing.T) {
	store, err := NewSQLiteModuleStoreWithOptions(filepath.Join(t.TempDir(), "cache.sqlite3"), SQLiteModuleStoreOptions{MaxBytes: 1024, MaxAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	key := ModuleCacheKey{ModuleID: 1, Size: 4}
	if !store.Put(key, ts.DSMCCModule{Data: []byte("data")}) {
		t.Fatal("put failed")
	}
	if err := store.queries.SetAllLastAccessed(t.Context(), 0); err != nil {
		t.Fatal(err)
	}
	store.prune()
	if _, ok := store.Get(key); ok {
		t.Fatal("expired module remained in cache")
	}
	if !store.WasEvicted(key.VersionKey()) {
		t.Fatal("expired module was not recorded as evicted")
	}
}

func TestSQLiteModuleStoreCoalescesAccessTouches(t *testing.T) {
	store, err := NewSQLiteModuleStore(filepath.Join(t.TempDir(), "cache.sqlite3"), 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	key := ModuleCacheKey{ChannelType: "GR", ChannelID: "27", ServiceID: 101, ComponentTag: 0x40, DownloadID: 1, ModuleID: 2, Version: 3, Size: 4}
	store.Put(key, ts.DSMCCModule{Data: []byte("data")})
	if _, ok := store.Get(key); !ok {
		t.Fatal("first read failed")
	}
	store.touchMu.Lock()
	first := store.touched[key]
	store.touchMu.Unlock()
	if _, ok := store.Get(key); !ok {
		t.Fatal("second read failed")
	}
	store.touchMu.Lock()
	second := store.touched[key]
	store.touchMu.Unlock()
	if !second.Equal(first) {
		t.Fatalf("touch timestamp changed within interval: %s -> %s", first, second)
	}
}

func TestModuleCacheReturnsCopy(t *testing.T) {
	cache := NewModuleCache(10)
	key := ModuleCacheKey{ModuleID: 1}
	cache.Put(key, ts.DSMCCModule{Data: []byte("data")})
	module, _ := cache.Get(key)
	module.Data[0] = 'X'
	again, _ := cache.Get(key)
	if string(again.Data) != "data" {
		t.Fatalf("cached data mutated: %q", again.Data)
	}
}

func TestModuleStoreFindsRetainedGenerationByImmutableURL(t *testing.T) {
	key := ModuleCacheKey{ChannelType: "GR", ChannelID: "27", ServiceID: 101, ComponentTag: 0x40, DownloadID: 1, ModuleID: 2, Version: 3, Size: 4}
	for _, store := range []ModuleStore{
		NewModuleCache(10),
	} {
		store.Put(key, ts.DSMCCModule{DownloadID: 1, ModuleID: 2, Version: 3, Size: 4, Data: []byte("data")})
		module, ok := store.GetVersion(key.VersionKey())
		if !ok || string(module.Data) != "data" {
			t.Fatalf("module = %#v, found = %v", module, ok)
		}
	}
}

func TestSQLiteModuleStoreFindsRetainedGenerationByImmutableURL(t *testing.T) {
	store, err := NewSQLiteModuleStore(filepath.Join(t.TempDir(), "cache.sqlite3"), 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	key := ModuleCacheKey{ChannelType: "GR", ChannelID: "27", ServiceID: 101, ComponentTag: 0x40, DownloadID: 1, ModuleID: 2, Version: 3, Size: 4}
	store.Put(key, ts.DSMCCModule{DownloadID: 1, ModuleID: 2, Version: 3, Size: 4, Data: []byte("data")})
	if _, ok := store.Get(key); !ok {
		t.Fatal("exact module missing")
	}
	module, ok := store.GetVersion(key.VersionKey())
	if !ok || string(module.Data) != "data" {
		t.Fatalf("module = %#v, found = %v", module, ok)
	}
}

func TestSQLiteModuleStoreMarksEvictedGeneration(t *testing.T) {
	store, err := NewSQLiteModuleStore(filepath.Join(t.TempDir(), "cache.sqlite3"), 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	first := ModuleCacheKey{ChannelType: "GR", ChannelID: "27", ServiceID: 101, ComponentTag: 0x40, DownloadID: 1, ModuleID: 2, Version: 3, Size: 4}
	second := first
	second.ModuleID = 3
	store.Put(first, ts.DSMCCModule{Data: []byte("first")})
	store.Put(second, ts.DSMCCModule{Data: []byte("next")})
	if !store.WasEvicted(first.VersionKey()) {
		t.Fatal("evicted module was not recorded")
	}
	if store.WasEvicted(second.VersionKey()) {
		t.Fatal("retained module marked evicted")
	}
}

func TestSQLiteModuleStorePersistsDecodedResources(t *testing.T) {
	store, err := NewSQLiteModuleStore(filepath.Join(t.TempDir(), "cache.sqlite3"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	key := ModuleCacheKey{ChannelType: "GR", ChannelID: "27", ServiceID: 101, ComponentTag: 0x40, DownloadID: 1, ModuleID: 2, Version: 3, Size: 100}
	raw := []byte("Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Location: index.bml\r\nContent-Type: text/bml\r\n\r\ncontent\r\n--x--\r\n")
	store.Put(key, ts.DSMCCModule{Info: []byte{}, Data: raw})
	resources, ok := store.GetDecodedResources(key.VersionKey())
	if !ok || len(resources) != 1 || resources[0].ContentLocation == nil || *resources[0].ContentLocation != "index.bml" || string(resources[0].Data) != "content" {
		t.Fatalf("resources = %#v, found = %v", resources, ok)
	}
	store.touchMu.Lock()
	first := store.touched[key]
	store.touchMu.Unlock()
	if _, ok := store.GetDecodedResources(key.VersionKey()); !ok {
		t.Fatal("second resource read failed")
	}
	store.touchMu.Lock()
	second := store.touched[key]
	store.touchMu.Unlock()
	if !second.Equal(first) {
		t.Fatalf("resource touch timestamp changed within interval: %s -> %s", first, second)
	}
	storedBytes, err := store.queries.GetStoredBytes(t.Context(), cachedb.GetStoredBytesParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), Size: int64(key.Size)})
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(len(raw) + len("content")); storedBytes != want {
		t.Fatalf("stored bytes = %d, want %d", storedBytes, want)
	}
}

func TestSQLiteModuleStorePersistsModuleScopedResource(t *testing.T) {
	store, err := NewSQLiteModuleStore(filepath.Join(t.TempDir(), "cache.sqlite3"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	key := ModuleCacheKey{ChannelType: "GR", ChannelID: "27", ServiceID: 101, ComponentTag: 0x40, DownloadID: 1, ModuleID: 2, Version: 3, Size: 100}
	store.Put(key, ts.DSMCCModule{Data: []byte("Content-Type: text/bml\r\nContent-Location: index.bml\r\n\r\ncontent")})
	resources, ok := store.GetDecodedResources(key.VersionKey())
	if !ok || len(resources) != 1 || resources[0].ContentLocation != nil {
		t.Fatalf("resources = %#v, found = %v", resources, ok)
	}
}

func TestSQLiteModuleStoreReplacesResourcesAtomically(t *testing.T) {
	store, err := NewSQLiteModuleStore(filepath.Join(t.TempDir(), "cache.sqlite3"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	key := ModuleCacheKey{ChannelType: "GR", ChannelID: "27", ServiceID: 101, ComponentTag: 0x40, DownloadID: 1, ModuleID: 2, Version: 3, Size: 100}
	store.Put(key, ts.DSMCCModule{Data: []byte("Content-Type: text/bml\r\nContent-Location: index.bml\r\n\r\nold")})
	store.Put(key, ts.DSMCCModule{Data: []byte("not a MIME entity")})
	if _, ok := store.GetDecodedResources(key.VersionKey()); ok {
		t.Fatal("stale decoded resource survived replacement")
	}
	module, ok := store.Get(key)
	if !ok || string(module.Data) != "not a MIME entity" {
		t.Fatalf("module = %#v, found = %v", module, ok)
	}
}
