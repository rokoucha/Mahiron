package databroadcast

import (
	"path/filepath"
	"testing"

	"github.com/21S1298001/mahiron/ts"
)

// Caches written before the prune index exists carry the original
// last_accessed index, which the upgrade replaces. Existing modules must
// survive that swap: the cache is disposable, but silently emptying it costs a
// receiver every carousel it had already assembled.
func TestSQLiteModuleStoreUpgradesLastAccessedIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.sqlite3")
	store, err := NewSQLiteModuleStore(path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	key := ModuleCacheKey{ChannelType: "GR", ChannelID: "27", ServiceID: 101, ComponentTag: 0x40, DownloadID: 1, ModuleID: 2, Version: 3, Size: 4}
	if !store.Put(key, ts.DSMCCModule{Info: []byte("meta"), Data: []byte("data")}) {
		t.Fatal("put failed")
	}
	if _, err := store.db.Exec(`DROP INDEX data_broadcast_modules_prune;
		CREATE INDEX data_broadcast_modules_last_accessed ON data_broadcast_modules(last_accessed);
		DELETE FROM atlas_schema_revisions WHERE version = '202608080001'`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	upgraded, err := NewSQLiteModuleStore(path, 1024)
	if err != nil {
		t.Fatalf("reopen pre-index cache: %v", err)
	}
	t.Cleanup(func() { _ = upgraded.Close() })
	var indexes int
	if err := upgraded.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'data_broadcast_modules_prune'`).Scan(&indexes); err != nil {
		t.Fatal(err)
	}
	if indexes != 1 {
		t.Fatal("prune index missing after upgrade")
	}
	if module, ok := upgraded.Get(key); !ok || string(module.Data) != "data" {
		t.Fatalf("module = %#v, found = %v", module, ok)
	}
}
