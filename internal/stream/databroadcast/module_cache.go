package databroadcast

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	mahirondb "github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/stream/databroadcast/cachedb"
	"github.com/21S1298001/mahiron/ts"
)

const (
	DefaultModuleCacheBytes  = 128 * 1024 * 1024
	maxModuleCacheTombstones = 4096
	moduleCacheTouchInterval = time.Minute
)

type ModuleCacheKey struct {
	ChannelType  string
	ChannelID    string
	ServiceID    uint16
	ComponentTag byte
	DownloadID   uint32
	ModuleID     uint16
	Version      byte
	Size         uint32
}

// ModuleVersionKey is the immutable URL identity of a module. Size remains a
// part of the DII identity used while restoring a live carousel, but is not
// present in resource URLs, so retained generations are looked up by this key.
type ModuleVersionKey struct {
	ChannelType  string
	ChannelID    string
	ServiceID    uint16
	ComponentTag byte
	DownloadID   uint32
	ModuleID     uint16
	Version      byte
}

func (k ModuleCacheKey) VersionKey() ModuleVersionKey {
	return ModuleVersionKey{
		ChannelType: k.ChannelType, ChannelID: k.ChannelID, ServiceID: k.ServiceID,
		ComponentTag: k.ComponentTag, DownloadID: k.DownloadID, ModuleID: k.ModuleID, Version: k.Version,
	}
}

// ModuleStore keeps completed modules across channel-session lifetimes. The
// assembler remains memory bounded; only completed, validated modules enter a
// store. ModuleCache is the small in-memory implementation used by default.
type ModuleStore interface {
	Get(ModuleCacheKey) (ts.DSMCCModule, bool)
	GetVersion(ModuleVersionKey) (ts.DSMCCModule, bool)
	Put(ModuleCacheKey, ts.DSMCCModule) bool
}

// PersistentModuleStore keeps successfully written modules independently of
// the live carousel, allowing its completed payload buffer to be released.
type PersistentModuleStore interface {
	ModuleStore
	PersistsCompletedModules()
}

// EvictedModuleStore records immutable module identities that were once
// completed but were removed to satisfy its cache limit.
type EvictedModuleStore interface {
	WasEvicted(ModuleVersionKey) bool
}

// DecodedModuleStore optionally retains expanded MIME resources alongside raw
// modules so requests do not repeatedly decompress and parse a carousel module.
type DecodedModuleStore interface {
	GetDecodedResources(ModuleVersionKey) ([]ModuleResource, bool)
}

type moduleCacheEntry struct {
	module ts.DSMCCModule
	used   uint64
}

// ModuleCache is a process-local, size-bounded LRU cache shared by channel
// sessions. Entries are keyed by every DII field that identifies module
// contents, so a new carousel version cannot restore stale bytes.
type ModuleCache struct {
	mu         sync.Mutex
	maxBytes   uint64
	bytes      uint64
	generation uint64
	entries    map[ModuleCacheKey]moduleCacheEntry
}

func NewModuleCache(maxBytes uint64) *ModuleCache {
	if maxBytes == 0 {
		maxBytes = DefaultModuleCacheBytes
	}
	return &ModuleCache{maxBytes: maxBytes, entries: map[ModuleCacheKey]moduleCacheEntry{}}
}

func (c *ModuleCache) Get(key ModuleCacheKey) (ts.DSMCCModule, bool) {
	if c == nil {
		return ts.DSMCCModule{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return ts.DSMCCModule{}, false
	}
	c.generation++
	entry.used = c.generation
	c.entries[key] = entry
	return cloneCachedModule(entry.module), true
}

func (c *ModuleCache) GetVersion(key ModuleVersionKey) (ts.DSMCCModule, bool) {
	if c == nil {
		return ts.DSMCCModule{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var found moduleCacheEntry
	ok := false
	for candidateKey, candidate := range c.entries {
		if candidateKey.VersionKey() != key {
			continue
		}
		// A conforming carousel changes moduleVersion when its bytes change.
		// Refuse an ambiguous retained entry rather than serving arbitrary data.
		if ok && candidate.module.Size != found.module.Size {
			return ts.DSMCCModule{}, false
		}
		found, ok = candidate, true
	}
	if !ok {
		return ts.DSMCCModule{}, false
	}
	c.generation++
	// Find the entry again to update its LRU use marker.
	for candidateKey, candidate := range c.entries {
		if candidateKey.VersionKey() == key && candidate.module.Size == found.module.Size {
			candidate.used = c.generation
			c.entries[candidateKey] = candidate
			break
		}
	}
	return cloneCachedModule(found.module), true
}

func (c *ModuleCache) Put(key ModuleCacheKey, module ts.DSMCCModule) bool {
	if c == nil || uint64(len(module.Data)) > c.maxBytes {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.entries[key]; ok {
		c.bytes -= uint64(len(old.module.Data))
	}
	c.generation++
	c.entries[key] = moduleCacheEntry{module: cloneCachedModule(module), used: c.generation}
	c.bytes += uint64(len(module.Data))
	for c.bytes > c.maxBytes {
		var oldestKey ModuleCacheKey
		var oldest moduleCacheEntry
		found := false
		for candidateKey, candidate := range c.entries {
			if !found || candidate.used < oldest.used {
				oldestKey, oldest, found = candidateKey, candidate, true
			}
		}
		if !found {
			break
		}
		delete(c.entries, oldestKey)
		c.bytes -= uint64(len(oldest.module.Data))
	}
	return true
}

func cloneCachedModule(module ts.DSMCCModule) ts.DSMCCModule {
	module.Info = append([]byte(nil), module.Info...)
	module.Data = append([]byte(nil), module.Data...)
	return module
}

// SQLiteModuleStore is a disposable, size-bounded cache for completed modules.
// It is deliberately separate from the application's primary database: cache
// loss must never affect EPG or recording data.
type SQLiteModuleStore struct {
	db       *sql.DB
	queries  *cachedb.Queries
	maxBytes uint64
	maxAge   time.Duration
	touchMu  sync.Mutex
	touched  map[ModuleCacheKey]time.Time
}

func NewSQLiteModuleStore(path string, maxBytes uint64) (*SQLiteModuleStore, error) {
	return NewSQLiteModuleStoreWithOptions(path, SQLiteModuleStoreOptions{MaxBytes: maxBytes})
}

type SQLiteModuleStoreOptions struct {
	MaxBytes uint64
	// MaxAge removes modules that have not been accessed within the duration.
	// A zero duration keeps entries until they are removed by the byte budget.
	MaxAge time.Duration
}

func NewSQLiteModuleStoreWithOptions(path string, options SQLiteModuleStoreOptions) (*SQLiteModuleStore, error) {
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultModuleCacheBytes
	}
	store, err := openSQLiteModuleStore(path, maxBytes, options.MaxAge)
	if err == nil || !isSQLiteCorruption(err) || path == ":memory:" || strings.HasPrefix(path, "file:") {
		return store, err
	}

	// This database is only a cache. A corrupt cache must not prevent the
	// receiver from starting; remove the database and its WAL sidecars, then
	// recreate an empty cache. Non-corruption errors (permissions, full disk,
	// and so on) are deliberately returned to let the caller choose a fallback.
	if removeErr := removeSQLiteCacheFiles(path); removeErr != nil {
		return nil, errors.Join(err, removeErr)
	}
	return openSQLiteModuleStore(path, maxBytes, options.MaxAge)
}

func openSQLiteModuleStore(path string, maxBytes uint64, maxAge time.Duration) (*SQLiteModuleStore, error) {
	db, err := mahirondb.Open(path)
	if err != nil {
		return nil, err
	}
	store := &SQLiteModuleStore{db: db, queries: cachedb.New(db), maxBytes: maxBytes, maxAge: maxAge, touched: map[ModuleCacheKey]time.Time{}}
	if err := cachedb.Migrate(context.Background(), db); err != nil {
		_ = db.Close()
		return nil, err
	}
	store.prune()
	return store, nil
}

func isSQLiteCorruption(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database disk image is malformed") ||
		strings.Contains(message, "database corruption") ||
		strings.Contains(message, "file is not a database") ||
		strings.Contains(message, "malformed database schema")
}

func removeSQLiteCacheFiles(path string) error {
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Remove(candidate); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (s *SQLiteModuleStore) Get(key ModuleCacheKey) (ts.DSMCCModule, bool) {
	if s == nil || s.db == nil {
		return ts.DSMCCModule{}, false
	}
	row, err := s.queries.GetModule(context.Background(), cachedb.GetModuleParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), Size: int64(key.Size)})
	if err != nil {
		return ts.DSMCCModule{}, false
	}
	module := ts.DSMCCModule{Info: row.Info, Data: row.Data}
	module.DownloadID, module.ModuleID, module.Version, module.Size = key.DownloadID, key.ModuleID, key.Version, key.Size
	s.touch(key)
	return module, true
}

func (s *SQLiteModuleStore) GetVersion(key ModuleVersionKey) (ts.DSMCCModule, bool) {
	if s == nil || s.db == nil {
		return ts.DSMCCModule{}, false
	}
	rows, err := s.queries.GetVersionModules(context.Background(), cachedb.GetVersionModulesParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version)})
	if err != nil {
		return ts.DSMCCModule{}, false
	}
	if len(rows) != 1 {
		return ts.DSMCCModule{}, false
	}
	row := rows[0]
	if row.Size < 0 || row.Size > int64(^uint32(0)) {
		return ts.DSMCCModule{}, false
	}
	module := ts.DSMCCModule{Size: uint32(row.Size), Info: row.Info, Data: row.Data}
	module.DownloadID, module.ModuleID, module.Version = key.DownloadID, key.ModuleID, key.Version
	s.touch(ModuleCacheKey{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: key.ServiceID, ComponentTag: key.ComponentTag, DownloadID: key.DownloadID, ModuleID: key.ModuleID, Version: key.Version, Size: module.Size})
	return module, true
}

func (s *SQLiteModuleStore) touch(key ModuleCacheKey) {
	if s == nil || s.db == nil {
		return
	}
	now := time.Now()
	s.touchMu.Lock()
	if previous, ok := s.touched[key]; ok && now.Sub(previous) < moduleCacheTouchInterval {
		s.touchMu.Unlock()
		return
	}
	s.touched[key] = now
	s.touchMu.Unlock()
	_ = s.queries.TouchModule(context.Background(), cachedb.TouchModuleParams{LastAccessed: now.Unix(), ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), Size: int64(key.Size)})
}

func (s *SQLiteModuleStore) Put(key ModuleCacheKey, module ts.DSMCCModule) bool {
	if s == nil || s.db == nil || uint64(len(module.Data)) > s.maxBytes {
		return false
	}
	resources, _ := DecodeModuleResources(CompletedModule(key.ComponentTag, module))
	storedBytes := uint64(len(module.Data))
	for _, resource := range resources {
		storedBytes += uint64(len(resource.Data))
	}
	if storedBytes > s.maxBytes || storedBytes > uint64(^uint64(0)>>1) {
		return false
	}
	tx, err := s.db.Begin()
	if err != nil {
		return false
	}
	defer func() { _ = tx.Rollback() }()
	queries := s.queries.WithTx(tx)
	ctx := context.Background()
	// A module may legitimately have no module-info bytes. SQLite maps a nil
	// slice to NULL, while the cache schema intentionally requires BLOB values.
	info := module.Info
	if info == nil {
		info = []byte{}
	}
	now := time.Now().Unix()
	if err := queries.UpsertModule(ctx, cachedb.UpsertModuleParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), Size: int64(key.Size), Info: info, Data: module.Data, LastAccessed: now, StoredBytes: int64(storedBytes)}); err != nil {
		return false
	}
	if err := queries.DeleteResources(ctx, deleteResourcesParams(key)); err != nil {
		return false
	}
	for _, resource := range resources {
		data := resource.Data
		if data == nil {
			data = []byte{}
		}
		contentLocation := sql.NullString{}
		if resource.ContentLocation != nil {
			contentLocation = sql.NullString{String: *resource.ContentLocation, Valid: true}
		}
		if err := queries.InsertResource(ctx, cachedb.InsertResourceParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), Size: int64(key.Size), ResourceID: resource.ID, ContentLocation: contentLocation, ContentType: resource.ContentType, Data: data}); err != nil {
			return false
		}
	}
	if err := queries.DeleteTombstone(ctx, cachedb.DeleteTombstoneParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version)}); err != nil {
		return false
	}
	if err := tx.Commit(); err != nil {
		return false
	}
	s.prune()
	_, err = s.queries.ModuleExists(ctx, cachedb.ModuleExistsParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), Size: int64(key.Size)})
	return err == nil
}

func (*SQLiteModuleStore) PersistsCompletedModules() {}

func (s *SQLiteModuleStore) GetDecodedResources(key ModuleVersionKey) ([]ModuleResource, bool) {
	if s == nil || s.db == nil {
		return nil, false
	}
	rows, err := s.queries.GetResources(context.Background(), cachedb.GetResourcesParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version)})
	if err != nil {
		return nil, false
	}
	resources := []ModuleResource{}
	var size int64 = -1
	for _, row := range rows {
		if row.Size < 0 || row.Size > int64(^uint32(0)) {
			return nil, false
		}
		if size >= 0 && size != row.Size {
			return nil, false
		}
		size = row.Size
		var contentLocation *string
		if row.ContentLocation.Valid {
			contentLocation = &row.ContentLocation.String
		}
		resources = append(resources, ModuleResource{ID: row.ResourceID, ContentLocation: contentLocation, ContentType: row.ContentType, Data: row.Data})
	}
	if len(resources) == 0 {
		return nil, false
	}
	s.touch(ModuleCacheKey{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: key.ServiceID, ComponentTag: key.ComponentTag, DownloadID: key.DownloadID, ModuleID: key.ModuleID, Version: key.Version, Size: uint32(size)})
	return resources, true
}

func (s *SQLiteModuleStore) WasEvicted(key ModuleVersionKey) bool {
	if s == nil || s.db == nil {
		return false
	}
	_, err := s.queries.WasEvicted(context.Background(), cachedb.WasEvictedParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version)})
	return err == nil
}

func (s *SQLiteModuleStore) prune() {
	ctx := context.Background()
	for {
		if s.maxAge > 0 {
			row, err := s.queries.OldestExpiredModule(ctx, time.Now().Add(-s.maxAge).Unix())
			key, valid := moduleCacheKey(row.ChannelType, row.ChannelID, row.ServiceID, row.ComponentTag, row.DownloadID, row.ModuleID, row.Version, row.Size)
			if err == nil && valid && s.pruneOne(key) {
				continue
			}
		}
		bytes, err := s.queries.TotalStoredBytes(ctx)
		if err != nil || bytes < 0 || uint64(bytes) <= s.maxBytes {
			return
		}
		row, err := s.queries.OldestModule(ctx)
		key, valid := moduleCacheKey(row.ChannelType, row.ChannelID, row.ServiceID, row.ComponentTag, row.DownloadID, row.ModuleID, row.Version, row.Size)
		if err != nil || !valid || !s.pruneOne(key) {
			return
		}
	}
}

func (s *SQLiteModuleStore) pruneOne(key ModuleCacheKey) bool {
	ctx := context.Background()
	if err := s.queries.UpsertTombstone(ctx, cachedb.UpsertTombstoneParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), EvictedAt: time.Now().Unix()}); err != nil {
		return false
	}
	if err := s.queries.DeleteResources(ctx, deleteResourcesParams(key)); err != nil {
		return false
	}
	if err := s.queries.DeleteModule(ctx, cachedb.DeleteModuleParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), Size: int64(key.Size)}); err != nil {
		return false
	}
	_ = s.queries.TrimTombstones(ctx, maxModuleCacheTombstones)
	return true
}

func deleteResourcesParams(key ModuleCacheKey) cachedb.DeleteResourcesParams {
	return cachedb.DeleteResourcesParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), Size: int64(key.Size)}
}

func moduleCacheKey(channelType, channelID string, serviceID, componentTag, downloadID, moduleID, version, size int64) (ModuleCacheKey, bool) {
	if serviceID < 0 || serviceID > int64(^uint16(0)) || componentTag < 0 || componentTag > int64(^byte(0)) || downloadID < 0 || downloadID > int64(^uint32(0)) || moduleID < 0 || moduleID > int64(^uint16(0)) || version < 0 || version > int64(^byte(0)) || size < 0 || size > int64(^uint32(0)) {
		return ModuleCacheKey{}, false
	}
	return ModuleCacheKey{ChannelType: channelType, ChannelID: channelID, ServiceID: uint16(serviceID), ComponentTag: byte(componentTag), DownloadID: uint32(downloadID), ModuleID: uint16(moduleID), Version: byte(version), Size: uint32(size)}, true
}

func (s *SQLiteModuleStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
