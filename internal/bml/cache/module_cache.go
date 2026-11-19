package cache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/bml"
	"github.com/21S1298001/mahiron/internal/bml/cache/cachedb"
	"github.com/21S1298001/mahiron/internal/bml/resource"
	mahirondb "github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/ts"
	"golang.org/x/sync/semaphore"
)

// Aliases to the canonical BML store types. The interfaces and keys are
// defined in the parent bml package so the transient Hub can use them
// without importing this persistence package; this package implements them.
type (
	ModuleCacheKey        = bml.ModuleCacheKey
	ModuleVersionKey      = bml.ModuleVersionKey
	ModuleStore           = bml.ModuleStore
	PersistentModuleStore = bml.PersistentModuleStore
	EvictedModuleStore    = bml.EvictedModuleStore
	ModuleExistenceStore  = bml.ModuleExistenceStore
	PersistedService      = bml.PersistedService
	PersistedCarousel     = bml.PersistedCarousel
	SnapshotStore         = bml.SnapshotStore
	ModuleResource        = resource.ModuleResource
)

const (
	DefaultModuleCacheBytes   = 128 * 1024 * 1024
	maxModuleCacheTombstones  = 4096
	moduleCacheTouchInterval  = time.Minute
	moduleCacheTouchQueueSize = 256

	// Free down to a low water mark so a full cache does not evict on every Put.
	moduleCacheEvictTargetPercent = 90

	// A cache under budget can still hold age-expired modules. Checking for
	// them is an index seek rather than a scan, but there is no reason to pay
	// it per module either.
	moduleCacheExpiryCheckInterval = time.Minute
	moduleCachePruneBatchSize      = 64
	moduleCachePruneBatchBytes     = 16 << 20
	moduleCachePruneRetryDelay     = 5 * time.Second
)

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

func (c *ModuleCache) Has(key ModuleCacheKey) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.entries[key]
	return ok
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
	// handle owns the connections backing write/read; Close closes it. The
	// cache is a separate database file from the application's primary one,
	// so it cannot contend with EPG/recording writes, but writers within the
	// cache itself (Put, prune, PutSnapshot, touch) still contend with each
	// other under SQLITE_BUSY if they share the read pool's multiple
	// connections. Routing writes through handle.Write (one connection,
	// BEGIN IMMEDIATE) serializes them the same way the primary database
	// does; reads keep using handle.Read.
	handle         *mahirondb.DB
	write          *sql.DB
	db             *sql.DB
	wq             *cachedb.Queries
	queries        *cachedb.Queries
	maxBytes       uint64
	maxAge         time.Duration
	snapshotMaxAge time.Duration
	touchMu        sync.Mutex
	touched        map[ModuleCacheKey]time.Time
	touchQueue     chan ModuleCacheKey
	touchDone      chan struct{}

	// Serialize module commits with their byte accounting. A prune releases
	// this lock after every batch, allowing module writers to make progress.
	bytesLock         *semaphore.Weighted
	storedBytes       int64
	bytesValid        bool
	pruningToLowWater bool
	pruneWake         chan struct{}
	pruneCancel       context.CancelFunc
	pruneDone         chan struct{}
}

func NewSQLiteModuleStore(path string, maxBytes uint64) (*SQLiteModuleStore, error) {
	return NewSQLiteModuleStoreWithOptions(path, SQLiteModuleStoreOptions{MaxBytes: maxBytes})
}

type SQLiteModuleStoreOptions struct {
	MaxBytes uint64
	// MaxAge removes modules that have not been accessed within the duration.
	// A zero duration keeps entries until they are removed by the byte budget.
	MaxAge time.Duration
	// SnapshotMaxAge removes provisional PMT/DII snapshots older than the
	// duration. Snapshots are not counted against MaxBytes, so a zero
	// duration keeps them indefinitely rather than falling back to a byte
	// budget.
	SnapshotMaxAge time.Duration
}

func NewSQLiteModuleStoreWithOptions(path string, options SQLiteModuleStoreOptions) (*SQLiteModuleStore, error) {
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultModuleCacheBytes
	}
	store, err := openSQLiteModuleStore(path, maxBytes, options.MaxAge, options.SnapshotMaxAge)
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
	return openSQLiteModuleStore(path, maxBytes, options.MaxAge, options.SnapshotMaxAge)
}

func openSQLiteModuleStore(path string, maxBytes uint64, maxAge, snapshotMaxAge time.Duration) (*SQLiteModuleStore, error) {
	handle, err := mahirondb.Open(path)
	if err != nil {
		return nil, err
	}
	db := handle.Read
	store := &SQLiteModuleStore{handle: handle, db: db, write: handle.Write, queries: cachedb.New(db), wq: cachedb.New(handle.Write), maxBytes: maxBytes, maxAge: maxAge, snapshotMaxAge: snapshotMaxAge, touched: map[ModuleCacheKey]time.Time{}, bytesLock: semaphore.NewWeighted(1)}
	if err := cachedb.Migrate(context.Background(), handle.Write); err != nil {
		_ = handle.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	store.pruneWake = make(chan struct{}, 1)
	store.pruneCancel = cancel
	store.pruneDone = make(chan struct{})
	store.pruneWake <- struct{}{}
	store.touchQueue = make(chan ModuleCacheKey, moduleCacheTouchQueueSize)
	store.touchDone = make(chan struct{})
	go store.runTouches(ctx)
	go store.runPrune(ctx)
	store.pruneSnapshots()
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
	return s.GetContext(context.Background(), key)
}

func (s *SQLiteModuleStore) GetContext(ctx context.Context, key ModuleCacheKey) (ts.DSMCCModule, bool) {
	if s == nil || s.db == nil {
		return ts.DSMCCModule{}, false
	}
	row, err := s.queries.GetModule(ctx, cachedb.GetModuleParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), Size: int64(key.Size)})
	if err != nil {
		return ts.DSMCCModule{}, false
	}
	module := ts.DSMCCModule{Info: row.Info, Data: row.Data}
	module.DownloadID, module.ModuleID, module.Version, module.Size = key.DownloadID, key.ModuleID, key.Version, key.Size
	s.touch(key)
	return module, true
}

func (s *SQLiteModuleStore) Has(key ModuleCacheKey) bool {
	if s == nil || s.db == nil {
		return false
	}
	_, err := s.queries.ModuleExists(context.Background(), cachedb.ModuleExistsParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), Size: int64(key.Size)})
	return err == nil
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

// touch coalesces best-effort recency writes without making readers wait for
// the SQLite writer. When full, the next read can try to queue the key again.
func (s *SQLiteModuleStore) touch(key ModuleCacheKey) {
	now := time.Now()
	s.touchMu.Lock()
	defer s.touchMu.Unlock()
	if previous, ok := s.touched[key]; ok && now.Sub(previous) < moduleCacheTouchInterval {
		return
	}
	select {
	case s.touchQueue <- key:
		s.touched[key] = now
	default:
	}
}

func (s *SQLiteModuleStore) runTouches(ctx context.Context) {
	defer close(s.touchDone)
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case key := <-s.touchQueue:
			err := s.writeTouch(ctx, key)
			if err != nil {
				s.touchMu.Lock()
				delete(s.touched, key)
				s.touchMu.Unlock()
			}
		}
	}
}

func (s *SQLiteModuleStore) writeTouch(ctx context.Context, key ModuleCacheKey) error {
	conn, err := s.write.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	// Stamp after acquiring the writer so a delayed update cannot move
	// the timestamp backwards after a newer Put of this key.
	return cachedb.New(conn).TouchModule(ctx, cachedb.TouchModuleParams{LastAccessed: time.Now().Unix(), ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), Size: int64(key.Size)})
}

func (s *SQLiteModuleStore) Put(key ModuleCacheKey, module ts.DSMCCModule) bool {
	return s.PutModules(context.Background(), []ModuleCacheKey{key}, module)
}

// PutModules decodes the common payload once and writes all service keys in
// one transaction. Every service keeps its own immutable cache URL.
func (s *SQLiteModuleStore) PutModules(ctx context.Context, keys []ModuleCacheKey, module ts.DSMCCModule) bool {
	if s == nil || s.db == nil || len(keys) == 0 || ctx.Err() != nil || uint64(len(module.Data)) > s.maxBytes {
		return false
	}
	resources, _ := resource.DecodeModuleResources(bml.Module{Info: module.Info, Data: module.Data})
	storedBytes := uint64(len(module.Data))
	for _, resource := range resources {
		storedBytes += uint64(len(resource.Data))
	}
	if storedBytes > s.maxBytes || storedBytes > uint64(^uint64(0)>>1) {
		return false
	}
	if err := s.bytesLock.Acquire(ctx, 1); err != nil {
		return false
	}
	defer s.bytesLock.Release(1)
	if !s.bytesValid {
		if err := s.refreshStoredBytes(ctx); err != nil {
			return false
		}
	}
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return false
	}
	defer func() { _ = tx.Rollback() }()
	queries := s.wq.WithTx(tx)
	var delta int64
	for _, key := range keys {
		previous, err := putModuleRows(ctx, queries, key, module, resources, int64(storedBytes))
		if err != nil {
			return false
		}
		delta += int64(storedBytes) - previous
	}
	if err := tx.Commit(); err != nil {
		s.bytesValid = false
		return false
	}
	s.storedBytes += delta
	if s.storedBytes > s.cappedMaxBytes() || s.pruningToLowWater {
		select {
		case s.pruneWake <- struct{}{}:
		default:
		}
		// The row is committed, but this pass may evict it. Keep the live
		// carousel payload until the cache can be relied on to retain it.
		return false
	}
	return true
}

func putModuleRows(ctx context.Context, queries *cachedb.Queries, key ModuleCacheKey, module ts.DSMCCModule, resources []ModuleResource, storedBytes int64) (int64, error) {
	// A Put may replace an existing module, so only the difference counts
	// towards the budget. This is a primary key lookup, unlike the cache-wide
	// sum it replaces.
	previousBytes, err := queries.GetStoredBytes(ctx, cachedb.GetStoredBytesParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), Size: int64(key.Size)})
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		previousBytes = 0
	}
	// A module may legitimately have no module-info bytes. SQLite maps a nil
	// slice to NULL, while the cache schema intentionally requires BLOB values.
	info := module.Info
	if info == nil {
		info = []byte{}
	}
	now := time.Now().Unix()
	if err := queries.UpsertModule(ctx, cachedb.UpsertModuleParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), Size: int64(key.Size), Info: info, Data: module.Data, LastAccessed: now, StoredBytes: storedBytes}); err != nil {
		return 0, err
	}
	if err := queries.DeleteResources(ctx, deleteResourcesParams(key)); err != nil {
		return 0, err
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
			return 0, err
		}
	}
	if err := queries.DeleteTombstone(ctx, cachedb.DeleteTombstoneParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version)}); err != nil {
		return 0, err
	}
	return previousBytes, nil
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

// A module row stores its payload inline, so any plan that scans
// data_broadcast_modules walks the blob overflow pages of the entire cache: on
// a full 1 GiB cache that is a gigabyte of reads, which prune() would pay on
// every startup and on every Put. Each statement below therefore reads modules
// only through data_broadcast_modules_prune, which covers the byte accounting,
// the age filter and the eviction order. TestSQLitePruneStatementsAvoidTableScans
// keeps them that way.
const (
	pruneStoredBytesQuery = `SELECT CAST(COALESCE(SUM(stored_bytes), 0) AS INTEGER) FROM data_broadcast_modules`

	pruneCreateCandidates = `CREATE TEMP TABLE IF NOT EXISTS data_broadcast_prune_candidates (
		channel_type TEXT NOT NULL, channel_id TEXT NOT NULL, service_id INTEGER NOT NULL,
		component_tag INTEGER NOT NULL, download_id INTEGER NOT NULL, module_id INTEGER NOT NULL,
		version INTEGER NOT NULL, size INTEGER NOT NULL, stored_bytes INTEGER NOT NULL,
		PRIMARY KEY (channel_type, channel_id, service_id, component_tag, download_id, module_id, version, size)
	)`

	pruneClearCandidates = `DELETE FROM data_broadcast_prune_candidates`

	// Scan the oldest entries through the covering index. The window runs
	// over at most one batch and excludes the current row so the final
	// selected module can take the cache below the byte target.
	pruneCollectBatch = `
		WITH oldest AS (
			SELECT channel_type, channel_id, service_id, component_tag, download_id,
				module_id, version, size, last_accessed, stored_bytes
			FROM data_broadcast_modules
			ORDER BY last_accessed, channel_type, channel_id, service_id, component_tag,
				download_id, module_id, version, size LIMIT ?
		), ranked AS (
			SELECT *, SUM(stored_bytes) OVER (
				ORDER BY last_accessed, channel_type, channel_id, service_id, component_tag,
					download_id, module_id, version, size
				ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
			) AS older_bytes FROM oldest
		)
		INSERT INTO data_broadcast_prune_candidates
		SELECT channel_type, channel_id, service_id, component_tag, download_id,
			module_id, version, size, stored_bytes
		FROM ranked WHERE COALESCE(older_bytes, 0) < ?
			AND (last_accessed < ? OR COALESCE(older_bytes, 0) < ?)`

	pruneInsertTombstones = `INSERT OR REPLACE INTO data_broadcast_module_tombstones
		SELECT channel_type, channel_id, service_id, component_tag, download_id, module_id, version, ?
		FROM data_broadcast_prune_candidates GROUP BY channel_type, channel_id, service_id, component_tag, download_id, module_id, version`

	// Both deletes are driven by the candidate set through a row value so
	// SQLite searches the target primary key once per evicted module. An EXISTS
	// correlated on the target scans it instead, costing a full pass over the
	// cache even when a single module is evicted.
	pruneDeleteResources = `DELETE FROM data_broadcast_resources WHERE
		(channel_type, channel_id, service_id, component_tag, download_id, module_id, version, size) IN (
		SELECT channel_type, channel_id, service_id, component_tag, download_id, module_id, version, size
		FROM data_broadcast_prune_candidates)`

	pruneDeleteModules = `DELETE FROM data_broadcast_modules WHERE
		(channel_type, channel_id, service_id, component_tag, download_id, module_id, version, size) IN (
		SELECT channel_type, channel_id, service_id, component_tag, download_id, module_id, version, size
		FROM data_broadcast_prune_candidates)`
)

// evictTargetBytes is the size the eviction pass frees down to. Scaling before
// dividing keeps the mark meaningful for the byte-sized budgets the tests use,
// where dividing first would floor it to zero and evict the whole cache.
func evictTargetBytes(maxBytes int64) int64 {
	if maxBytes <= 0 {
		return 0
	}
	if maxBytes <= math.MaxInt64/moduleCacheEvictTargetPercent {
		return maxBytes * moduleCacheEvictTargetPercent / 100
	}
	return maxBytes / 100 * moduleCacheEvictTargetPercent
}

func (s *SQLiteModuleStore) cappedMaxBytes() int64 {
	if s.maxBytes > uint64(^uint64(0)>>1) {
		return int64(^uint64(0) >> 1)
	}
	return int64(s.maxBytes)
}

// runPrune coalesces Put notifications and periodically checks expiry even
// when no new modules arrive. Failures retry after a short backoff.
func (s *SQLiteModuleStore) runPrune(ctx context.Context) {
	defer close(s.pruneDone)
	ticker := time.NewTicker(moduleCacheExpiryCheckInterval)
	defer ticker.Stop()
	var retry <-chan time.Time
	initial := true
	for {
		checkExpiry := initial
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.pruneSnapshotsContext(ctx)
			s.touchMu.Lock()
			maps.DeleteFunc(s.touched, func(_ ModuleCacheKey, last time.Time) bool {
				return now.Sub(last) >= moduleCacheTouchInterval
			})
			s.touchMu.Unlock()
			if retry != nil {
				continue
			}
			checkExpiry = true
		case <-retry:
			retry = nil
			checkExpiry = true
		case <-s.pruneWake:
			if retry != nil {
				continue
			}
		}
		initial = false
		if err := s.prune(ctx, checkExpiry); err != nil && ctx.Err() == nil {
			slog.Warn("failed to prune data broadcast cache", "err", err)
			retry = time.After(moduleCachePruneRetryDelay)
		}
	}
}

// refreshStoredBytes reconciles the counter while bytesLock excludes writes
// from this store. The covering index keeps module payloads out of the scan.
func (s *SQLiteModuleStore) refreshStoredBytes(ctx context.Context) error {
	err := s.db.QueryRowContext(ctx, pruneStoredBytesQuery).Scan(&s.storedBytes)
	s.bytesValid = err == nil
	return err
}

func (s *SQLiteModuleStore) prune(ctx context.Context, checkExpiry bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.bytesLock.Acquire(ctx, 1); err != nil {
		return err
	}
	if !s.bytesValid || checkExpiry {
		if err := s.refreshStoredBytes(ctx); err != nil {
			s.bytesLock.Release(1)
			return fmt.Errorf("count stored bytes: %w", err)
		}
	}
	maxBytes := s.cappedMaxBytes()
	lowWater := evictTargetBytes(maxBytes)
	s.pruningToLowWater = s.storedBytes > maxBytes || (s.pruningToLowWater && s.storedBytes > lowWater)
	evict := s.pruningToLowWater
	s.bytesLock.Release(1)
	if !evict && (!checkExpiry || s.maxAge <= 0) {
		return nil
	}

	target := maxBytes
	if evict {
		target = lowWater
	}
	expiredBefore := int64(math.MinInt64)
	if checkExpiry && s.maxAge > 0 {
		expiredBefore = time.Now().Add(-s.maxAge).Unix()
	}
	for {
		more, err := s.pruneBatch(ctx, target, expiredBefore)
		if err != nil || !more {
			return err
		}
	}
}

func (s *SQLiteModuleStore) pruneBatch(ctx context.Context, target, expiredBefore int64) (bool, error) {
	if err := s.bytesLock.Acquire(ctx, 1); err != nil {
		return false, err
	}
	defer s.bytesLock.Release(1)
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin batch: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, step := range []struct {
		name, query string
		args        []any
	}{
		{"create candidates", pruneCreateCandidates, nil},
		{"clear candidates", pruneClearCandidates, nil},
		{"collect candidates", pruneCollectBatch, []any{moduleCachePruneBatchSize, moduleCachePruneBatchBytes, expiredBefore, s.storedBytes - target}},
	} {
		if _, err := tx.ExecContext(ctx, step.query, step.args...); err != nil {
			return false, fmt.Errorf("%s: %w", step.name, err)
		}
	}

	var count int
	var removedBytes int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(stored_bytes), 0) FROM data_broadcast_prune_candidates`).Scan(&count, &removedBytes); err != nil {
		return false, fmt.Errorf("count candidates: %w", err)
	}
	if count == 0 {
		return false, nil
	}
	for _, step := range []struct {
		name, query string
		args        []any
	}{
		{"insert tombstones", pruneInsertTombstones, []any{time.Now().Unix()}},
		{"delete resources", pruneDeleteResources, nil},
		{"delete modules", pruneDeleteModules, nil},
	} {
		if _, err := tx.ExecContext(ctx, step.query, step.args...); err != nil {
			return false, fmt.Errorf("%s: %w", step.name, err)
		}
	}
	if err := s.wq.WithTx(tx).TrimTombstones(ctx, maxModuleCacheTombstones); err != nil {
		return false, fmt.Errorf("trim tombstones: %w", err)
	}
	if err := tx.Commit(); err != nil {
		s.bytesValid = false
		return false, fmt.Errorf("commit batch: %w", err)
	}
	s.storedBytes -= removedBytes
	if s.storedBytes <= evictTargetBytes(s.cappedMaxBytes()) {
		s.pruningToLowWater = false
	}
	return count == moduleCachePruneBatchSize || removedBytes >= moduleCachePruneBatchBytes, nil
}

func deleteResourcesParams(key ModuleCacheKey) cachedb.DeleteResourcesParams {
	return cachedb.DeleteResourcesParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), Size: int64(key.Size)}
}

// PutSnapshot persists the raw PMT and per-component DII sections needed to
// reconstruct a provisional snapshot. Components no longer present in the new
// PMT are removed so a stale carousel cannot outlive the component it
// belonged to.
func (s *SQLiteModuleStore) PutSnapshot(channelType, channelID string, service PersistedService) error {
	return s.PutSnapshotContext(context.Background(), channelType, channelID, service)
}

func (s *SQLiteModuleStore) PutSnapshotContext(ctx context.Context, channelType, channelID string, service PersistedService) error {
	if s == nil || s.db == nil {
		return nil
	}
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	queries := s.wq.WithTx(tx)
	now := time.Now().Unix()
	// SQLite maps a nil slice to NULL, while the cache schema requires BLOB
	// values (see the same normalization for module Info in Put).
	pmtSection := service.PMTSection
	if pmtSection == nil {
		pmtSection = []byte{}
	}
	if err := queries.UpsertSnapshot(ctx, cachedb.UpsertSnapshotParams{ChannelType: channelType, ChannelID: channelID, ServiceID: int64(service.ServiceID), PmtSection: pmtSection, StoredAt: now}); err != nil {
		return err
	}
	existingTags, err := queries.ListSnapshotCarouselComponentTags(ctx, cachedb.ListSnapshotCarouselComponentTagsParams{ChannelType: channelType, ChannelID: channelID, ServiceID: int64(service.ServiceID)})
	if err != nil {
		return err
	}
	keep := make(map[int64]bool, len(service.Carousels))
	for _, carousel := range service.Carousels {
		keep[int64(carousel.ComponentTag)] = true
		diiSection := carousel.DIISection
		if diiSection == nil {
			diiSection = []byte{}
		}
		if err := queries.UpsertSnapshotCarousel(ctx, cachedb.UpsertSnapshotCarouselParams{ChannelType: channelType, ChannelID: channelID, ServiceID: int64(service.ServiceID), ComponentTag: int64(carousel.ComponentTag), Pid: int64(carousel.PID), DiiSection: diiSection, StoredAt: now}); err != nil {
			return err
		}
	}
	for _, tag := range existingTags {
		if keep[tag] {
			continue
		}
		if err := queries.DeleteSnapshotCarousel(ctx, cachedb.DeleteSnapshotCarouselParams{ChannelType: channelType, ChannelID: channelID, ServiceID: int64(service.ServiceID), ComponentTag: tag}); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

// GetSnapshot returns the persisted PMT/DII sections for a service, if any.
func (s *SQLiteModuleStore) GetSnapshot(channelType, channelID string, serviceID uint16) (PersistedService, bool) {
	if s == nil || s.db == nil {
		return PersistedService{}, false
	}
	ctx := context.Background()
	row, err := s.queries.GetSnapshot(ctx, cachedb.GetSnapshotParams{ChannelType: channelType, ChannelID: channelID, ServiceID: int64(serviceID)})
	if err != nil {
		return PersistedService{}, false
	}
	rows, err := s.queries.GetSnapshotCarousels(ctx, cachedb.GetSnapshotCarouselsParams{ChannelType: channelType, ChannelID: channelID, ServiceID: int64(serviceID)})
	if err != nil {
		return PersistedService{}, false
	}
	carousels := make([]PersistedCarousel, 0, len(rows))
	for _, carouselRow := range rows {
		if carouselRow.ComponentTag < 0 || carouselRow.ComponentTag > int64(^byte(0)) || carouselRow.Pid < 0 || carouselRow.Pid > int64(^uint16(0)) {
			continue
		}
		carousels = append(carousels, PersistedCarousel{ComponentTag: byte(carouselRow.ComponentTag), PID: uint16(carouselRow.Pid), DIISection: carouselRow.DiiSection})
	}
	return PersistedService{ServiceID: serviceID, PMTSection: row.PmtSection, Carousels: carousels, StoredAt: row.StoredAt}, true
}

func (s *SQLiteModuleStore) pruneSnapshots() {
	s.pruneSnapshotsContext(context.Background())
}

func (s *SQLiteModuleStore) pruneSnapshotsContext(ctx context.Context) {
	if s == nil || s.db == nil || s.snapshotMaxAge <= 0 {
		return
	}
	cutoff := time.Now().Add(-s.snapshotMaxAge).Unix()
	_ = s.wq.DeleteExpiredSnapshots(ctx, cutoff)
	_ = s.wq.DeleteExpiredSnapshotCarousels(ctx, cutoff)
}

func (s *SQLiteModuleStore) Close() error {
	if s == nil || s.handle == nil {
		return nil
	}
	s.pruneCancel()
	<-s.pruneDone
	<-s.touchDone
	return s.handle.Close()
}
