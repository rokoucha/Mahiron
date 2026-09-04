package databroadcast

import (
	"context"
	"database/sql"
	"errors"
	"math"
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

	// The eviction pass is O(cache size). Evicting only back to the budget
	// puts the cache one module over it again on the very next Put, so a full
	// cache paid that pass for every module it stored. Freeing down to a low
	// water mark instead amortizes one pass over the modules that fit in the
	// gap.
	moduleCacheEvictTargetPercent = 90

	// A cache under budget can still hold age-expired modules. Checking for
	// them is an index seek rather than a scan, but there is no reason to pay
	// it per module either.
	moduleCacheExpiryCheckInterval = time.Minute
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

// ModuleExistenceStore reports whether a completed module is retained without
// reading its payload. RestoreSnapshot uses this to mark modules complete in a
// provisional snapshot without paying for a full module read per module.
type ModuleExistenceStore interface {
	Has(ModuleCacheKey) bool
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
	// handle owns the connections backing db; Close closes it. The module
	// cache is a separate database from the application's primary one and
	// does not need the read/write connection split that database uses to
	// avoid blocking API reads — db is simply its read pool, sized for
	// concurrent access from both reads and the occasional write.
	handle         *mahirondb.DB
	db             *sql.DB
	queries        *cachedb.Queries
	maxBytes       uint64
	maxAge         time.Duration
	snapshotMaxAge time.Duration
	touchMu        sync.Mutex
	touched        map[ModuleCacheKey]time.Time

	// storedBytes tracks the cache size between eviction passes so Put can
	// decide whether a pass is needed without summing the cache first. It is
	// reset from the database whenever a pass runs, so a drifted count is
	// corrected rather than accumulated.
	bytesMu         sync.Mutex
	storedBytes     int64
	bytesValid      bool
	lastExpiryCheck time.Time
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
	handle, err := mahirondb.OpenCache(path)
	if err != nil {
		return nil, err
	}
	db := handle.Read
	store := &SQLiteModuleStore{handle: handle, db: db, queries: cachedb.New(db), maxBytes: maxBytes, maxAge: maxAge, snapshotMaxAge: snapshotMaxAge, touched: map[ModuleCacheKey]time.Time{}}
	if err := cachedb.Migrate(context.Background(), handle.Write); err != nil {
		_ = handle.Close()
		return nil, err
	}
	store.prune()
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
	// A Put may replace an existing module, so only the difference counts
	// towards the budget. This is a primary key lookup, unlike the cache-wide
	// sum it replaces.
	previousBytes, err := queries.GetStoredBytes(ctx, cachedb.GetStoredBytesParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), Size: int64(key.Size)})
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return false
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
	s.noteStoredBytes(int64(storedBytes) - previousBytes)
	s.maybePrune()
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

// A module row stores its payload inline, so any plan that scans
// data_broadcast_modules walks the blob overflow pages of the entire cache: on
// a full 1 GiB cache that is a gigabyte of reads, which prune() would pay on
// every startup and on every Put. Each statement below therefore reads modules
// only through data_broadcast_modules_prune, which covers the byte accounting,
// the age filter and the eviction order. TestSQLitePruneStatementsAvoidTableScans
// keeps them that way.
const (
	pruneStoredBytesQuery = `SELECT CAST(COALESCE(SUM(stored_bytes), 0) AS INTEGER) FROM data_broadcast_modules`

	pruneHasExpiredQuery = `SELECT EXISTS(SELECT 1 FROM data_broadcast_modules WHERE last_accessed < ?)`

	pruneCreateCandidates = `CREATE TEMP TABLE IF NOT EXISTS data_broadcast_prune_candidates (
		channel_type TEXT NOT NULL, channel_id TEXT NOT NULL, service_id INTEGER NOT NULL,
		component_tag INTEGER NOT NULL, download_id INTEGER NOT NULL, module_id INTEGER NOT NULL,
		version INTEGER NOT NULL, size INTEGER NOT NULL,
		PRIMARY KEY (channel_type, channel_id, service_id, component_tag, download_id, module_id, version, size)
	)`

	pruneClearCandidates = `DELETE FROM data_broadcast_prune_candidates`

	pruneCollectExpired = `
		INSERT OR IGNORE INTO data_broadcast_prune_candidates
		SELECT channel_type, channel_id, service_id, component_tag, download_id, module_id, version, size
		FROM data_broadcast_modules WHERE last_accessed < ?`

	// Retain as many of the newest modules as fit. Everything beyond the byte
	// budget joins the same eviction set as age-expired modules. The window
	// ordering matches the index, so no sort of the whole cache is needed.
	pruneCollectOverBudget = `
		WITH retained AS (
			SELECT m.channel_type, m.channel_id, m.service_id, m.component_tag, m.download_id,
				m.module_id, m.version, m.size, m.last_accessed, m.stored_bytes
			FROM data_broadcast_modules m
			WHERE NOT EXISTS (
				SELECT 1 FROM data_broadcast_prune_candidates c
				WHERE c.channel_type=m.channel_type AND c.channel_id=m.channel_id AND c.service_id=m.service_id
				AND c.component_tag=m.component_tag AND c.download_id=m.download_id AND c.module_id=m.module_id
				AND c.version=m.version AND c.size=m.size
			)
		), ranked AS (
			SELECT channel_type, channel_id, service_id, component_tag, download_id, module_id, version, size,
				SUM(stored_bytes) OVER (ORDER BY last_accessed DESC, channel_type DESC, channel_id DESC, service_id DESC,
					component_tag DESC, download_id DESC, module_id DESC, version DESC, size DESC) AS retained_bytes
			FROM retained
		)
		INSERT OR IGNORE INTO data_broadcast_prune_candidates
		SELECT channel_type, channel_id, service_id, component_tag, download_id, module_id, version, size
		FROM ranked WHERE retained_bytes > ?`

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

// noteStoredBytes applies a Put's net change to the tracked cache size. It is a
// no-op while the count is unknown, so a failed refresh degrades into running
// the eviction pass rather than into an incorrect size.
func (s *SQLiteModuleStore) noteStoredBytes(delta int64) {
	s.bytesMu.Lock()
	defer s.bytesMu.Unlock()
	if !s.bytesValid {
		return
	}
	s.storedBytes += delta
	if s.storedBytes < 0 {
		s.storedBytes = 0
	}
}

// maybePrune runs the eviction pass only when it can do useful work: the cache
// is over budget, the age check is due, or the size is unknown. Running the
// pass unconditionally made every Put cost a walk of the whole cache.
func (s *SQLiteModuleStore) maybePrune() {
	s.bytesMu.Lock()
	valid, stored := s.bytesValid, s.storedBytes
	expiryDue := s.maxAge > 0 && time.Since(s.lastExpiryCheck) >= moduleCacheExpiryCheckInterval
	if expiryDue {
		s.lastExpiryCheck = time.Now()
	}
	s.bytesMu.Unlock()

	if valid && stored <= s.cappedMaxBytes() && !expiryDue {
		return
	}
	s.prune()
}

func (s *SQLiteModuleStore) prune() {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback() }()

	maxBytes := s.cappedMaxBytes()
	var storedBytes int64
	if err := tx.QueryRowContext(ctx, pruneStoredBytesQuery).Scan(&storedBytes); err != nil {
		return
	}
	s.bytesMu.Lock()
	s.storedBytes, s.bytesValid = storedBytes, true
	s.lastExpiryCheck = time.Now()
	s.bytesMu.Unlock()
	expiredBefore := time.Now().Add(-s.maxAge).Unix()
	hasExpired := false
	if s.maxAge > 0 {
		if err := tx.QueryRowContext(ctx, pruneHasExpiredQuery, expiredBefore).Scan(&hasExpired); err != nil {
			return
		}
	}
	if !hasExpired && storedBytes >= 0 && storedBytes <= maxBytes {
		_ = tx.Commit()
		return
	}

	// Materialize the eviction set once. The old implementation selected and
	// deleted one module at a time, causing several SQLite commits per module
	// during startup. Besides being slow, repeatedly calculating SUM over the
	// remaining cache made large caches increasingly expensive to prune.
	if _, err := tx.ExecContext(ctx, pruneCreateCandidates); err != nil {
		return
	}
	if _, err := tx.ExecContext(ctx, pruneClearCandidates); err != nil {
		return
	}
	if s.maxAge > 0 {
		if _, err := tx.ExecContext(ctx, pruneCollectExpired, expiredBefore); err != nil {
			return
		}
	}
	// Free down to the low water mark, not merely back to the budget, so the
	// next pass is due only after the freed gap has been refilled.
	if _, err := tx.ExecContext(ctx, pruneCollectOverBudget, evictTargetBytes(maxBytes)); err != nil {
		return
	}

	now := time.Now().Unix()
	statements := []struct {
		query string
		args  []any
	}{
		{pruneInsertTombstones, []any{now}},
		{pruneDeleteResources, nil},
		{pruneDeleteModules, nil},
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement.query, statement.args...); err != nil {
			return
		}
	}
	if err := s.queries.WithTx(tx).TrimTombstones(ctx, maxModuleCacheTombstones); err != nil {
		return
	}
	var remaining int64
	if err := tx.QueryRowContext(ctx, pruneStoredBytesQuery).Scan(&remaining); err != nil {
		return
	}
	if err := tx.Commit(); err != nil {
		return
	}
	s.bytesMu.Lock()
	s.storedBytes, s.bytesValid = remaining, true
	s.bytesMu.Unlock()
}

func deleteResourcesParams(key ModuleCacheKey) cachedb.DeleteResourcesParams {
	return cachedb.DeleteResourcesParams{ChannelType: key.ChannelType, ChannelID: key.ChannelID, ServiceID: int64(key.ServiceID), ComponentTag: int64(key.ComponentTag), DownloadID: int64(key.DownloadID), ModuleID: int64(key.ModuleID), Version: int64(key.Version), Size: int64(key.Size)}
}

// PutSnapshot persists the raw PMT and per-component DII sections needed to
// reconstruct a provisional snapshot. Components no longer present in the new
// PMT are removed so a stale carousel cannot outlive the component it
// belonged to.
func (s *SQLiteModuleStore) PutSnapshot(channelType, channelID string, service PersistedService) error {
	if s == nil || s.db == nil {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	queries := s.queries.WithTx(tx)
	ctx := context.Background()
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
	s.pruneSnapshots()
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
	if s == nil || s.db == nil || s.snapshotMaxAge <= 0 {
		return
	}
	cutoff := time.Now().Add(-s.snapshotMaxAge).Unix()
	ctx := context.Background()
	_ = s.queries.DeleteExpiredSnapshots(ctx, cutoff)
	_ = s.queries.DeleteExpiredSnapshotCarousels(ctx, cutoff)
}

func (s *SQLiteModuleStore) Close() error {
	if s == nil || s.handle == nil {
		return nil
	}
	return s.handle.Close()
}
