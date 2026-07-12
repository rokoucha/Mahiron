package databroadcast

import (
	"sync"

	"github.com/21S1298001/mahiron/ts"
)

const DefaultModuleCacheBytes = 128 * 1024 * 1024

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

func (c *ModuleCache) Put(key ModuleCacheKey, module ts.DSMCCModule) {
	if c == nil || uint64(len(module.Data)) > c.maxBytes {
		return
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
}

func cloneCachedModule(module ts.DSMCCModule) ts.DSMCCModule {
	module.Info = append([]byte(nil), module.Info...)
	module.Data = append([]byte(nil), module.Data...)
	return module
}
