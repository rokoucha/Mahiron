package databroadcast

import (
	"testing"

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
