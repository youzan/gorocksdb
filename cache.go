package gorocksdb

// #include "rocksdb/c.h"
import "C"

// Cache is a cache used to store data read from data in memory.
type Cache struct {
	c *C.rocksdb_cache_t
}

// NewLRUCache creates a new LRU Cache object with the capacity given.
func NewLRUCache(capacity int) *Cache {
	return NewNativeCache(C.rocksdb_cache_create_lru(C.size_t(capacity)))
}

// NewNativeCache creates a Cache object.
func NewNativeCache(c *C.rocksdb_cache_t) *Cache {
	return &Cache{c}
}

// Destroy deallocates the Cache object.
func (c *Cache) Destroy() {
	C.rocksdb_cache_destroy(c.c)
	c.c = nil
}

func (c *Cache) GetUsage() int64 {
	return 0
	//return int64(C.rocksdb_cache_get_usage(c.c))
}

func (c *Cache) GetPinnedUsage() int64 {
	return 0
	//return int64(C.rocksdb_cache_get_pinned_usage(c.c))
}
