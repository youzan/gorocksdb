package gorocksdb

// #include "rocksdb/c.h"
// #include "gorocksdb.h"
import "C"

type RateLimiter struct {
	//c *C.rocksdb_rate_limiter_t
}

// TODO: wait merge from rocksdb
func NewGenericRateLimiter(bytes_per_sec int64) *RateLimiter {
	//c := C.rocksdb_ratelimiter_create(C.int64_t(bytes_per_sec), C.int64_t(100*1000), C.int32_t(10))
	return &RateLimiter{}
}

// Destroy deallocates the BlockBasedTableOptions object.
func (r *RateLimiter) Destroy() {
	//C.rocksdb_ratelimiter_destroy(r.c)
	//r.c = nil
}
