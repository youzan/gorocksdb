package gorocksdb

// #include "rocksdb/c.h"
// #include "gorocksdb.h"
import "C"

type RateLimiter struct {
	c *C.rocksdb_ratelimiter_t
}

// refill_period_us default to 100*1000
// fairness default to 10
func NewGenericRateLimiter(bytes_per_sec, refill_period_us int64, fairness int32) *RateLimiter {
	c := C.rocksdb_ratelimiter_create(
		C.int64_t(bytes_per_sec),
		C.int64_t(refill_period_us),
		C.int32_t(fairness),
	)
	return &RateLimiter{c}
}

func (r *RateLimiter) SetBytesPerSecond(bytes_per_sec int64) {
	if bytes_per_sec <= 0 || r.c == nil {
		return
	}
	C.rocksdb_options_set_ratelimiter_bytes_per_second(r.c, C.int64_t(bytes_per_sec))
}

// Destroy deallocates the BlockBasedTableOptions object.
func (r *RateLimiter) Destroy() {
	C.rocksdb_ratelimiter_destroy(r.c)
	r.c = nil
}
