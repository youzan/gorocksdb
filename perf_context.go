package gorocksdb

// #include "rocksdb/c.h"
// #include "gorocksdb.h"
import "C"
import "unsafe"

type PerfLevel int

const (
	PerfUnknown                  PerfLevel = iota
	PerfDisable                            = 1
	PerfEnableCount                        = 2
	PerfEnableTimeExceptForMutex           = 3
	PerfEnableTime                         = 4
)

func SetPerfLevel(level PerfLevel) {
	C.rocksdb_set_perf_level(C.int(level))
}

type PerfContext struct {
	c *C.rocksdb_perfcontext_t
}

// refill_period_us default to 100*1000
// fairness default to 10
func NewPerfContext() *PerfContext {
	c := C.rocksdb_perfcontext_create()
	return &PerfContext{c}
}

// Destroy deallocates the BlockBasedTableOptions object.
func (pf *PerfContext) Reset() {
	C.rocksdb_perfcontext_reset(pf.c)
}

func (pf *PerfContext) Report(excludeZero bool) string {
	exclude := C.uchar(0)
	if excludeZero {
		exclude = C.uchar(1)
	}
	cValue := C.rocksdb_perfcontext_report(pf.c, exclude)
	if cValue == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(cValue))
	return C.GoString(cValue)
}

func (pf *PerfContext) Destroy() {
	C.rocksdb_perfcontext_destroy(pf.c)
	pf.c = nil
}
