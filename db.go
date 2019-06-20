package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Range is a range of keys in the database. GetApproximateSizes calls with it
// begin at the key Start and end right before the key Limit.
type Range struct {
	Start []byte
	Limit []byte
}

var errDBClosed = errors.New("db engine closed")

// DB is a reusable handle to a RocksDB database on disk, created by Open.
type DB struct {
	// lock protect the read from closed engine
	// for snapshot, iterator, should call rlock by caller
	sync.RWMutex
	c      *C.rocksdb_t
	name   string
	opts   *Options
	opened int32
}

// OpenDb opens a database with the specified options.
func OpenDb(opts *Options, name string) (*DB, error) {
	var (
		cErr  *C.char
		cName = C.CString(name)
	)
	defer C.free(unsafe.Pointer(cName))
	db := C.rocksdb_open(opts.c, cName, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return &DB{
		name:   name,
		c:      db,
		opts:   opts,
		opened: int32(1),
	}, nil
}

// OpenDbForReadOnly opens a database with the specified options for readonly usage.
func OpenDbForReadOnly(opts *Options, name string, errorIfLogFileExist bool) (*DB, error) {
	var (
		cErr  *C.char
		cName = C.CString(name)
	)
	defer C.free(unsafe.Pointer(cName))
	db := C.rocksdb_open_for_read_only(opts.c, cName, boolToChar(errorIfLogFileExist), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return &DB{
		name:   name,
		c:      db,
		opts:   opts,
		opened: int32(1),
	}, nil
}

// OpenDbColumnFamilies opens a database with the specified column families.
func OpenDbColumnFamilies(
	opts *Options,
	name string,
	cfNames []string,
	cfOpts []*Options,
) (*DB, []*ColumnFamilyHandle, error) {
	numColumnFamilies := len(cfNames)
	if numColumnFamilies != len(cfOpts) {
		return nil, nil, errors.New("must provide the same number of column family names and options")
	}

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	cNames := make([]*C.char, numColumnFamilies)
	for i, s := range cfNames {
		cNames[i] = C.CString(s)
	}
	defer func() {
		for _, s := range cNames {
			C.free(unsafe.Pointer(s))
		}
	}()

	cOpts := make([]*C.rocksdb_options_t, numColumnFamilies)
	for i, o := range cfOpts {
		cOpts[i] = o.c
	}

	cHandles := make([]*C.rocksdb_column_family_handle_t, numColumnFamilies)

	var cErr *C.char
	db := C.rocksdb_open_column_families(
		opts.c,
		cName,
		C.int(numColumnFamilies),
		&cNames[0],
		&cOpts[0],
		&cHandles[0],
		&cErr,
	)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, nil, errors.New(C.GoString(cErr))
	}

	cfHandles := make([]*ColumnFamilyHandle, numColumnFamilies)
	for i, c := range cHandles {
		cfHandles[i] = NewNativeColumnFamilyHandle(c)
	}

	return &DB{
		name:   name,
		c:      db,
		opts:   opts,
		opened: int32(1),
	}, cfHandles, nil
}

// OpenDbForReadOnlyColumnFamilies opens a database with the specified column
// families in read only mode.
func OpenDbForReadOnlyColumnFamilies(
	opts *Options,
	name string,
	cfNames []string,
	cfOpts []*Options,
	errorIfLogFileExist bool,
) (*DB, []*ColumnFamilyHandle, error) {
	numColumnFamilies := len(cfNames)
	if numColumnFamilies != len(cfOpts) {
		return nil, nil, errors.New("must provide the same number of column family names and options")
	}

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	cNames := make([]*C.char, numColumnFamilies)
	for i, s := range cfNames {
		cNames[i] = C.CString(s)
	}
	defer func() {
		for _, s := range cNames {
			C.free(unsafe.Pointer(s))
		}
	}()

	cOpts := make([]*C.rocksdb_options_t, numColumnFamilies)
	for i, o := range cfOpts {
		cOpts[i] = o.c
	}

	cHandles := make([]*C.rocksdb_column_family_handle_t, numColumnFamilies)

	var cErr *C.char
	db := C.rocksdb_open_for_read_only_column_families(
		opts.c,
		cName,
		C.int(numColumnFamilies),
		&cNames[0],
		&cOpts[0],
		&cHandles[0],
		boolToChar(errorIfLogFileExist),
		&cErr,
	)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, nil, errors.New(C.GoString(cErr))
	}

	cfHandles := make([]*ColumnFamilyHandle, numColumnFamilies)
	for i, c := range cHandles {
		cfHandles[i] = NewNativeColumnFamilyHandle(c)
	}

	return &DB{
		name:   name,
		c:      db,
		opts:   opts,
		opened: int32(1),
	}, cfHandles, nil
}

// ListColumnFamilies lists the names of the column families in the DB.
func ListColumnFamilies(opts *Options, name string) ([]string, error) {
	var (
		cErr  *C.char
		cLen  C.size_t
		cName = C.CString(name)
	)
	defer C.free(unsafe.Pointer(cName))
	cNames := C.rocksdb_list_column_families(opts.c, cName, &cLen, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	namesLen := int(cLen)
	names := make([]string, namesLen)
	cNamesArr := (*[1 << 30]*C.char)(unsafe.Pointer(cNames))[:namesLen:namesLen]
	for i, n := range cNamesArr {
		names[i] = C.GoString(n)
	}
	C.rocksdb_list_column_families_destroy(cNames, cLen)
	return names, nil
}

// UnsafeGetDB returns the underlying c rocksdb instance.
func (db *DB) UnsafeGetDB() unsafe.Pointer {
	return unsafe.Pointer(db.c)
}

// Name returns the name of the database.
func (db *DB) Name() string {
	return db.name
}

func (db *DB) IsOpened() bool {
	return atomic.LoadInt32(&db.opened) != 0
}

// Get returns the data associated with the key from the database.
func (db *DB) Get(opts *ReadOptions, key []byte) (*Slice, error) {
	var (
		cErr    *C.char
		cValLen C.size_t
		cKey    = byteToChar(key)
	)
	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return nil, errDBClosed
	}
	cValue := C.rocksdb_get(db.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
	db.RUnlock()
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return NewSlice(cValue, cValLen), nil
}

func (db *DB) GetBytesNoLock(opts *ReadOptions, key []byte) ([]byte, error) {
	var (
		cErr    *C.char
		cValLen C.size_t
		cKey    = byteToChar(key)
	)
	if atomic.LoadInt32(&db.opened) == 0 {
		return nil, errDBClosed
	}

	cValue := C.rocksdb_get(db.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	if cValue == nil {
		return nil, nil
	}
	defer C.free(unsafe.Pointer(cValue))
	return C.GoBytes(unsafe.Pointer(cValue), C.int(cValLen)), nil
}

// GetBytes is like Get but returns a copy of the data.
func (db *DB) GetBytes(opts *ReadOptions, key []byte) ([]byte, error) {
	var (
		cErr    *C.char
		cValLen C.size_t
		cKey    = byteToChar(key)
	)
	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return nil, errDBClosed
	}

	cValue := C.rocksdb_get(db.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
	db.RUnlock()
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	if cValue == nil {
		return nil, nil
	}
	defer C.free(unsafe.Pointer(cValue))
	return C.GoBytes(unsafe.Pointer(cValue), C.int(cValLen)), nil
}

// GetCF returns the data associated with the key from the database and column family.
func (db *DB) GetCF(opts *ReadOptions, cf *ColumnFamilyHandle, key []byte) (*Slice, error) {
	var (
		cErr    *C.char
		cValLen C.size_t
		cKey    = byteToChar(key)
	)
	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return nil, errDBClosed
	}

	cValue := C.rocksdb_get_cf(db.c, opts.c, cf.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
	db.RUnlock()
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return NewSlice(cValue, cValLen), nil
}

func (db *DB) MultiGetBytes(opts *ReadOptions, keyList [][]byte, values [][]byte, errs []error) {
	cKeys := make([]*C.char, len(keyList))
	cKeySizeList := make([]C.size_t, len(keyList))
	cValues := make([]*C.char, len(keyList))
	cValueSizeList := make([]C.size_t, len(keyList))
	cErrs := make([]*C.char, len(keyList))
	for i, k := range keyList {
		cKeys[i] = cByteSlice(k)
		cKeySizeList[i] = C.size_t(len(k))
	}
	db.RLock()
	if db.opened == 1 {
		C.rocksdb_multi_get(db.c, opts.c, C.size_t(len(keyList)),
			(**C.char)(unsafe.Pointer(&cKeys[0])),
			(*C.size_t)(unsafe.Pointer(&cKeySizeList[0])),
			(**C.char)(unsafe.Pointer(&cValues[0])),
			(*C.size_t)(unsafe.Pointer(&cValueSizeList[0])),
			(**C.char)(unsafe.Pointer(&cErrs[0])),
		)
	} else {
		for i := 0; i < len(keyList); i++ {
			values[i] = nil
			errs[i] = errDBClosed
			C.free(unsafe.Pointer(cKeys[i]))
		}
		db.RUnlock()
		return
	}
	db.RUnlock()
	for i := 0; i < len(keyList); i++ {
		if cErrs[i] == nil {
			if cValues[i] == nil {
				values[i] = nil
			} else {
				values[i] = C.GoBytes(unsafe.Pointer(cValues[i]), C.int(cValueSizeList[i]))
				C.free(unsafe.Pointer(cValues[i]))
			}
		} else {
			values[i] = nil
			errs[i] = errors.New(C.GoString(cErrs[i]))
			C.free(unsafe.Pointer(cErrs[i]))
		}
		C.free(unsafe.Pointer(cKeys[i]))
	}
}

// Put writes data associated with a key to the database.
func (db *DB) Put(opts *WriteOptions, key, value []byte) error {
	var (
		cErr   *C.char
		cKey   = byteToChar(key)
		cValue = byteToChar(value)
	)
	C.rocksdb_put(db.c, opts.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// PutCF writes data associated with a key to the database and column family.
func (db *DB) PutCF(opts *WriteOptions, cf *ColumnFamilyHandle, key, value []byte) error {
	var (
		cErr   *C.char
		cKey   = byteToChar(key)
		cValue = byteToChar(value)
	)
	C.rocksdb_put_cf(db.c, opts.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Delete removes the data associated with the key from the database.
func (db *DB) Delete(opts *WriteOptions, key []byte) error {
	var (
		cErr *C.char
		cKey = byteToChar(key)
	)
	C.rocksdb_delete(db.c, opts.c, cKey, C.size_t(len(key)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// DeleteCF removes the data associated with the key from the database and column family.
func (db *DB) DeleteCF(opts *WriteOptions, cf *ColumnFamilyHandle, key []byte) error {
	var (
		cErr *C.char
		cKey = byteToChar(key)
	)
	C.rocksdb_delete_cf(db.c, opts.c, cf.c, cKey, C.size_t(len(key)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Merge merges the data associated with the key with the actual data in the database.
func (db *DB) Merge(opts *WriteOptions, key []byte, value []byte) error {
	var (
		cErr   *C.char
		cKey   = byteToChar(key)
		cValue = byteToChar(value)
	)
	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return errDBClosed
	}

	C.rocksdb_merge(db.c, opts.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	db.RUnlock()
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// MergeCF merges the data associated with the key with the actual data in the
// database and column family.
func (db *DB) MergeCF(opts *WriteOptions, cf *ColumnFamilyHandle, key []byte, value []byte) error {
	var (
		cErr   *C.char
		cKey   = byteToChar(key)
		cValue = byteToChar(value)
	)
	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return errDBClosed
	}

	C.rocksdb_merge_cf(db.c, opts.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	db.RUnlock()
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Write writes a WriteBatch to the database
func (db *DB) Write(opts *WriteOptions, batch *WriteBatch) error {
	var cErr *C.char
	C.rocksdb_write(db.c, opts.c, batch.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// NewIterator returns an Iterator over the the database that uses the
// ReadOptions given.
// iterator should be protected by rlock by caller since it may hold during iterating
func (db *DB) NewIterator(opts *ReadOptions) (*Iterator, error) {
	if db.opened == 0 {
		return nil, errDBClosed
	}
	cIter := C.rocksdb_create_iterator(db.c, opts.c)
	return NewNativeIterator(unsafe.Pointer(cIter)), nil
}

// NewIteratorCF returns an Iterator over the the database and column family
// that uses the ReadOptions given.
func (db *DB) NewIteratorCF(opts *ReadOptions, cf *ColumnFamilyHandle) (*Iterator, error) {
	if db.opened == 0 {
		return nil, errDBClosed
	}
	cIter := C.rocksdb_create_iterator_cf(db.c, opts.c, cf.c)
	return NewNativeIterator(unsafe.Pointer(cIter)), nil
}

// NewSnapshot creates a new snapshot of the database.
func (db *DB) NewSnapshot() (*Snapshot, error) {
	if db.opened == 0 {
		return nil, errDBClosed
	}
	cSnap := C.rocksdb_create_snapshot(db.c)
	return NewNativeSnapshot(cSnap, db.c), nil
}

// GetProperty returns the value of a database property.
func (db *DB) GetProperty(propName string) string {
	cprop := C.CString(propName)
	defer C.free(unsafe.Pointer(cprop))
	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return ""
	}

	cValue := C.rocksdb_property_value(db.c, cprop)
	db.RUnlock()
	defer C.free(unsafe.Pointer(cValue))
	return C.GoString(cValue)
}

// GetPropertyCF returns the value of a database property.
func (db *DB) GetPropertyCF(propName string, cf *ColumnFamilyHandle) string {
	cProp := C.CString(propName)
	defer C.free(unsafe.Pointer(cProp))
	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return ""
	}

	cValue := C.rocksdb_property_value_cf(db.c, cf.c, cProp)
	db.RUnlock()
	defer C.free(unsafe.Pointer(cValue))
	return C.GoString(cValue)
}

// CreateColumnFamily create a new column family.
// ColumnFamily should be closed before the engine closed
func (db *DB) CreateColumnFamily(opts *Options, name string) (*ColumnFamilyHandle, error) {
	var (
		cErr  *C.char
		cName = C.CString(name)
	)
	defer C.free(unsafe.Pointer(cName))
	cHandle := C.rocksdb_create_column_family(db.c, opts.c, cName, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return NewNativeColumnFamilyHandle(cHandle), nil
}

// DropColumnFamily drops a column family.
func (db *DB) DropColumnFamily(c *ColumnFamilyHandle) error {
	var cErr *C.char
	C.rocksdb_drop_column_family(db.c, c.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// GetApproximateSizes returns the approximate number of bytes of file system
// space used by one or more key ranges.
//
// The keys counted will begin at Range.Start and end on the key before
// Range.Limit.
func (db *DB) GetApproximateSizes(ranges []Range, includeMem bool) []uint64 {
	sizes := make([]uint64, len(ranges))
	if len(ranges) == 0 {
		return sizes
	}
	memSizes := make([]uint64, len(ranges))

	cStarts := make([]*C.char, len(ranges))
	cLimits := make([]*C.char, len(ranges))
	cStartLens := make([]C.size_t, len(ranges))
	cLimitLens := make([]C.size_t, len(ranges))

	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return nil
	}
	for i, r := range ranges {
		cStarts[i] = cByteSlice(r.Start)
		cStartLens[i] = C.size_t(len(r.Start))
		cLimits[i] = cByteSlice(r.Limit)
		cLimitLens[i] = C.size_t(len(r.Limit))
	}
	C.rocksdb_approximate_sizes(
		db.c,
		C.int(len(ranges)),
		&cStarts[0],
		&cStartLens[0],
		&cLimits[0],
		&cLimitLens[0],
		(*C.uint64_t)(&sizes[0]))

	if includeMem {
		C.rocksdb_approximate_memtable_sizes(
			db.c,
			C.int(len(ranges)),
			&cStarts[0],
			&cStartLens[0],
			&cLimits[0],
			&cLimitLens[0],
			(*C.uint64_t)(&memSizes[0]),
		)
	}

	db.RUnlock()
	for i := 0; i < len(cStarts); i++ {
		C.free(unsafe.Pointer(cStarts[i]))
		C.free(unsafe.Pointer(cLimits[i]))
	}
	if includeMem {
		for i, s := range memSizes {
			sizes[i] += s
		}
	}
	return sizes
}

// GetApproximateSizesCF returns the approximate number of bytes of file system
// space used by one or more key ranges in the column family.
//
// The keys counted will begin at Range.Start and end on the key before
// Range.Limit.
func (db *DB) GetApproximateSizesCF(cf *ColumnFamilyHandle, ranges []Range, includeMem bool) []uint64 {
	sizes := make([]uint64, len(ranges))
	if len(ranges) == 0 {
		return sizes
	}
	memSizes := make([]uint64, len(ranges))

	cStarts := make([]*C.char, len(ranges))
	cLimits := make([]*C.char, len(ranges))
	cStartLens := make([]C.size_t, len(ranges))
	cLimitLens := make([]C.size_t, len(ranges))

	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return nil
	}
	for i, r := range ranges {
		cStarts[i] = cByteSlice(r.Start)
		cStartLens[i] = C.size_t(len(r.Start))
		cLimits[i] = cByteSlice(r.Limit)
		cLimitLens[i] = C.size_t(len(r.Limit))
	}

	C.rocksdb_approximate_sizes_cf(
		db.c,
		cf.c,
		C.int(len(ranges)),
		&cStarts[0],
		&cStartLens[0],
		&cLimits[0],
		&cLimitLens[0],
		(*C.uint64_t)(&sizes[0]))

	if includeMem {
		C.rocksdb_approximate_memtable_sizes_cf(
			db.c,
			cf.c,
			C.int(len(ranges)),
			&cStarts[0],
			&cStartLens[0],
			&cLimits[0],
			&cLimitLens[0],
			(*C.uint64_t)(&memSizes[0]),
		)
	}
	db.RUnlock()

	for i := 0; i < len(cStarts); i++ {
		C.free(unsafe.Pointer(cStarts[i]))
		C.free(unsafe.Pointer(cLimits[i]))
	}
	if includeMem {
		for i, s := range memSizes {
			sizes[i] += s
		}
	}
	return sizes
}

func (db *DB) GetApproximateKeyNum(ranges []Range) uint64 {
	if len(ranges) == 0 {
		return 0
	}
	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return 0
	}

	cStarts := make([]*C.char, len(ranges))
	cLimits := make([]*C.char, len(ranges))
	cStartLens := make([]C.size_t, len(ranges))
	cLimitLens := make([]C.size_t, len(ranges))
	for i, r := range ranges {
		cStarts[i] = cByteSlice(r.Start)
		cStartLens[i] = C.size_t(len(r.Start))
		cLimits[i] = cByteSlice(r.Limit)
		cLimitLens[i] = C.size_t(len(r.Limit))
	}

	cNum := C.rocksdb_get_table_property_keynum_in_ranges(
		db.c,
		C.int(len(ranges)),
		&cStarts[0],
		&cStartLens[0],
		&cLimits[0],
		&cLimitLens[0])

	db.RUnlock()

	for i := 0; i < len(cStarts); i++ {
		C.free(unsafe.Pointer(cStarts[i]))
		C.free(unsafe.Pointer(cLimits[i]))
	}
	return uint64(cNum)
}

// LiveFileMetadata is a metadata which is associated with each SST file.
type LiveFileMetadata struct {
	Name        string
	Level       int
	Size        int64
	SmallestKey []byte
	LargestKey  []byte
}

// GetLiveFilesMetaData returns a list of all table files with their
// level, start key and end key.
func (db *DB) GetLiveFilesMetaData() []LiveFileMetadata {
	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return nil
	}

	lf := C.rocksdb_livefiles(db.c)

	defer C.rocksdb_livefiles_destroy(lf)

	count := C.rocksdb_livefiles_count(lf)
	liveFiles := make([]LiveFileMetadata, int(count))
	for i := C.int(0); i < count; i++ {
		var liveFile LiveFileMetadata
		liveFile.Name = C.GoString(C.rocksdb_livefiles_name(lf, i))
		liveFile.Level = int(C.rocksdb_livefiles_level(lf, i))
		liveFile.Size = int64(C.rocksdb_livefiles_size(lf, i))

		var cSize C.size_t
		key := C.rocksdb_livefiles_smallestkey(lf, i, &cSize)
		liveFile.SmallestKey = C.GoBytes(unsafe.Pointer(key), C.int(cSize))

		key = C.rocksdb_livefiles_largestkey(lf, i, &cSize)
		liveFile.LargestKey = C.GoBytes(unsafe.Pointer(key), C.int(cSize))
		liveFiles[int(i)] = liveFile
	}
	db.RUnlock()
	return liveFiles
}

// CompactRange runs a manual compaction on the Range of keys given. This is
// not likely to be needed for typical usage.
func (db *DB) CompactRange(r Range) {
	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return
	}
	if r.Start == nil && r.Limit == nil {
		C.rocksdb_compact_range(db.c, nil, C.size_t(0), nil, C.size_t(0))
	} else {
		cStart := cByteSlice(r.Start)
		cLimit := cByteSlice(r.Limit)
		defer C.free(unsafe.Pointer(cStart))
		defer C.free(unsafe.Pointer(cLimit))

		C.rocksdb_compact_range(db.c, cStart, C.size_t(len(r.Start)), cLimit, C.size_t(len(r.Limit)))
	}
	db.RUnlock()
}

// CompactRangeCF runs a manual compaction on the Range of keys given on the
// given column family. This is not likely to be needed for typical usage.
func (db *DB) CompactRangeCF(cf *ColumnFamilyHandle, r Range) {
	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return
	}
	cStart := cByteSlice(r.Start)
	cLimit := cByteSlice(r.Limit)
	defer C.free(unsafe.Pointer(cStart))
	defer C.free(unsafe.Pointer(cLimit))

	C.rocksdb_compact_range_cf(db.c, cf.c, cStart, C.size_t(len(r.Start)), cLimit, C.size_t(len(r.Limit)))
	db.RUnlock()
}

// Flush triggers a manuel flush for the database.
func (db *DB) Flush(opts *FlushOptions) error {
	var cErr *C.char
	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return errDBClosed
	}

	C.rocksdb_flush(db.c, opts.c, &cErr)
	db.RUnlock()
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// DisableFileDeletions disables file deletions and should be used when backup the database.
func (db *DB) DisableFileDeletions() error {
	var cErr *C.char
	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return errDBClosed
	}

	C.rocksdb_disable_file_deletions(db.c, &cErr)
	db.RUnlock()
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// EnableFileDeletions enables file deletions for the database.
func (db *DB) EnableFileDeletions(force bool) error {
	var cErr *C.char
	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return errDBClosed
	}

	C.rocksdb_enable_file_deletions(db.c, boolToChar(force), &cErr)
	db.RUnlock()
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

func (db *DB) DeleteFilesInRange(r Range) error {
	var (
		cErr   *C.char
		cStart = byteToChar(r.Start)
		cLimit = byteToChar(r.Limit)
	)
	C.rocksdb_delete_file_in_range(db.c, cStart, C.size_t(len(r.Start)), cLimit, C.size_t(len(r.Limit)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// DeleteFile deletes the file name from the db directory and update the internal state to
// reflect that. Supports deletion of sst and log files only. 'name' must be
// path relative to the db directory. eg. 000001.sst, /archive/000003.log.
func (db *DB) DeleteFile(name string) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	db.RLock()
	if db.opened == 0 {
		db.RUnlock()
		return
	}

	C.rocksdb_delete_file(db.c, cName)
	db.RUnlock()
}

// just close engine and not free db handle
// this can trigger all running write and compact return error
func (db *DB) Shutdown() {
	db.RLock()
	defer db.RUnlock()
	if db.opened == 0 {
		return
	}
	C.rocksdb_shutdown(db.c)
}

// Close closes the database.
func (db *DB) Close() {
	db.Lock()
	C.rocksdb_close(db.c)
	atomic.StoreInt32(&db.opened, 0)
	db.Unlock()
}

// DestroyDb removes a database entirely, removing everything from the
// filesystem.
func DestroyDb(name string, opts *Options) error {
	var (
		cErr  *C.char
		cName = C.CString(name)
	)
	defer C.free(unsafe.Pointer(cName))
	C.rocksdb_destroy_db(opts.c, cName, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// RepairDb repairs a database.
func RepairDb(name string, opts *Options) error {
	var (
		cErr  *C.char
		cName = C.CString(name)
	)
	defer C.free(unsafe.Pointer(cName))
	C.rocksdb_repair_db(opts.c, cName, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}
