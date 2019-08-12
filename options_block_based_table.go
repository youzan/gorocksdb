package gorocksdb

// #include "rocksdb/c.h"
// #include "gorocksdb.h"
import "C"

const (
	// the index type for blockbased table
	IndexTypeBinarySearch int = iota
	IndexTypeHashSearch
	IndexTypeTwoLevelIndexSearch
)

// BlockBasedTableOptions represents block-based table options.
type BlockBasedTableOptions struct {
	c *C.rocksdb_block_based_table_options_t

	// Hold references for GC.
	cache     *Cache
	compCache *Cache

	// We keep these so we can free their memory in Destroy.
	cFp *C.rocksdb_filterpolicy_t
}

// NewDefaultBlockBasedTableOptions creates a default BlockBasedTableOptions object.
func NewDefaultBlockBasedTableOptions() *BlockBasedTableOptions {
	return NewNativeBlockBasedTableOptions(C.rocksdb_block_based_options_create())
}

// NewNativeBlockBasedTableOptions creates a BlockBasedTableOptions object.
func NewNativeBlockBasedTableOptions(c *C.rocksdb_block_based_table_options_t) *BlockBasedTableOptions {
	return &BlockBasedTableOptions{c: c}
}

// Destroy deallocates the BlockBasedTableOptions object.
func (opts *BlockBasedTableOptions) Destroy() {
	C.rocksdb_block_based_options_destroy(opts.c)
	opts.c = nil
	opts.cache = nil
	opts.compCache = nil
}

// SetBlockSize sets the approximate size of user data packed per block.
// Note that the block size specified here corresponds opts uncompressed data.
// The actual size of the unit read from disk may be smaller if
// compression is enabled. This parameter can be changed dynamically.
// Default: 4K
func (opts *BlockBasedTableOptions) SetBlockSize(blockSize int) {
	C.rocksdb_block_based_options_set_block_size(opts.c, C.size_t(blockSize))
}

// SetBlockSizeDeviation sets the block size deviation.
// This is used opts close a block before it reaches the configured
// 'block_size'. If the percentage of free space in the current block is less
// than this specified number and adding a new record opts the block will
// exceed the configured block size, then this block will be closed and the
// new record will be written opts the next block.
// Default: 10
func (opts *BlockBasedTableOptions) SetBlockSizeDeviation(blockSizeDeviation int) {
	C.rocksdb_block_based_options_set_block_size_deviation(opts.c, C.int(blockSizeDeviation))
}

// SetBlockRestartInterval sets the number of keys between
// restart points for delta encoding of keys.
// This parameter can be changed dynamically. Most clients should
// leave this parameter alone.
// Default: 16
func (opts *BlockBasedTableOptions) SetBlockRestartInterval(blockRestartInterval int) {
	C.rocksdb_block_based_options_set_block_restart_interval(opts.c, C.int(blockRestartInterval))
}

// SetIndexBlockRestartInterval sets the number of keys between
// restart points for delta encoding of keys for index data block.
// Default: 1
func (opts *BlockBasedTableOptions) SetIndexBlockRestartInterval(restartInterval int) {
	C.rocksdb_block_based_options_set_index_block_restart_interval(opts.c, C.int(restartInterval))
}

// SetFilterPolicy sets the filter policy opts reduce disk reads.
// Many applications will benefit from passing the result of
// NewBloomFilterPolicy() here.
// Default: nil
func (opts *BlockBasedTableOptions) SetFilterPolicy(fp FilterPolicy) {
	if nfp, ok := fp.(nativeFilterPolicy); ok {
		opts.cFp = nfp.c
	} else {
		idx := registerFilterPolicy(fp)
		opts.cFp = C.gorocksdb_filterpolicy_create(C.uintptr_t(idx))
	}
	C.rocksdb_block_based_options_set_filter_policy(opts.c, opts.cFp)
}

// SetNoBlockCache specify whether block cache should be used or not.
// Default: false
func (opts *BlockBasedTableOptions) SetNoBlockCache(value bool) {
	C.rocksdb_block_based_options_set_no_block_cache(opts.c, boolToChar(value))
}

// SetBlockCache sets the control over blocks (user data is stored in a set of blocks, and
// a block is the unit of reading from disk).
//
// If set, use the specified cache for blocks.
// If nil, rocksdb will auoptsmatically create and use an 8MB internal cache.
// Default: nil
func (opts *BlockBasedTableOptions) SetBlockCache(cache *Cache) {
	opts.cache = cache
	C.rocksdb_block_based_options_set_block_cache(opts.c, cache.c)
}

func (opts *BlockBasedTableOptions) GetBlockCache() *Cache {
	return opts.cache
}

// SetBlockCacheCompressed sets the cache for compressed blocks.
// If nil, rocksdb will not use a compressed block cache.
// Default: nil
func (opts *BlockBasedTableOptions) SetBlockCacheCompressed(cache *Cache) {
	opts.compCache = cache
	C.rocksdb_block_based_options_set_block_cache_compressed(opts.c, cache.c)
}

// SetWholeKeyFiltering specify if whole keys in the filter (not just prefixes)
// should be placed.
// This must generally be true for gets opts be efficient.
// Default: true
func (opts *BlockBasedTableOptions) SetWholeKeyFiltering(value bool) {
	C.rocksdb_block_based_options_set_whole_key_filtering(opts.c, boolToChar(value))
}

func (opts *BlockBasedTableOptions) SetCacheIndexAndFilterBlocks(value bool) {
	C.rocksdb_block_based_options_set_cache_index_and_filter_blocks(opts.c, boolToChar(value))
}

func (opts *BlockBasedTableOptions) SetPinL0FilterAndIndexBlocksInCache(value bool) {
	C.rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(opts.c, boolToChar(value))
}

// SetMetaDataBlockSize sets the approximate size of index data packed per block.
// Note that the block size specified here corresponds opts uncompressed data.
// The actual size of the unit read from disk may be smaller if
// compression is enabled. This parameter can be changed dynamically.
// Default: 4K
func (opts *BlockBasedTableOptions) SetMetaDataBlockSize(blockSize uint64) {
	C.rocksdb_block_based_options_set_metadata_block_size(opts.c, C.uint64_t(blockSize))
}

func (opts *BlockBasedTableOptions) SetPartitionFilters(enable bool) {
	C.rocksdb_block_based_options_set_partition_filters(opts.c, boolToChar(enable))
}

func (opts *BlockBasedTableOptions) SetIndexType(it int) {
	//enum {
	//	rocksdb_block_based_table_index_type_binary_search = 0,
	//	rocksdb_block_based_table_index_type_hash_search = 1,
	//	rocksdb_block_based_table_index_type_two_level_index_search = 2,
	//  };
	C.rocksdb_block_based_options_set_index_type(opts.c, C.int(it))
}

func (opts *BlockBasedTableOptions) SetCacheIndexAndFilterBlocksWithHighPriority(value bool) {
	C.rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(opts.c, boolToChar(value))
}

func (opts *BlockBasedTableOptions) SetFormatVersion(value int) {
	C.rocksdb_block_based_options_set_format_version(opts.c, C.int(value))
}
