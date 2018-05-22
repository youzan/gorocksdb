# gorocksdb, a Go wrapper for RocksDB

[![Build Status](https://travis-ci.org/youzan/gorocksdb.png)](https://travis-ci.org/youzan/gorocksdb) [![GoDoc](https://godoc.org/github.com/youzan/gorocksdb?status.png)](http://godoc.org/github.com/youzan/gorocksdb)

## Install

You'll need to build [RocksDB](https://github.com/absolute8511/rocksdb) v5.5+ on your machine.

After that, you can install gorocksdb using the following command:

    CGO_CFLAGS="-I/path/to/rocksdb/include" \
    CGO_LDFLAGS="-L/path/to/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4" \
      go get github.com/youzan/gorocksdb

Please note that this package might upgrade the required RocksDB version at any moment.
Vendoring is thus highly recommended if you require high stability.