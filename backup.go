package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"errors"
	"unsafe"
)

// BackupEngineInfo represents the information about the backups
// in a backup engine instance. Use this to get the state of the
// backup like number of backups and their ids and timestamps etc.
type BackupEngineInfo struct {
	c *C.rocksdb_backup_engine_info_t
}

// GetCount gets the number backsup available.
func (b *BackupEngineInfo) GetCount() int {
	return int(C.rocksdb_backup_engine_info_count(b.c))
}

// GetTimestamp gets the timestamp at which the backup index was taken.
func (b *BackupEngineInfo) GetTimestamp(index int) int64 {
	return int64(C.rocksdb_backup_engine_info_timestamp(b.c, C.int(index)))
}

// GetBackupId gets an id that uniquely identifies a backup
// regardless of its position.
func (b *BackupEngineInfo) GetBackupId(index int) int64 {
	return int64(C.rocksdb_backup_engine_info_backup_id(b.c, C.int(index)))
}

// GetSize get the size of the backup in bytes.
func (b *BackupEngineInfo) GetSize(index int) int64 {
	return int64(C.rocksdb_backup_engine_info_size(b.c, C.int(index)))
}

// GetNumFiles gets the number of files in the backup index.
func (b *BackupEngineInfo) GetNumFiles(index int) int32 {
	return int32(C.rocksdb_backup_engine_info_number_files(b.c, C.int(index)))
}

// Destroy destroys the backup engine info instance.
func (b *BackupEngineInfo) Destroy() {
	C.rocksdb_backup_engine_info_destroy(b.c)
	b.c = nil
}

// RestoreOptions captures the options to be used during
// restoration of a backup.
type RestoreOptions struct {
	c *C.rocksdb_restore_options_t
}

// NewRestoreOptions creates a RestoreOptions instance.
func NewRestoreOptions() *RestoreOptions {
	return &RestoreOptions{
		c: C.rocksdb_restore_options_create(),
	}
}

// SetKeepLogFiles is used to set or unset the keep_log_files option
// If true, restore won't overwrite the existing log files in wal_dir. It will
// also move all log files from archive directory to wal_dir.
// By default, this is false.
func (ro *RestoreOptions) SetKeepLogFiles(v int) {
	C.rocksdb_restore_options_set_keep_log_files(ro.c, C.int(v))
}

// Destroy destroys this RestoreOptions instance.
func (ro *RestoreOptions) Destroy() {
	C.rocksdb_restore_options_destroy(ro.c)
}

// BackupEngine is a reusable handle to a RocksDB Backup, created by
// OpenBackupEngine.
type BackupEngine struct {
	c    *C.rocksdb_backup_engine_t
	path string
	opts *Options
}

// OpenBackupEngine opens a backup engine with specified options.
func OpenBackupEngine(opts *Options, path string) (*BackupEngine, error) {
	var cErr *C.char
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	be := C.rocksdb_backup_engine_open(opts.c, cpath, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return &BackupEngine{
		c:    be,
		path: path,
		opts: opts,
	}, nil
}

// UnsafeGetBackupEngine returns the underlying c backup engine.
func (b *BackupEngine) UnsafeGetBackupEngine() unsafe.Pointer {
	return unsafe.Pointer(b.c)
}

// CreateNewBackup takes a new backup from db.
func (b *BackupEngine) CreateNewBackup(db *DB) error {
	var cErr *C.char

	C.rocksdb_backup_engine_create_new_backup(b.c, db.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}

	return nil
}

// GetInfo gets an object that gives information about
// the backups that have already been taken
func (b *BackupEngine) GetInfo() *BackupEngineInfo {
	return &BackupEngineInfo{
		c: C.rocksdb_backup_engine_get_backup_info(b.c),
	}
}

// RestoreDBFromLatestBackup restores the latest backup to dbDir. walDir
// is where the write ahead logs are restored to and usually the same as dbDir.
func (b *BackupEngine) RestoreDBFromLatestBackup(dbDir, walDir string, ro *RestoreOptions) error {
	var cErr *C.char
	cDbDir := C.CString(dbDir)
	cWalDir := C.CString(walDir)
	defer func() {
		C.free(unsafe.Pointer(cDbDir))
		C.free(unsafe.Pointer(cWalDir))
	}()

	C.rocksdb_backup_engine_restore_db_from_latest_backup(b.c, cDbDir, cWalDir, ro.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Close close the backup engine and cleans up state
// The backups already taken remain on storage.
func (b *BackupEngine) Close() {
	C.rocksdb_backup_engine_close(b.c)
	b.c = nil
}

func (b *BackupEngine) PurgeOldBackups(db *DB, num_backups_to_keep int) error {
	var cErr *C.char
	C.rocksdb_backup_engine_purge_old_backups(b.c, C.uint32_t(num_backups_to_keep), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

type Checkpoint struct {
	c   *C.rocksdb_checkpoint_t
	cDb *C.rocksdb_t
}

func NewCheckpoint(db *DB) (*Checkpoint, error) {
	var cErr *C.char
	cCheck := C.rocksdb_checkpoint_object_create(db.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return &Checkpoint{
		c:   cCheck,
		cDb: db.c,
	}, nil
}

func (c *Checkpoint) Save(dir string, log_size_for_flush uint64) error {
	var (
		cErr *C.char
		cDir = C.CString(dir)
	)
	defer C.free(unsafe.Pointer(cDir))
	C.rocksdb_checkpoint_create(c.c, cDir,
		C.uint64_t(log_size_for_flush), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

func (c *Checkpoint) Destroy() {
	C.rocksdb_checkpoint_object_destroy(c.c)
	c.c, c.cDb = nil, nil
}
