package storage

import (
	"context"

	"github.com/nagarajRPoojari/lsm/storage/cache"
	"github.com/nagarajRPoojari/lsm/storage/compactor"
	"github.com/nagarajRPoojari/lsm/storage/errors"
	"github.com/nagarajRPoojari/lsm/storage/memtable"
	"github.com/nagarajRPoojari/lsm/storage/metadata"
	"github.com/nagarajRPoojari/lsm/storage/types"
)

// StorageOpts defines configuration options for the storage engine.
type StorageOpts struct {
	// Root directory where all data (WALs, SSTables, manifests) will be stored
	Directory string

	// Memtable configuration
	// Threshold (in bytes) after which the active memtable is flushed to disk
	MemtableThreshold int
	// Maximum number of memtables allowed in flush queue before blocking writes
	QueueHardLimit int
	// Soft limit to trigger proactive flushing before hitting the hard limit
	QueueSoftLimit int

	// Compaction configuration
	// Enables background compaction and garbage collection
	TurnOnCompaction bool
	// Directory to store compaction-related WALs or logs
	GCLogDir string

	// Write-Ahead Log configuration
	// Enables WAL for durability of writes
	TurnOnWal bool
	// Directory to store WAL files
	WalLogDir string
}

type Storage[K types.Key, V types.Value] struct {
	name     string
	store    *memtable.MemtableStore[K, V]
	manifest *metadata.Manifest

	reader *Reader[K, V]
	writer *Writer[K, V]

	opts StorageOpts
}

func NewStorage[K types.Key, V types.Value](name string, ctx context.Context, opts StorageOpts) *Storage[K, V] {
	v := &Storage[K, V]{name: name, opts: opts}
	v.createOrLoadCollection()
	v.reader = NewReader(v.store, ReaderOpts{})
	v.writer = NewWriter(v.store, WriterOpts{})

	if opts.TurnOnCompaction {

		gc := compactor.NewGC(
			v.manifest,
			(*cache.CacheManager[types.IntKey, types.IntValue])(v.store.DecoderCache),
			&compactor.SizeTiredCompaction[types.IntKey, types.IntValue]{Opts: compactor.SizeTiredCompactionOpts{Level0MaxSizeInBytes: 1024 * 2, MaxSizeInBytesGrowthFactor: 2}},
			opts.GCLogDir,
		)
		go gc.Run(ctx)
	}

	return v
}

func (t *Storage[K, V]) createOrLoadCollection() {
	mf := metadata.NewManifest(t.name, metadata.ManifestOpts{Dir: t.opts.Directory})
	mf.Load()

	ctx, _ := context.WithCancel(context.Background())
	go mf.Sync(ctx)

	mt := memtable.NewMemtableStore[K, V](mf,
		memtable.MemtableOpts{
			MemtableSoftLimit: int64(t.opts.MemtableThreshold),
			QueueHardLimit:    t.opts.QueueHardLimit,
			QueueSoftLimit:    t.opts.QueueSoftLimit,
			LogDir:            t.opts.WalLogDir,
		})
	t.store = mt
	t.manifest = mf
}

func (t *Storage[K, V]) Get(key K) ReadStatus[V] {
	return t.reader.Get(key)
}

func (t *Storage[K, V]) Put(key K, value V) WriteStatus {
	return t.writer.Put(key, value)
}

type ReadStatus[V types.Value] struct {
	Value V
	Err   error
}

type ReaderOpts struct {
}

type Reader[K types.Key, V types.Value] struct {
	store *memtable.MemtableStore[K, V]

	opts ReaderOpts
}

func NewReader[K types.Key, V types.Value](store *memtable.MemtableStore[K, V], opts ReaderOpts) *Reader[K, V] {
	r := &Reader[K, V]{
		store: store,
		opts:  opts,
	}

	r.opts = opts

	return r
}

func (t *Reader[K, V]) Get(key K) ReadStatus[V] {
	val, ok := t.store.Read(key)
	if !ok {
		return ReadStatus[V]{Err: errors.KeyNotFoundError}
	}
	return ReadStatus[V]{Value: val}
}

type WriteStatus struct {
	err error
}

type WriterOpts struct {
}

type Writer[K types.Key, V types.Value] struct {
	store *memtable.MemtableStore[K, V]

	opts WriterOpts
}

func NewWriter[K types.Key, V types.Value](store *memtable.MemtableStore[K, V], opts WriterOpts) *Writer[K, V] {
	r := &Writer[K, V]{
		store: store,
		opts:  opts,
	}
	r.opts = opts

	return r
}

func (t *Writer[K, V]) Put(key K, value V) WriteStatus {
	_ = t.store.Write(key, value)
	return WriteStatus{err: nil}
}
