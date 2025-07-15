package storage

import (
	"context"

	"github.com/nagarajRPoojari/lsm/storage/cache"
	"github.com/nagarajRPoojari/lsm/storage/compactor"
	"github.com/nagarajRPoojari/lsm/storage/errors"
	"github.com/nagarajRPoojari/lsm/storage/memtable"
	"github.com/nagarajRPoojari/lsm/storage/metadata"
	"github.com/nagarajRPoojari/lsm/storage/types"
	"github.com/nagarajRPoojari/lsm/storage/utils/log"
)

type StorageOpts struct {
	ReadWorkersCount int
	ReadQueueSize    int
	WriteQueueSize   int
	Directory        string

	MemtableThreshold int
	TurnOnCompaction  bool
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
	v := &Storage[K, V]{name: name}
	v.createOrLoadCollection(opts.MemtableThreshold)
	v.reader = NewReader(v.store, ReaderOpts{
		RequestQueueSize: opts.ReadQueueSize,
		WorkersCount:     opts.ReadWorkersCount,
	})
	v.writer = NewWriter(v.store, WriterOpts{
		RequestQueueSize: opts.WriteQueueSize,
		WorkersCount:     1,
	})

	v.opts = opts

	if opts.TurnOnCompaction {

		gc := compactor.NewGC(
			v.manifest,
			(*cache.CacheManager[types.IntKey, types.IntValue])(v.store.DecoderCache),
			&compactor.SizeTiredCompaction[types.IntKey, types.IntValue]{Opts: compactor.SizeTiredCompactionOpts{Levle0MaxSizeInBytes: 1024 * 2, MaxSizeInBytesGrowthFactor: 2}},
		)
		go gc.Run(ctx)
	}

	return v
}

func (t *Storage[K, V]) createOrLoadCollection(memtableSoftLimit int) {
	mf := metadata.NewManifest(t.name, metadata.ManifestOpts{Dir: t.opts.Directory})
	mf.Load()

	ctx, _ := context.WithCancel(context.Background())
	go mf.Sync(ctx)

	mt := memtable.NewMemtableStore[K, V](mf, memtable.MemtableOpts{MemtableSoftLimit: int64(memtableSoftLimit)})
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

type ReadRequest[K types.Key, V types.Value] struct {
	key    K
	result chan ReadStatus[V]
}

type ReaderOpts struct {
	WorkersCount     int
	RequestQueueSize int
}

type Reader[K types.Key, V types.Value] struct {
	q     chan ReadRequest[K, V]
	store *memtable.MemtableStore[K, V]

	opts ReaderOpts
}

func NewReader[K types.Key, V types.Value](store *memtable.MemtableStore[K, V], opts ReaderOpts) *Reader[K, V] {
	r := &Reader[K, V]{
		store: store,
		q:     make(chan ReadRequest[K, V], opts.RequestQueueSize),
	}

	r.opts = opts

	for range r.opts.WorkersCount {
		go r.rworker()
	}

	return r
}

func (t *Reader[K, V]) Get(key K) ReadStatus[V] {
	req := ReadRequest[K, V]{key: key, result: make(chan ReadStatus[V])}
	t.q <- req
	return <-req.result
}

func (t *Reader[K, V]) rworker() {
	for req := range t.q {
		val, ok := t.store.Read(req.key)
		if !ok {
			req.result <- ReadStatus[V]{Err: errors.KeyNotFoundError}
		}
		req.result <- ReadStatus[V]{Value: val}
	}
}

/////////

type WriteStatus struct {
	err error
}

type WriterOpts struct {
	WorkersCount     int
	RequestQueueSize int
}

type WriteRequest[K types.Key, V types.Value] struct {
	key    K
	value  V
	status chan WriteStatus
}

type Writer[K types.Key, V types.Value] struct {
	q     chan WriteRequest[K, V]
	store *memtable.MemtableStore[K, V]

	opts WriterOpts
}

func NewWriter[K types.Key, V types.Value](store *memtable.MemtableStore[K, V], opts WriterOpts) *Writer[K, V] {
	r := &Writer[K, V]{
		store: store,
		q:     make(chan WriteRequest[K, V], opts.RequestQueueSize),
	}
	r.opts = opts

	for range r.opts.WorkersCount {
		go r.wworker()
	}

	return r
}

func (t *Writer[K, V]) Put(key K, value V) WriteStatus {
	req := WriteRequest[K, V]{key: key, value: value, status: make(chan WriteStatus)}
	t.q <- req
	return <-req.status
}

func (t *Writer[K, V]) wworker() {
	log.Infof("write worker running")
	for req := range t.q {
		log.Infof("write worker got a write request")
		_ = t.store.Write(req.key, req.value)
		req.status <- WriteStatus{err: nil}
	}
}
