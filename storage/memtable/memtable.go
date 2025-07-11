package memtable

import (
	"log"
	"sync"

	"github.com/nagarajRPoojari/lsm/storage/cache"
	"github.com/nagarajRPoojari/lsm/storage/metadata"
	"github.com/nagarajRPoojari/lsm/storage/types"
)

type MemtableOpts struct {
	MemtableSoftLimit int64 // bytes
	QueueHardLimit    int
	QueueSoftLimit    int
}

type Memtable[K types.Key, V types.Value] struct {
	data map[K]V

	// RWMutex to prevent concurrent io
	mu   *sync.RWMutex
	opts MemtableOpts
}

func NewMemtable[K types.Key, V types.Value](opts MemtableOpts) *Memtable[K, V] {
	mem := &Memtable[K, V]{data: map[K]V{}, mu: &sync.RWMutex{}, opts: opts}
	return mem
}

// @todo: optimize
func (t *Memtable[K, V]) BuildPayloadList() ([]types.Payload[K, V], int64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var pl []types.Payload[K, V]
	var size int64
	for k, v := range t.data {
		pl = append(pl, types.Payload[K, V]{Key: k, Val: v})
		size += int64(v.SizeOf())
	}
	return pl, size
}

func (t *Memtable[K, V]) Write(key K, value V) bool {
	// check soft threshold
	t.mu.Lock()
	defer t.mu.Unlock()
	if uintptr(len(t.data)+1)*value.SizeOf() > uintptr(t.opts.MemtableSoftLimit) {
		return false
	}
	t.data[key] = value
	return true
}

func (t *Memtable[K, V]) Read(key K) (V, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	val, ok := t.data[key]
	return val, ok
}

type MemtableStore[K types.Key, V types.Value] struct {
	mf *metadata.Manifest
	q  *Queue[K, V]

	// keep track of current non-disposable memtable & it's corresponding node
	mem *Memtable[K, V]

	// keep track of memNode to modify disposablity
	memNode *Node[K, V]

	flusher *Flusher[K, V]
	opts    MemtableOpts

	decoder *cache.CacheManager[K, V]
}

func NewMemtableStore[K types.Key, V types.Value](mf *metadata.Manifest, opts MemtableOpts) *MemtableStore[K, V] {
	q := NewQueue[K, V](QueueOpts{HardLimit: opts.QueueHardLimit})
	mem := NewMemtable[K, V](opts)
	node := NewNode(mem)

	// make head node non-disposable
	node.immutable.Lock()
	q.Push(node)

	flusher := NewFlusher(q, mf, FlusherOpts{})
	go flusher.Run()

	return &MemtableStore[K, V]{
		mf:      mf,
		q:       q,
		mem:     mem,
		opts:    opts,
		flusher: flusher,
		memNode: node,
		decoder: cache.NewCacheManager[K, V](),
	}
}

// warning! : helper function for unit tests
func (t *MemtableStore[K, V]) Clear() {
	t.mem = nil
}

// return value is true if flush is triggered
func (t *MemtableStore[K, V]) Write(key K, value V) bool {
	if ok := t.mem.Write(key, value); !ok {
		log.Println("Memtable overflow")

		// create new memtable with same options
		mem := NewMemtable[K, V](t.opts)
		node := NewNode(mem)

		// make current memtable non-disposable
		node.immutable.Lock()

		t.q.Push(node)
		mem.Write(key, value)

		// unlock previous memtable to allow dumping
		t.memNode.immutable.Unlock()

		// update current memtable
		t.memNode = node
		t.mem = mem
		return true
	}
	return false
}

func (t *MemtableStore[K, V]) Read(key K) (V, bool) {
	// Search backwards in Queue

	// log.Println("Started reading from memtables")

	node := t.q.tail
	c := 0
	for node != nil {
		if v, ok := node.mem.Read(key); ok {
			return v, true
		}
		node = node.Prev
		c++
	}

	// Search backward at all sst
	level, _ := t.mf.GetLSM().GetLevel(0)
	cnt := 0
	for level != nil {
		if len(level.GetTables()) == 0 {
			break
		}
		for _, table := range level.GetTables() {

			l, _ := t.decoder.Get(table.Path)
			// @todo: use min/max lookup to avoid full table search
			for _, k := range l {
				if k.Key == key {
					return k.Val, true
				}
			}

		}
		level, _ = t.mf.GetLSM().GetLevel(1)
		cnt++
	}
	var empty V
	return empty, false
}
