package memtable

import (
	"fmt"
	"log"
	"sync"

	"github.com/nagarajRPoojari/lsm/storage/io"
	"github.com/nagarajRPoojari/lsm/storage/metadata"
	"github.com/nagarajRPoojari/lsm/storage/utils"
)

type MemtableOpts struct {
	MemtableSoftLimit int64 // bytes
	QueueHardLimit    int
	QueueSoftLimit    int
}

type Memtable[K utils.Key, V utils.Value] struct {
	data map[K]V

	// RWMutex to prevent concurrent io
	mu   *sync.RWMutex
	opts MemtableOpts
}

func NewMemtable[K utils.Key, V utils.Value](opts MemtableOpts) *Memtable[K, V] {
	mem := &Memtable[K, V]{data: map[K]V{}, mu: &sync.RWMutex{}, opts: opts}
	return mem
}

func (t *Memtable[K, V]) BuildPayloadList() []utils.Payload[K, V] {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var pl []utils.Payload[K, V]
	for k, v := range t.data {
		pl = append(pl, utils.Payload[K, V]{Key: k, Val: v})
	}
	return pl
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

type MemtableStore[K utils.Key, V utils.Value] struct {
	mf *metadata.Manifest
	q  *Queue[K, V]

	// keep track of current non-disposable memtable & it's corresponding node
	mem *Memtable[K, V]

	// keep track of memNode to modify disposablity
	memNode *Node[K, V]

	flusher *Flusher[K, V]
	opts    MemtableOpts
}

func NewMemtableStore[K utils.Key, V utils.Value](mf *metadata.Manifest, opts MemtableOpts) *MemtableStore[K, V] {
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
			// log.Println("read from memtable")
			return v, true
		}
		node = node.Prev
		c++
	}

	// log.Println("Started reading from sstables")
	// Search backward at all sst
	level, _ := t.mf.GetLSM().GetLevel(0)
	cnt := 0
	for level != nil {
		for _, t := range level.GetTables() {
			fm := io.GetFileManager()
			fr := fm.OpenForRead(t.Path)

			l, _ := utils.Decode[K, V](fr)
			// @todo: use min/max lookup to avoid full table search
			for _, k := range l {
				if k.Key == key {
					// log.Println("Read key-val from sst")
					return k.Val, true
				}
			}

		}
		level, _ = t.mf.GetLSM().GetLevel(1)
		cnt++
	}
	fmt.Println("can't find with nodes, levels", c, cnt)
	var empty V
	return empty, false
}
