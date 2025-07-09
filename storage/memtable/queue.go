package memtable

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/nagarajRPoojari/lsm/storage/utils"
)

type Node[K utils.Key, V utils.Value] struct {
	Next *Node[K, V]
	Prev *Node[K, V]

	mem *Memtable[K, V]

	// mutex to keep track of disposability of memtable
	immutable sync.RWMutex
}

func NewNode[K utils.Key, V utils.Value](mem *Memtable[K, V]) *Node[K, V] {
	return &Node[K, V]{
		mem:       mem,
		immutable: sync.RWMutex{},
	}
}

type QueueOpts struct {
	// Hard limit on queue size
	HardLimit int
}

type Queue[K utils.Key, V utils.Value] struct {
	head *Node[K, V]
	tail *Node[K, V]

	headLock sync.RWMutex
	tailLock sync.RWMutex

	len atomic.Int32

	opts QueueOpts
}

func NewQueue[K utils.Key, V utils.Value](opts QueueOpts) *Queue[K, V] {
	return &Queue[K, V]{
		opts: opts,
	}
}

func (t *Queue[K, V]) Push(node *Node[K, V]) {
	// Push should only bother about modifying tail concurrently, acquiring tail
	// lock serializes push completely
	t.tailLock.Lock()
	defer t.tailLock.Unlock()

	defer t.len.Add(1)

	if t.head == nil {
		t.headLock.Lock()
		defer t.headLock.Unlock()

		t.head = node
		t.tail = node
		return
	}

	node.Prev = t.tail
	t.tail.Next = node
	t.tail = node

}

func (t *Queue[K, V]) Pop() (*Memtable[K, V], error) {

	t.headLock.Lock()
	defer t.headLock.Unlock()

	if t.head == nil {
		return nil, fmt.Errorf("head is nil")
	}

	defer t.len.Add(-1)

	// head.mu.Lock defines disposability
	// it is pre-acquired lock for active write ops & prevent flusher from disposing
	// it will be realeased only when it is immutable/disposable (no active writes allowed)
	t.head.immutable.Lock()
	ret := t.head

	if t.len.Load() <= 1 {
		t.tailLock.Lock()
		defer t.tailLock.Unlock()
	}

	if t.head == t.tail {
		t.head = nil
		t.tail = nil
		return ret.mem, nil
	}

	if t.head.Next != nil {
		t.head.Next.Prev = nil
	}
	t.head = t.head.Next

	return ret.mem, nil
}
