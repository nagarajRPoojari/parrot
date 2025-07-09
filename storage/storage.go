package storage

import (
	"github.com/nagarajRPoojari/lsm/storage/memtable"
	"github.com/nagarajRPoojari/lsm/storage/metadata"
	"github.com/nagarajRPoojari/lsm/storage/utils"
)

type Storage[K comparable, V utils.Value] struct {
	name string
	mt   *memtable.MemtableStore[K, V]
}

type StringValue struct {
	v string
}

func (t StringValue) SizeOf() uintptr {
	return uintptr(len(t.v))
}

func NewStorage[K comparable, V utils.Value](name string) *Storage[K, V] {
	v := &Storage[K, V]{name: name}
	v.createOrLoadCollection()
	return v
}

func (t *Storage[K, V]) createOrLoadCollection() {
	mf := metadata.NewManifest(t.name, metadata.ManifestOpts{})
	mf.Load()
	mt := memtable.NewMemtableStore[K, V](mf, memtable.MemtableOpts{})
	t.mt = mt
}

// func (t *Storage[K, V]) Get(key K) (V, error) {

// }
