package storage

import (
	"github.com/nagarajRPoojari/lsm/storage/memtable"
	"github.com/nagarajRPoojari/lsm/storage/metadata"
	"github.com/nagarajRPoojari/lsm/storage/types"
)

type Storage[K types.Key, V types.Value] struct {
	name string
	mt   *memtable.MemtableStore[K, V]
}

func NewStorage[K types.Key, V types.Value](name string) *Storage[K, V] {
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
