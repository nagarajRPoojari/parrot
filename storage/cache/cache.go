package cache

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"sync"

	"github.com/nagarajRPoojari/lsm/storage/types"

	fio "github.com/nagarajRPoojari/lsm/storage/io"
)

type CacheManager[K types.Key, V types.Value] struct {
	// using sync.Map to prevent race
	cache sync.Map
}

func NewCacheManager[K types.Key, V types.Value]() *CacheManager[K, V] {
	return &CacheManager[K, V]{
		cache: sync.Map{},
	}
}

func (m *CacheManager[K, V]) Get(path string) ([]types.Payload[K, V], error) {
	val, loaded := m.cache.Load(path)
	if loaded {
		return val.(*CacheUnit[K, V]).GetDecoded()
	}
	fm := fio.GetFileManager()
	fr := fm.OpenForRead(path)

	// Create new cache and use LoadOrStore to avoid race
	newCache := &CacheUnit[K, V]{payload: fr.GetPayload()}
	actual, _ := m.cache.LoadOrStore(path, newCache)

	return actual.(*CacheUnit[K, V]).GetDecoded()
}

type CacheUnit[K types.Key, V types.Value] struct {
	// payload directly maps to mmap page (shared with multiple readers)
	payload []byte

	once sync.Once
	// decoded version of loaded payload
	decoded []types.Payload[K, V]
	err     error
}

func (dc *CacheUnit[K, V]) GetDecoded() ([]types.Payload[K, V], error) {
	dc.once.Do(func() {
		var result []types.Payload[K, V]
		decoder := gob.NewDecoder(bytes.NewReader(dc.payload))

		for {
			var entry types.Payload[K, V]
			err := decoder.Decode(&entry)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				dc.err = err
				return
			}
			result = append(result, entry)
		}
		dc.decoded = result
	})
	return dc.decoded, dc.err
}
