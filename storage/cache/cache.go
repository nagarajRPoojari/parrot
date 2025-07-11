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

type DecoderCacheManager[K types.Key, V types.Value] struct {
	cache sync.Map
}

func NewDecoderCacheManager[K types.Key, V types.Value]() *DecoderCacheManager[K, V] {
	return &DecoderCacheManager[K, V]{
		cache: sync.Map{},
	}
}

func (m *DecoderCacheManager[K, V]) Get(path string) ([]types.Payload[K, V], error) {
	val, loaded := m.cache.Load(path)
	if loaded {
		return val.(*DecoderCache[K, V]).GetDecoded()
	}
	fm := fio.GetFileManager()
	fr := fm.OpenForRead(path)

	// Create new cache and use LoadOrStore to avoid race
	newCache := &DecoderCache[K, V]{payload: fr.GetPayload()}
	actual, _ := m.cache.LoadOrStore(path, newCache)

	return actual.(*DecoderCache[K, V]).GetDecoded()
}

type DecoderCache[K types.Key, V types.Value] struct {
	payload []byte

	once    sync.Once
	decoded []types.Payload[K, V]
	err     error
}

func (dc *DecoderCache[K, V]) GetDecoded() ([]types.Payload[K, V], error) {
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
