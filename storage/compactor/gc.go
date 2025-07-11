package compactor

import (
	"context"
	"time"

	"github.com/nagarajRPoojari/lsm/storage/cache"
	"github.com/nagarajRPoojari/lsm/storage/metadata"
	"github.com/nagarajRPoojari/lsm/storage/types"
)

type GC[K types.Key, V types.Value] struct {
	mf    *metadata.Manifest
	cache *cache.DecoderCacheManager[K, V]
}

func NewGC[K types.Key, V types.Value](mf *metadata.Manifest, cache *cache.DecoderCacheManager[K, V]) *GC[K, V] {
	return &GC[K, V]{mf, cache}
}

func (t *GC[K, V]) Run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:

		}

	}
}

type CompactionStrategyOpts interface {
}

type CompactionStrategy[K types.Key, V types.Value] interface {
	Run(*metadata.Manifest, int)
}

type SizeTiredCompactionOpts struct {
	Levle0MaxSizeInBytes       int64
	MaxSizeInBytesGrowthFactor int32
}

type SizeTiredCompaction[K types.Key, V types.Value] struct {
	Opts SizeTiredCompactionOpts
}

// type GenericHeap[K types.Key, V types.Value] []types.Payload[K, V]

// func (h GenericHeap[K, V]) Len() int           { return len(h) }
// func (h GenericHeap[K, V]) Less(i, j int) bool { return h[i].Key.Less(h[j].Key) }
// func (h GenericHeap[K, V]) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// func (h *GenericHeap[K, V]) Push(x any) {
// 	*h = append(*h, x.(types.Payload[K, V]))
// }

// func (h *GenericHeap[K, V]) Pop() any {
// 	old := *h
// 	n := len(old)
// 	x := old[n-1]
// 	*h = old[0 : n-1]
// 	return x
// }

// // WIP
// func (t *SizeTiredCompaction[K, V]) Run(mf *metadata.Manifest, cache *cache.DecoderCacheManager[K, V], l int) {
// 	levelL, err := mf.GetLSM().GetLevel(l)
// 	if err != nil {
// 		fmt.Println(err)
// 	}

// 	// path := mf.GetLevelPath(l)
// 	size := levelL.SizeInBytes.Load()
// 	if int64(size) > t.Opts.Levle0MaxSizeInBytes*max(int64(l)*int64(t.Opts.MaxSizeInBytesGrowthFactor), 1) {
// 		log.Println("Compaction started on level ", l)

// 		sstList := make([][]types.Payload[K, V], 0)

// 		for _, table := range levelL.GetTables() {
// 			sst, err := cache.Get(table.Path)
// 			if err != nil {
// 				log.Fatalln("failed to read sst while running gc")
// 			}
// 			sstList = append(sstList, sst)
// 		}

// 		newSST := make([]types.Payload[K, V], 0)

// 		h := &GenericHeap[K, V]{}
// 		heap.Init(h)

// 	} else {
// 		return
// 	}

// }
