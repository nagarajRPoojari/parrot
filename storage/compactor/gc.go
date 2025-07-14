package compactor

import (
	"container/heap"
	"context"
	"time"

	"github.com/nagarajRPoojari/lsm/storage/utils/log"

	"github.com/nagarajRPoojari/lsm/storage/cache"
	"github.com/nagarajRPoojari/lsm/storage/io"
	"github.com/nagarajRPoojari/lsm/storage/metadata"
	"github.com/nagarajRPoojari/lsm/storage/types"
	"github.com/nagarajRPoojari/lsm/storage/utils"
)

type GC[K types.Key, V types.Value] struct {
	mf       *metadata.Manifest
	cache    *cache.CacheManager[K, V]
	strategy CompactionStrategy[K, V]
}

func NewGC[K types.Key, V types.Value](mf *metadata.Manifest, cache *cache.CacheManager[K, V], strategy CompactionStrategy[K, V]) *GC[K, V] {
	return &GC[K, V]{mf, cache, strategy}
}

func (t *GC[K, V]) Run(ctx context.Context) {
	// @todo: read from config
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// gc should run synchronously
			t.strategy.Run(t.mf, t.cache, 0)
		}
	}
}

type CompactionStrategyOpts interface {
}

type CompactionStrategy[K types.Key, V types.Value] interface {
	Run(*metadata.Manifest, *cache.CacheManager[K, V], int)
}

type SizeTiredCompactionOpts struct {
	// softlimit on level=0
	Levle0MaxSizeInBytes int64
	// level-x-softlimit = level-0-softlimit * max(x * growth-factor, 1)
	MaxSizeInBytesGrowthFactor int32
}

type SizeTiredCompaction[K types.Key, V types.Value] struct {
	Opts SizeTiredCompactionOpts
}

func (t *SizeTiredCompaction[K, V]) Run(mf *metadata.Manifest, cache *cache.CacheManager[K, V], l int) {
	levelL, err := mf.GetLSM().GetLevel(l)
	if err != nil {
		return
	}

	// path := mf.GetLevelPath(l)
	size := levelL.SizeInBytes.Load()
	if int64(size) > t.Opts.Levle0MaxSizeInBytes*max(int64(l)*int64(t.Opts.MaxSizeInBytesGrowthFactor), 1) {
		log.Infof("Size(level=%d)=%d, growth_factor=%d, l0MaxSize=%d", l, size, t.Opts.MaxSizeInBytesGrowthFactor, t.Opts.Levle0MaxSizeInBytes)
		log.Infof("Compaction started on level ", l)

		tablesCount := levelL.TablesCount()

		// Load all sst from level=l
		sstList := make([][]types.Payload[K, V], tablesCount)
		// total size of level=l ( sum of all sst size )
		totalSizeInBytes := 0
		keyCount := 0
		for i, table := range levelL.GetTables() {
			sst, err := cache.Get(table.Path)
			if err != nil {
				log.Panicf("failed to read sst while running gc")
			}
			sstList[i] = sst
			totalSizeInBytes += int(table.SizeInBytes)
			keyCount += len(sst)
		}

		// K-way merge using next-pointer min heap
		h := &MergerHeap[K, V]{h: make([]Pair[K, V], 0)}
		merged := make([]types.Payload[K, V], keyCount)

		// init with min payload(j=0) of all tables
		for i := range tablesCount {
			heap.Push(h, Pair[K, V]{pl: &sstList[i][0], I: i, J: 0})
		}

		for keyCount > 0 {
			// pop the minimum payload
			poped := heap.Pop(h).(Pair[K, V])
			merged[len(merged)-keyCount] = *poped.pl
			i, j := poped.I, poped.J

			// push the next pointed payload by current popped paylod
			if j < len(sstList[i])-1 {
				heap.Push(h, Pair[K, V]{pl: &sstList[i][j], I: i, J: j + 1})
			}
			keyCount--
		}

		// order of update:
		// - write merged sst to level-l+1
		// - update level-l+1 manifest
		// - update level-l manifest
		// - delete level-l[:tablesCount] ssts

		// save to file before updating manifest
		nextLevel, err := mf.GetLSM().GetLevel(l + 1)
		if err != nil {
			// indicates no next level, so create one
			mf.GetLSM().AppendLevel()
			nextLevel, _ = mf.GetLSM().GetLevel(l + 1)
		}

		manager := io.GetFileManager()
		path := mf.FormatPath(l+1, nextLevel.TablesCount())

		wt := manager.OpenForWrite(path)
		defer wt.Close()

		err = utils.Encode(wt.GetFile(), merged)
		if err != nil {
			log.Fatalf("error=%v\n", err)
		}

		log.Infof("LSM address - %p %p %p\n", mf.GetLSM(), levelL, nextLevel)

		// @todo: getPath & appendSSTable should be atomic
		// for now no two go routines can appendSSTable on same level
		// - only gc can append table for level > 0
		// - only flushed can append table for lebel = 0
		path = mf.FormatPath(l+1, nextLevel.TablesCount())
		nextLevel.AppendSSTable(metadata.NewSSTable(path, int64(totalSizeInBytes)))

		// @todo: by that time writer could have added new sst (specifically in case of level=0)
		old := levelL.Clear(tablesCount)

		for i, table := range old[:tablesCount] {
			if err := manager.Delete(table.Path); err != nil {
				log.Panicf("failed to delete %s, got error=%v", table.Path, err)
			}
			old[i] = nil
		}

		// adding new table to next level can lead to overflow
		t.Run(mf, cache, l+1)

	} else {
		return
	}

}
