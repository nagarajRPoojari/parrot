package compactor

import (
	"container/heap"
	"context"
	"time"

	"github.com/nagarajRPoojari/lsm/storage/utils/log"
	"github.com/nagarajRPoojari/lsm/storage/wal"

	"github.com/nagarajRPoojari/lsm/storage/cache"
	"github.com/nagarajRPoojari/lsm/storage/io"
	"github.com/nagarajRPoojari/lsm/storage/metadata"
	"github.com/nagarajRPoojari/lsm/storage/types"
	"github.com/nagarajRPoojari/lsm/storage/utils"
)

type Operation string

const (
	DeleteStarted   Operation = "DELETE_START"
	DeleteCompleted Operation = "DELETE_COMPLETE"
	WriteStarted    Operation = "WRITE_START"
	WriteCompleted  Operation = "WRITE_COMPLETE"
)

const (
	LOG_FILE = "./test.log"
)

type Event struct {
	Path string
	Op   Operation
}

type GC[K types.Key, V types.Value] struct {
	mf       *metadata.Manifest
	cache    *cache.CacheManager[K, V]
	strategy CompactionStrategy[K, V]
	wal      *wal.WAL[Event]
}

func NewGC[K types.Key, V types.Value](mf *metadata.Manifest, cache *cache.CacheManager[K, V], strategy CompactionStrategy[K, V]) *GC[K, V] {
	events, err := wal.Replay[Event](LOG_FILE)
	if err == nil {
		rollback(events)
	}

	wal, _ := wal.NewWAL[Event](LOG_FILE)
	gc := &GC[K, V]{mf, cache, strategy, wal}

	return gc
}

func rollback(events []Event) {
	partialDeletes := map[string]struct{}{}
	partialWrites := map[string]struct{}{}
	for _, event := range events {
		switch event.Op {
		case DeleteCompleted:
			delete(partialDeletes, event.Path)
		case WriteCompleted:
			delete(partialWrites, event.Path)
		case DeleteStarted:
			partialDeletes[event.Path] = struct{}{}
		case WriteStarted:
			partialWrites[event.Path] = struct{}{}
		}
	}

	fm := io.GetFileManager()
	for path := range partialDeletes {
		fm.Delete(path)
	}
	for path := range partialWrites {
		fm.Delete(path)
	}
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
			t.strategy.Run(t.mf, t.cache, t.wal, 0)
		}
	}
}

type CompactionStrategyOpts interface {
}

type CompactionStrategy[K types.Key, V types.Value] interface {
	Run(*metadata.Manifest, *cache.CacheManager[K, V], *wal.WAL[Event], int)
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

func (t *SizeTiredCompaction[K, V]) Run(mf *metadata.Manifest, cache *cache.CacheManager[K, V], wal *wal.WAL[Event], l int) {
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
		l0TablesIds := []int{}
		l0TablePaths := []string{}
		// Load all sst from level=l
		sstList := make([][]types.Payload[K, V], tablesCount)
		// total size of level=l ( sum of all sst size )
		totalSizeInBytes := 0
		keyCount := 0

		index := 0
		for id, table := range levelL.GetTables() {
			sst, err := cache.Get(table.Path)
			if err != nil {
				log.Panicf("failed to read sst while running gc")
			}
			sstList[index] = sst
			totalSizeInBytes += int(table.SizeInBytes)
			keyCount += len(sst)

			if index == tablesCount {
				break
			}

			index++

			l0TablesIds = append(l0TablesIds, id)
			l0TablePaths = append(l0TablePaths, table.Path)
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
				heap.Push(h, Pair[K, V]{pl: &sstList[i][j+1], I: i, J: j + 1})
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
		l1TablesNextId := nextLevel.GetNextId()
		path := mf.FormatPath(l+1, l1TablesNextId)

		wt := manager.OpenForWrite(path)
		defer wt.Close()

		wal.Append(Event{Path: path, Op: WriteStarted})
		err = utils.Encode(wt.GetFile(), merged)
		if err != nil {
			log.Fatalf("error=%v\n", err)
		}
		wal.Append(Event{Path: path, Op: WriteCompleted})

		// Ensure all buffered data is flushed to disk through fsync system call
		wt.GetFile().Sync()
		log.Infof("LSM address - %p %p %p\n", mf.GetLSM(), levelL, nextLevel)

		// @todo: getPath & SetSSTable should be atomic
		// for now no two go routines can SetSSTable on same level
		// - only gc can append table for level > 0
		// - only flusher can append table for lebel = 0
		nextLevel.SetSSTable(l1TablesNextId, metadata.NewSSTable(path, int64(totalSizeInBytes)))

		// @todo: by that time writer could have added new sst (specifically in case of level=0)
		levelL.Clear(l0TablesIds)

		// - Concurrent read routines may still be accessing these L0 files.
		// - Fortunately, the OS will not actually remove the files from disk
		//   until all file descriptors referencing them are closed.
		for _, path := range l0TablePaths {
			wal.Append(Event{Path: path, Op: DeleteStarted})
			if err := manager.Delete(path); err != nil {
				log.Panicf("failed to delete %s, got error=%v", path, err)
			}
			wal.Append(Event{Path: path, Op: DeleteCompleted})
		}

		// adding new table to next level can lead to overflow
		t.Run(mf, cache, wal, l+1)

	} else {
		return
	}

}
