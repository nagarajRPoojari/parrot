package compactor

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nagarajRPoojari/lsm/storage/utils/log"

	"github.com/nagarajRPoojari/lsm/storage/cache"
	"github.com/nagarajRPoojari/lsm/storage/memtable"
	"github.com/nagarajRPoojari/lsm/storage/metadata"
	"github.com/nagarajRPoojari/lsm/storage/types"
)

func TestGC(t *testing.T) {
	log.Disable()
	tempDir := t.TempDir()

	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: tempDir})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go mf.Sync(ctx)

	mts := memtable.NewMemtableStore[types.IntKey, types.IntValue](mf, memtable.MemtableOpts{MemtableSoftLimit: 1024})
	d := types.IntValue{V: 0}

	gc := NewGC(
		mf,
		(*cache.CacheManager[types.IntKey, types.IntValue])(mts.DecoderCache),
		&SizeTiredCompaction[types.IntKey, types.IntValue]{Opts: SizeTiredCompactionOpts{Levle0MaxSizeInBytes: 1000, MaxSizeInBytesGrowthFactor: 10}},
	)
	go gc.Run(ctx)

	// overflow memtable to trigger flush
	for i := range int(1024 / d.SizeOf()) {
		mts.Write(types.IntKey{K: i}, types.IntValue{V: int32(i)})
	}

	k, v := types.IntKey{K: 90892389}, types.IntValue{V: 1993920}
	if ok := mts.Write(k, v); !ok {
		t.Errorf("Expected to trigger flush")
	}

	// wait for memtable to flush & clear both memtable
	time.Sleep(3 * time.Second)
	mts.Clear()

	val, ok := mts.Read(types.IntKey{K: 244})
	v = types.IntValue{V: 244}

	if !ok || val != v {
		t.Errorf("Expected %v, got %v", v, val)
	}

	// gc should have added new sst-0.db at level-1
	if _, err := os.Stat(fmt.Sprintf("%s/test/level-1/sst-0.db", tempDir)); err != nil {
		t.Errorf("Expected file %s/test/level-1/sst-0.db to exist, got error: %v", tempDir, err)
	}

	l1, err := mf.GetLSM().GetLevel(1)
	if err != nil {
		t.Errorf("Expected to have sst-0 under level-1, got error: %v", err)
	}
	if l1.TablesCount() != 1 {
		t.Errorf("Expected to have sst-0 under level-1, got tablesCount: %d", l1.TablesCount())
	}
}
