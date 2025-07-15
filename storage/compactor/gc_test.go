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

	level1Path := fmt.Sprintf("%s/test/level-1", tempDir)
	entries, err := os.ReadDir(level1Path)
	if err != nil {
		t.Errorf("Expected to read %s, got error: %v", level1Path, err)
	}
	if len(entries) == 0 {
		t.Errorf("Expected %s to be non-empty, but it is empty", level1Path)
	}

}

func TestGC_Intensive(t *testing.T) {
	log.Disable()
	tempDir := t.TempDir()

	const MEMTABLE_THRESHOLD = 1024

	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: tempDir})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go mf.Sync(ctx)

	mts := memtable.NewMemtableStore[types.IntKey, types.IntValue](mf, memtable.MemtableOpts{MemtableSoftLimit: MEMTABLE_THRESHOLD})
	d := types.IntValue{V: 0}

	gc := NewGC(
		mf,
		(*cache.CacheManager[types.IntKey, types.IntValue])(mts.DecoderCache),
		&SizeTiredCompaction[types.IntKey, types.IntValue]{
			Opts: SizeTiredCompactionOpts{
				Levle0MaxSizeInBytes:       2 * MEMTABLE_THRESHOLD, // softlimit = 2kb
				MaxSizeInBytesGrowthFactor: 2,                      // growth_factor = 2
			},
		},
	)
	go gc.Run(ctx)

	// overflow memtable to trigger flush
	multiples := 10
	totalOps := int(MEMTABLE_THRESHOLD/d.SizeOf()) * multiples

	for i := range totalOps {
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

	level3Path := fmt.Sprintf("%s/test/level-3", tempDir)
	entries, err := os.ReadDir(level3Path)
	if err != nil {
		t.Errorf("Expected to read %s, got error: %v", level3Path, err)
	}
	if len(entries) == 0 {
		t.Errorf("Expected %s to be non-empty, but it is empty", level3Path)
	}
}
