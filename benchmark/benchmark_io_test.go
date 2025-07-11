package storage

import (
	"context"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/nagarajRPoojari/lsm/storage/memtable"
	"github.com/nagarajRPoojari/lsm/storage/metadata"
	"github.com/nagarajRPoojari/lsm/storage/types"
)

// BenchmarkMemtable_Intensive_Read benchmarks intensive concurrent reads
// after flushing a large number of entries to disk-backed memtables.
//
//   - sst/memtable size is set to 2kb
//   - WAL is disabled
//   - cache & manifest sync() are enabled
//   - limits concurrent read threads to 5000 to prevent lock starvation
func BenchmarkMemtable_Intensive_Read(t *testing.B) {
	log.SetOutput(io.Discard)

	const MEMTABLE_THRESHOLD = 1024 * 2
	const MAX_CONCURRENT_READ_ROUTINES = 5000

	temp := t.TempDir()
	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: temp})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go mf.Sync(ctx)

	// overflow first memtable to trigger flush
	mts := memtable.NewMemtableStore[types.IntKey, types.IntValue](mf, memtable.MemtableOpts{MemtableSoftLimit: MEMTABLE_THRESHOLD})
	d := types.IntValue{V: 0}

	multiples := 50
	totalOps := int(MEMTABLE_THRESHOLD/d.SizeOf()) * multiples

	for i := range totalOps {
		mts.Write(types.IntKey{K: i}, types.IntValue{V: int32(i)})
	}

	// A small gap to let it flush to disk & erase
	// further read should come from disk sst
	time.Sleep(2000 * time.Millisecond)
	wg := sync.WaitGroup{}

	start := time.Now()

	ticket := make(chan struct{}, MAX_CONCURRENT_READ_ROUTINES)

	for i := range totalOps {
		wg.Add(1)

		go func(i int) {
			ticket <- struct{}{} // acquire a ticket
			defer func() {
				<-ticket // release the ticket
				wg.Done()
			}()
			val, ok := mts.Read(types.IntKey{K: i})
			v := types.IntValue{V: int32(i)}
			if !ok || val != v {
				t.Errorf("Expected %v, got %v", v, val)
			}
		}(i)
	}

	wg.Wait()
	t.Logf("total ops = %d", totalOps)
	elapsed := time.Since(start)
	opsPerSec := float64(totalOps) / elapsed.Seconds()
	t.Logf("Total time taken: %v, Ops/sec: %.2f", elapsed, opsPerSec)
}

// BenchmarkMemtable_Intensive_Write benchmarks intensive serial writes
//
//   - sst/memtable size is set to 2kb
//   - WAL is disabled
//   - cache & manifest sync() are enabled
func BenchmarkMemtable_Intensive_Write(t *testing.B) {
	log.SetOutput(io.Discard)

	const MEMTABLE_THRESHOLD = 1024 * 2
	temp := t.TempDir()
	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: temp})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go mf.Sync(ctx)

	// overflow first memtable to trigger flush
	mts := memtable.NewMemtableStore[types.IntKey, types.IntValue](mf, memtable.MemtableOpts{MemtableSoftLimit: MEMTABLE_THRESHOLD})
	d := types.IntValue{V: 0}

	multiples := 10
	totalOps := int(MEMTABLE_THRESHOLD/d.SizeOf()) * multiples

	start := time.Now()

	for i := range totalOps {
		mts.Write(types.IntKey{K: i}, types.IntValue{V: int32(i)})
	}

	t.Logf("total ops = %d", totalOps)
	elapsed := time.Since(start)
	opsPerSec := float64(totalOps) / elapsed.Seconds()
	t.Logf("Total time taken: %v, Ops/sec: %.2f", elapsed, opsPerSec)
}
