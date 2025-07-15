package storage

import (
	"context"
	"os"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/nagarajRPoojari/lsm/storage"
	"github.com/nagarajRPoojari/lsm/storage/utils/log"

	"github.com/nagarajRPoojari/lsm/storage/memtable"
	"github.com/nagarajRPoojari/lsm/storage/metadata"
	"github.com/nagarajRPoojari/lsm/storage/types"
)

const MILLION = 10_00_000

// BenchmarkMemtable_Intensive_Read benchmarks intensive concurrent reads
// after flushing a large number of entries to disk-backed memtables.
//
//   - sst/memtable size is set to 2kb
//   - WAL is disabled
//   - cache & manifest sync() are enabled
//   - limits concurrent read threads to 5000 to prevent lock starvation
func BenchmarkMemtable_Intensive_Read(t *testing.B) {
	log.Disable()

	dbName := "test"

	const MEMTABLE_THRESHOLD = 1024 * 2
	const MAX_CONCURRENT_READ_ROUTINES = 500
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	db := storage.NewStorage[types.IntKey, types.IntValue](
		dbName,
		ctx,
		storage.StorageOpts{
			WriteQueueSize:    1000,
			ReadWorkersCount:  500,
			ReadQueueSize:     1000,
			Directory:         ".",
			MemtableThreshold: MEMTABLE_THRESHOLD,
			TurnOnCompaction:  true,
		})

	d := types.IntValue{V: 0}

	multiples := 50
	totalOps := int(MEMTABLE_THRESHOLD/d.SizeOf()) * multiples

	for i := range totalOps {
		db.Put(types.IntKey{K: i}, types.IntValue{V: int32(i)})
	}

	time.Sleep(1 * time.Second)
	wg := sync.WaitGroup{}

	start := time.Now()
	ticket := make(chan struct{}, MAX_CONCURRENT_READ_ROUTINES)
	for i := 0; i < totalOps; i++ {
		wg.Add(1)

		ticket <- struct{}{} // acquire ticket
		go func(i int) {
			defer func() {
				<-ticket // release ticket
				wg.Done()
			}()

			readStatus := db.Get(types.IntKey{K: i})
			v := types.IntValue{V: int32(i)}
			if readStatus.Err != nil || readStatus.Value != v {
				t.Errorf("Expected %v, got %v", v, readStatus)
			}
		}(i)
	}

	wg.Wait()
	t.Logf("total ops = %d", totalOps)
	elapsed := time.Since(start)

	opsPerSec := float64(totalOps) / elapsed.Seconds()
	t.Logf("Total time taken: %v, Ops/sec: %.2fM", elapsed, opsPerSec/MILLION)

	dumpGoroutines()
}

// BenchmarkMemtable_Intensive_Write benchmarks intensive serial writes
//
//   - sst/memtable size is set to 2kb
//   - WAL is disabled
//   - cache & manifest sync() are enabled
func BenchmarkMemtable_Intensive_Write(t *testing.B) {
	log.Disable()

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
	t.Logf("Total time taken: %v, Ops/sec: %.2fM", elapsed, opsPerSec/MILLION)

	dumpGoroutines()
}

func dumpGoroutines() {
	f, err := os.Create("../goroutine.prof")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	pprof.Lookup("goroutine").WriteTo(f, 1)
}
