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
)

type StringValue struct {
	v string
}

func (t StringValue) SizeOf() uintptr {
	return uintptr(len(t.v))
}

type IntValue struct {
	V int32
}

func (t IntValue) SizeOf() uintptr {
	return 4
}

func BenchmarkRead(b *testing.B) {
	log.SetOutput(io.Discard)

	const MEMTABLE_THRESHOLD = 1024
	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: b.TempDir()})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	go mf.Sync(ctx)

	// overflow first memtable to trigger flush
	mts := memtable.NewMemtableStore[int, IntValue](mf, memtable.MemtableOpts{MemtableSoftLimit: MEMTABLE_THRESHOLD})
	d := IntValue{0}

	for i := range int(MEMTABLE_THRESHOLD / d.SizeOf()) {
		mts.Write(i, IntValue{V: int32(i)})
	}

	max := 10000

	for i := range max {
		mts.Write(i, IntValue{V: int32(i)})
	}

	// A small gap to let it flush to disk & erase
	// further read should come from disk sst
	time.Sleep(3 * time.Second)

	wg := sync.WaitGroup{}

	start := time.Now()
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			val, ok := mts.Read(i)
			v := IntValue{int32(i)}
			if !ok || val != v {
				b.Errorf("Expected %v, got %v", v, val)
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	opsPerSec := float64(1000) / elapsed.Seconds()
	b.Logf("Total time taken: %v, Ops/sec: %.2f", elapsed, opsPerSec)

}

func BenchmarkMemtable_Intensive_Write_And_Read(t *testing.B) {

	const MEMTABLE_THRESHOLD = 1024 * 2
	temp := t.TempDir()
	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: temp})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go mf.Sync(ctx)

	// overflow first memtable to trigger flush
	mts := memtable.NewMemtableStore[int, IntValue](mf, memtable.MemtableOpts{MemtableSoftLimit: MEMTABLE_THRESHOLD})
	d := IntValue{0}

	multiples := 10
	totalOps := int(MEMTABLE_THRESHOLD/d.SizeOf()) * multiples

	for i := range totalOps {
		mts.Write(i, IntValue{V: int32(i)})
	}

	// A small gap to let it flush to disk & erase
	// further read should come from disk sst
	time.Sleep(2000 * time.Millisecond)
	wg := sync.WaitGroup{}

	start := time.Now()
	for i := 0; i < totalOps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			val, ok := mts.Read(i)
			v := IntValue{int32(i)}
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
