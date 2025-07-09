package memtable

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

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

func TestMemtable_Write_And_Read_In_Mem(t *testing.T) {
	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: t.TempDir()})
	mts := NewMemtableStore[string, StringValue](mf, MemtableOpts{MemtableSoftLimit: 1024, QueueHardLimit: 10})
	k, v := "key-0", StringValue{"val-0"}
	mts.Write(k, v)

	log.Println("Write successfull")

	val, ok := mts.Read(k)

	if !ok || val != v {
		t.Errorf("Expected %v, got %v", v, val)
	}
}

func TestMemtable_Write_Overflow_Trigger_Flush(t *testing.T) {

	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: t.TempDir()})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go mf.Sync(ctx)

	mts := NewMemtableStore[int, IntValue](mf, MemtableOpts{MemtableSoftLimit: 1024})
	d := IntValue{0}

	// overflow memtable to trigger flush
	for i := range int(1024 / d.SizeOf()) {
		mts.Write(i, IntValue{V: int32(i)})
	}

	k, v := 90892389, IntValue{1993920}
	if ok := mts.Write(k, v); !ok {
		t.Errorf("Expected to trigger flush")
	}

	// wait for memtable to flush & clear both memtable
	time.Sleep(3 * time.Second)

	mts.Clear()

	val, ok := mts.Read(244)
	v = IntValue{244}

	if !ok || val != v {
		t.Errorf("Expected %v, got %v", v, val)
	}
}

func TestMemtable_Write_With_Multiple_Reader(t *testing.T) {
	const MEMTABLE_THRESHOLD = 1024
	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: t.TempDir()})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go mf.Sync(ctx)

	// overflow first memtable to trigger flush
	mts := NewMemtableStore[int, IntValue](mf, MemtableOpts{MemtableSoftLimit: MEMTABLE_THRESHOLD})
	d := IntValue{0}
	for i := range int(MEMTABLE_THRESHOLD / d.SizeOf()) {
		mts.Write(i, IntValue{V: int32(i)})
	}
	offset := int(MEMTABLE_THRESHOLD / d.SizeOf())
	for i := range int(MEMTABLE_THRESHOLD / d.SizeOf()) {
		mts.Write(i+offset, IntValue{V: int32(i + offset)})
	}

	// A small gap to let it flush to disk & erase
	// further read should come from disk sst
	time.Sleep(3 * time.Second)
	wg := sync.WaitGroup{}

	for i := range int(MEMTABLE_THRESHOLD / d.SizeOf()) {
		wg.Add(1)
		func(i int) {
			val, ok := mts.Read(i + 10)
			v := IntValue{int32(i + 10)}
			if !ok || val != v {
				t.Errorf("Expected %v, got %v", v, val)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

}

func TestMemtable_Intensive_Write_And_Read(t *testing.T) {

	const MEMTABLE_THRESHOLD = 1024
	temp := t.TempDir()
	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: temp})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go mf.Sync(ctx)

	// overflow first memtable to trigger flush
	mts := NewMemtableStore[int, IntValue](mf, MemtableOpts{MemtableSoftLimit: MEMTABLE_THRESHOLD})
	d := IntValue{0}

	multiples := 10
	totalOps := int(MEMTABLE_THRESHOLD/d.SizeOf()) * multiples

	for i := range totalOps {
		mts.Write(i, IntValue{V: int32(i)})
	}

	// A small gap to let it flush to disk & erase
	// further read should come from disk sst
	time.Sleep(3 * time.Second)
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
	elapsed := time.Since(start)
	opsPerSec := float64(totalOps) / elapsed.Seconds()
	t.Logf("Total time taken: %v, Ops/sec: %.2f", elapsed, opsPerSec)

	wg.Wait()
}
