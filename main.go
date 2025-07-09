package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/nagarajRPoojari/lsm/storage/memtable"
	"github.com/nagarajRPoojari/lsm/storage/metadata"
)

func main() {
	go http.ListenAndServe("localhost:6060", nil)
	log.SetOutput(io.Discard)

	cpuFile, err := os.Create("prof/cpu.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer cpuFile.Close()

	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}

	Run()

	pprof.StopCPUProfile()

	memFile, err := os.Create("prof/mem.prof")
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}
	defer memFile.Close()

	runtime.GC()

	if err := pprof.WriteHeapProfile(memFile); err != nil {
		log.Fatal("could not write memory profile: ", err)
	}
}

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
func Run() {
	const MEMTABLE_THRESHOLD = 1024
	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: os.TempDir()})
	mf.Load()

	ctx, _ := context.WithCancel(context.Background())
	// b.Cleanup(cancel)

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
				fmt.Printf("Expected %v, got %v", v, val)
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	opsPerSec := float64(1000) / elapsed.Seconds()
	fmt.Printf("Total time taken: %v, Ops/sec: %.2f", elapsed, opsPerSec)
}
