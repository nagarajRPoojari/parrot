package main

import (
	"fmt"
	_ "net/http/pprof"
	"unsafe"

	"context"

	"github.com/nagarajRPoojari/lsm/storage"
	"github.com/nagarajRPoojari/lsm/storage/types"
)

type Value struct {
	Name string
	Age  int
}

func (t Value) SizeOf() uintptr {
	return uintptr(len(t.Name)) + unsafe.Sizeof(t.Age)
}

func main() {
	Run()
}

func Run() {

	dbName := "test"
	tempDir := "temp"

	const MEMTABLE_THRESHOLD = 1024 * 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := storage.NewStorage[types.IntKey, Value](
		dbName,
		ctx,
		storage.StorageOpts{
			Directory:         tempDir,
			MemtableThreshold: MEMTABLE_THRESHOLD,
			TurnOnCompaction:  true,
			TurnOnWal:         true,
			GCLogDir:          tempDir,
		})

	k, v := types.IntKey{K: 10}, Value{Name: "nagaraj", Age: 22}

	db.Put(k, v)

	readStatus := db.Get(k)
	fmt.Printf("value: %v \n", readStatus)
}
