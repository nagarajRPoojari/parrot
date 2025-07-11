package memtable

import (
	"log"

	"github.com/nagarajRPoojari/lsm/storage/io"
	"github.com/nagarajRPoojari/lsm/storage/metadata"
	"github.com/nagarajRPoojari/lsm/storage/types"
	"github.com/nagarajRPoojari/lsm/storage/utils"
)

type FlusherOpts struct {
}

type Flusher[K types.Key, V types.Value] struct {
	q  *Queue[K, V]
	mf *metadata.Manifest

	opts FlusherOpts
}

func NewFlusher[K types.Key, V types.Value](q *Queue[K, V], mf *metadata.Manifest, opts FlusherOpts) *Flusher[K, V] {
	return &Flusher[K, V]{
		opts: opts,
		q:    q,
		mf:   mf,
	}
}

func (t *Flusher[K, V]) Run() {
	for {
		t.q.Pop(t.flush)
	}
}

func (t *Flusher[K, V]) flush(mem *Memtable[K, V]) {
	log.Printf("deleting %p \n", mem)

	manager := io.GetFileManager()
	size, _ := t.mf.LevelSize(0)
	path := t.mf.GetPath(0, size)

	wt := manager.OpenForWrite(path)
	defer wt.Close()

	pls, totalSizeInBytes := mem.BuildPayloadList()
	err := utils.Encode(wt.GetFile(), pls)

	// Ensure all buffered data is flushed to disk through fsync system call
	wt.GetFile().Sync()

	if err != nil {
		log.Fatal(err)
	}

	// update manifest, should acquire write lock over level-0
	lvl, _ := t.mf.GetLSM().GetLevel(0)
	lvl.AppendSSTable(metadata.NewSSTable(path, totalSizeInBytes))

	mem.mu.Lock()
	defer mem.mu.Unlock()

	for k := range mem.data {
		delete(mem.data, k)
	}

	log.Println("deleted memtable at ", path)
}
