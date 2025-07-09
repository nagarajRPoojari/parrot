package memtable

import (
	"log"

	"github.com/nagarajRPoojari/lsm/storage/io"
	"github.com/nagarajRPoojari/lsm/storage/metadata"
	"github.com/nagarajRPoojari/lsm/storage/utils"
)

type FlusherOpts struct {
}

type Flusher[K utils.Key, V utils.Value] struct {
	q  *Queue[K, V]
	mf *metadata.Manifest

	opts FlusherOpts
}

func NewFlusher[K utils.Key, V utils.Value](q *Queue[K, V], mf *metadata.Manifest, opts FlusherOpts) *Flusher[K, V] {
	return &Flusher[K, V]{
		opts: opts,
		q:    q,
		mf:   mf,
	}
}

func (t *Flusher[K, V]) Run() {
	for {
		mem, err := t.q.Pop()
		if err == nil {
			t.flush(mem)
		}
	}
}

func (t *Flusher[K, V]) flush(mem *Memtable[K, V]) {
	log.Printf("deleting %p \n", mem)

	manager := io.GetFileManager()
	size, _ := t.mf.LevelSize(0)
	path := t.mf.GetPath(0, size)

	wt := manager.OpenForWrite(path)
	defer wt.Close()

	err := utils.Encode(wt.GetFile(), mem.BuildPayloadList())
	if err != nil {
		log.Fatal(err)
	}

	// update manifest, should acquire write lock over level-0
	lvl, _ := t.mf.GetLSM().GetLevel(0)
	lvl.AppendSSTable(metadata.NewSSTable(path))

	mem.mu.Lock()
	defer mem.mu.Unlock()

	for k := range mem.data {
		delete(mem.data, k)
	}

	log.Println("deleted memtable")
}
