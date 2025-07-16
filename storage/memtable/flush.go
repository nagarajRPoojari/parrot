package memtable

import (
	"github.com/nagarajRPoojari/lsm/storage/utils/log"

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
	log.Infof("deleting %p \n", mem)

	manager := io.GetFileManager()
	l0, _ := t.mf.GetLSM().GetLevel(0)
	nextId := l0.GetNextId()
	path := t.mf.FormatPath(0, nextId)

	wt := manager.OpenForWrite(path)
	defer wt.Close()

	pls, totalSizeInBytes := mem.BuildPayloadList()
	err := utils.Encode(wt.GetFile(), pls)
	if err != nil {
		log.Panicf("failed to encode & store, error=%v", err)
	}

	// Ensure all buffered data is flushed to disk through fsync system call
	wt.GetFile().Sync()

	// update manifest, should acquire write lock over level-0
	lvl, _ := t.mf.GetLSM().GetLevel(0)
	lvl.SetSSTable(nextId, metadata.NewSSTable(path, totalSizeInBytes))

	mem.mu.Lock()
	defer mem.mu.Unlock()

	for k := range mem.data {
		delete(mem.data, k)
	}

	// delete current memetable log file

	mem.wal.Delete()

	log.Infof("deleted memtable at %s", path)
}
