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

// Flusher handles asynchronous flushing of memtables.
// Responsibilities:
//   - Persists flushable memtable data to disk & updates manifest
//   - Frees associated in-memory memtable resources
//   - Optionally deletes the WAL file associated with the flushed memtable
type Flusher[K types.Key, V types.Value] struct {
	// q holds memtable nodes that are pending flush
	q *Queue[K, V]

	// mf manages storage metadata, such as table manifests
	mf *metadata.Manifest

	// opts holds configuration options for the flushing process
	opts FlusherOpts
}

// NewFlusher creates new instance of Flusher
func NewFlusher[K types.Key, V types.Value](q *Queue[K, V], mf *metadata.Manifest, opts FlusherOpts) *Flusher[K, V] {
	return &Flusher[K, V]{
		opts: opts,
		q:    q,
		mf:   mf,
	}
}

func (t *Flusher[K, V]) Run() {
	for {
		// Pop waits for lock, which will be available on when atleast one
		// disposable memtable available
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

	// order of update:
	//	-	write new table to level-0
	//	-	update it in manifest
	//	-	flush memtable
	//	-	delete corresponding log file if wal turned on

	// write new table to disk (level-0)
	pls, totalSizeInBytes := mem.BuildPayloadList()
	err := utils.Encode(wt.GetFile(), pls)
	if err != nil {
		log.Panicf("failed to encode & store, error=%v", err)
	}

	// Ensure all buffered data is flushed to disk through fsync system call
	wt.GetFile().Sync()

	// append new table to level-0
	lvl, _ := t.mf.GetLSM().GetLevel(0)
	lvl.SetSSTable(nextId, metadata.NewSSTable(path, totalSizeInBytes))

	mem.mu.Lock()
	defer mem.mu.Unlock()

	for k := range mem.data {
		delete(mem.data, k)
	}

	// delete current memetable log file if wal is turned on
	if mem.opts.TurnOnWal {
		mem.wal.Delete()
	}

	log.Infof("deleted memtable at %s", path)
}
