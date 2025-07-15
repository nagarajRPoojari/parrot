package wal

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/nagarajRPoojari/lsm/storage/io"
)

type Event struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Op    string `json:"op"`
}

type WAL struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer

	eventCh chan Event
	closeCh chan struct{}
	wg      sync.WaitGroup
}

func NewWAL(path string) (*WAL, error) {

	fm := io.GetFileManager()
	fw := fm.OpenForAppend(path)

	w := &WAL{
		file:    fw.GetFile(),
		writer:  bufio.NewWriter(fw.GetFile()),
		eventCh: make(chan Event, 1024),
		closeCh: make(chan struct{}),
	}

	w.wg.Add(1)
	go w.run()

	return w, nil
}

func (w *WAL) Append(entry Event) {
	w.eventCh <- entry
}

func (t *WAL) run() {
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case event := <-t.eventCh:
			t.write(event)
		case <-t.closeCh:
			t.drain()
			return
		case <-ticker.C:
			t.flush()
		}
	}
}

// write writes a single entry to the WAL.
func (t *WAL) write(entry Event) {
	t.mu.Lock()
	defer t.mu.Unlock()

	data, err := json.Marshal(entry)
	if err == nil {
		t.writer.Write(data)
		t.writer.WriteByte('\n')
	}
}

func (t *WAL) flush() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.writer.Flush()
	t.file.Sync()
}

func (t *WAL) drain() {
	for {
		select {
		case event := <-t.eventCh:
			t.write(event)
		default:
			t.flush()
			t.file.Close()
			return
		}
	}
}

// Close gracefully shuts down the WAL.
func (t *WAL) Close() {
	close(t.closeCh)
	t.wg.Wait()
}
