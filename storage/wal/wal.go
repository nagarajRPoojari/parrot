package wal

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"

	"github.com/nagarajRPoojari/lsm/storage/utils/log"

	"os"
	"sync"

	customerr "github.com/nagarajRPoojari/lsm/storage/errors"
	fio "github.com/nagarajRPoojari/lsm/storage/io"
)

type Event interface {
}

type WAL[E Event] struct {
	mu      sync.Mutex
	file    *os.File
	encoder *gob.Encoder

	eventCh chan E
	closeCh chan struct{}
	wg      sync.WaitGroup
}

func Replay[E Event](path string) ([]E, error) {
	fm := fio.GetFileManager()
	if !fm.Exists(path) {
		return nil, customerr.FileNotFoundError
	}
	fr, err := fm.OpenForRead(path)
	if err != nil {
		return nil, err
	}
	defer fr.Close()

	var events []E
	decoder := gob.NewDecoder(bytes.NewReader(fr.GetPayload()))

	for {
		var entry E
		err := decoder.Decode(&entry)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Errorf("failed to decode, err=%v", err)
		}
		events = append(events, entry)
	}
	return events, nil
}

func NewWAL[E Event](path string) (*WAL[E], error) {

	fm := fio.GetFileManager()
	fw := fm.OpenForAppend(path)

	w := &WAL[E]{
		file:    fw.GetFile(),
		eventCh: make(chan E, 1024),
		closeCh: make(chan struct{}),
		encoder: gob.NewEncoder(fw.GetFile()),
	}

	w.wg.Add(1)
	go w.run()

	return w, nil
}

func (w *WAL[E]) Append(entry E) {
	w.eventCh <- entry
}

func (t *WAL[E]) run() {
	for {
		select {
		case event := <-t.eventCh:
			t.write(event)
		case <-t.closeCh:
			t.drain()
			return
		}
	}
}

// write writes a single entry to the WAL[E].
func (t *WAL[E]) write(entry Event) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.encoder.Encode(entry); err != nil {
		log.Fatalf("failed to encode log event %v", err)
	}

}

func (t *WAL[E]) drain() {
	for {
		select {
		case event := <-t.eventCh:
			t.write(event)
		default:
			t.file.Close()
			return
		}
	}
}

// Close gracefully shuts down the WAL[E].
func (t *WAL[E]) Close() {
	close(t.closeCh)
	t.wg.Wait()
}
