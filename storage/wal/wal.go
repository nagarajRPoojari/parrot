package wal

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"time"

	"github.com/nagarajRPoojari/lsm/storage/utils/log"

	"sync"

	customerr "github.com/nagarajRPoojari/lsm/storage/errors"

	fio "github.com/nagarajRPoojari/lsm/storage/io"
)

type Event interface {
}

type WAL[E Event] struct {
	mu         sync.Mutex
	path       string
	fileWriter *fio.FileWriter
	encoder    *gob.Encoder

	bufferedWriter *bufio.Writer

	eventCh chan E
	closeCh chan struct{}
	wg      sync.WaitGroup

	once sync.Once
}

func NewWAL[E Event](path string) (*WAL[E], error) {

	fm := fio.GetFileManager()
	fw := fm.OpenForAppend(path)

	bw := bufio.NewWriterSize(fw.GetFile(), 4*1024*1024)
	w := &WAL[E]{
		fileWriter:     fw,
		path:           path,
		eventCh:        make(chan E, 1024),
		closeCh:        make(chan struct{}),
		encoder:        gob.NewEncoder(bw),
		once:           sync.Once{},
		bufferedWriter: bw,
	}

	w.wg.Add(1)
	go w.run()

	return w, nil
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

func (w *WAL[E]) Append(entry E) {
	w.eventCh <- entry
}

func (t *WAL[E]) run() {
	flushTicker := time.NewTicker(100 * time.Microsecond)
	defer flushTicker.Stop()

	for {
		select {
		case event := <-t.eventCh:
			t.write(event)

		case <-flushTicker.C:
			if err := t.flush(); err != nil {
			}

		case <-t.closeCh:
			t.drain()
			t.flush() // final flush before exiting
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

func (t *WAL[E]) flush() error {
	if err := t.bufferedWriter.Flush(); err != nil {
		return err
	}
	return nil
}

func (t *WAL[E]) Truncate() {
	t.fileWriter.Truncate(0)
}

func (t *WAL[E]) Delete() {
	fm := fio.GetFileManager()
	fm.Delete(t.path)
}

func (t *WAL[E]) drain() {
	for {
		select {
		case event := <-t.eventCh:
			t.write(event)
		default:
			t.fileWriter.Close()
			return
		}
	}
}

// Close gracefully shuts down the WAL[E].
func (t *WAL[E]) Close() {
	close(t.closeCh)
	t.wg.Wait()
}
