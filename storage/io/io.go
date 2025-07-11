package io

import (
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/edsrzf/mmap-go"
)

type FileReader struct {
	payload mmap.MMap
	file    *os.File
}

func (t *FileReader) GetFile() *os.File {
	return t.file
}

func (t *FileReader) GetPayload() mmap.MMap {
	return t.payload
}

func (t *FileReader) Close() {
	t.file.Close()
	t.payload.Unmap()
}

type FileWriter struct {
	file *os.File
}

func (t *FileWriter) GetFile() *os.File {
	return t.file
}

func (t *FileWriter) Close() {
	t.file.Close()
}

func (t *FileWriter) Write(data []byte) {
	_, err := t.file.Write(data)
	if err != nil {
		log.Fatal(err)
	}

	// Sync call to flush data to disk
	t.file.Sync()
}

type FileManager struct {
	sharedFileReadersMap map[string]*FileReader
	lockMap              map[string]*sync.Mutex

	// globalMu prevents multiple goroutines creating same instance
	globalMu sync.Mutex
}

func newFileManager() *FileManager {
	return &FileManager{
		sharedFileReadersMap: map[string]*FileReader{},
	}
}

var singleInstance *FileManager
var once sync.Once

func GetFileManager() *FileManager {
	once.Do(func() { singleInstance = newFileManager() })
	return singleInstance
}

func (t *FileManager) openForSharedRead(path string) *FileReader {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	info, err := f.Stat()
	if err != nil {
		log.Fatal(err)
	}

	// empty files can't be opened through mmap
	if info.Size() == 0 || info.IsDir() {
		log.Fatalf("Invalid file for mmap: size=%d, isDir=%v", info.Size(), info.IsDir())
	}

	mmapData, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		log.Fatalf("unable to open mmap error=%v", err)
	}

	fileReader := &FileReader{payload: mmapData, file: f}
	return fileReader
}

func (t *FileManager) getOrCreateLock(path string) *sync.Mutex {
	t.globalMu.Lock()
	defer t.globalMu.Unlock()

	if t.lockMap == nil {
		t.lockMap = make(map[string]*sync.Mutex)
	}

	lock, ok := t.lockMap[path]
	if !ok {
		lock = &sync.Mutex{}
		t.lockMap[path] = lock
	}
	return lock
}

// OpenForRead utilizes mmap for multiple read only ops
// mmap with SHARED_READ points to common page cache
//
// !Caution: might not in sync with data on disk
//
//   - if write is using mmap with COPY mode, safe read but
//     needs fsync() to keep it sync
//
//   - if write is using mmap with RDWR mode or without mmap, might update same
//     page cache, leading to torn or corrupt data
func (t *FileManager) OpenForRead(path string) *FileReader {
	lock := t.getOrCreateLock(path)
	lock.Lock()
	defer lock.Unlock()

	if reader, ok := t.sharedFileReadersMap[path]; ok {
		return reader
	}
	reader := t.openForSharedRead(path)
	t.sharedFileReadersMap[path] = reader

	return reader
}

// OpenForWrite requires Close call to flush data to disk properly.
// Suitable for single write/dump
func (t *FileManager) OpenForWrite(path string) *FileWriter {
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatalf("failed to create directory %v: %v", dir, err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("failed to open file for writing %v", err)
	}

	return &FileWriter{file: f}
}
