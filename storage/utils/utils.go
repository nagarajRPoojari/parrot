package utils

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"unsafe"

	fio "github.com/nagarajRPoojari/lsm/storage/io"
)

type Value interface {
	SizeOf() uintptr
}

type Key interface {
	comparable
}

func SizeOfValue[V any](v V) uintptr {
	return unsafe.Sizeof(v)
}

func SwapPointers[T any](a, b **T) {
	temp := *a
	*a = *b
	*b = temp
}

type Payload[K Key, V Value] struct {
	Key K
	Val V
}

func Encode[K Key, V Value](file *os.File, kv []Payload[K, V]) error {
	encoder := gob.NewEncoder(file)
	for _, user := range kv {
		if err := encoder.Encode(user); err != nil {
			return err
		}
	}
	return nil
}

func Decode[K Key, V Value](fr *fio.FileReader) ([]Payload[K, V], error) {
	reader := bytes.NewReader(fr.GetPayload())
	decoder := gob.NewDecoder(reader)

	var payloads []Payload[K, V]
	for {
		var entry Payload[K, V]
		err := decoder.Decode(&entry)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		payloads = append(payloads, entry)
	}
	return payloads, nil
}
