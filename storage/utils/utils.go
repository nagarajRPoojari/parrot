package utils

import (
	"encoding/gob"
	"os"
	"unsafe"
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
