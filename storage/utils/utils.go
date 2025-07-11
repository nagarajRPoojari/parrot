package utils

import (
	"encoding/gob"
	"os"

	"github.com/nagarajRPoojari/lsm/storage/types"
)

func Encode[K types.Key, V types.Value](file *os.File, kv []types.Payload[K, V]) error {
	encoder := gob.NewEncoder(file)
	for _, user := range kv {
		if err := encoder.Encode(user); err != nil {
			return err
		}
	}
	return nil
}
