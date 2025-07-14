package storage

import (
	"testing"

	"github.com/nagarajRPoojari/lsm/storage/types"
	"github.com/nagarajRPoojari/lsm/storage/utils/log"
)

func TestStorage_Load(t *testing.T) {
	dbName := "test"

	st := NewStorage[types.IntKey, types.IntValue](dbName,
		StorageOpts{
			WriteQueueSize:    100,
			ReadWorkersCount:  100,
			ReadQueueSize:     100,
			Directory:         ".",
			MemtableThreshold: 1024,
		})

	k := types.IntKey{K: 100}
	r := st.Put(k, types.IntValue{V: 189})
	log.Infof("put value: %v", r)
	v := st.Get(k)

	log.Infof("get value: %v", v)
}
