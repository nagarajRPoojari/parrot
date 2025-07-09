package compactor

import (
	"context"
	"time"

	"github.com/nagarajRPoojari/lsm/storage/metadata"
)

type GC struct {
	mf *metadata.Manifest
}

func NewGC(mf *metadata.Manifest) *GC {
	return &GC{mf}
}

func (t *GC) Run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:

		}

	}
}

// func (t *GC) RunCompaction() {
// 	level0, err := t.mf.GetLSM().GetLevel(0)
// 	if err != nil {
// 		fmt.Println(err)
// 	}

// 	table0, err := level0.GetTable(0)

// 	size, err := utils.DirSize(table0.Path)

// }
