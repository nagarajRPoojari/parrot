package compactor

import (
	"context"
	"fmt"
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

type CompactionStrategyOpts interface {
}

type CompactionStrategy interface {
	Run(*metadata.Manifest, int)
}

type SizeTiredCompactionOpts struct {
	Levle0MaxSizeInBytes       int64
	MaxSizeInBytesGrowthFactor int32
}

type SizeTiredCompaction struct {
	Opts SizeTiredCompactionOpts
}

// WIP
func (t *SizeTiredCompaction) Run(mf *metadata.Manifest, l int) {
	levelL, err := mf.GetLSM().GetLevel(l)
	if err != nil {
		fmt.Println(err)
	}

	// path := mf.GetLevelPath(l)
	size := levelL.SizeInBytes.Load()
	if int64(size) > t.Opts.Levle0MaxSizeInBytes*max(int64(l)*int64(t.Opts.MaxSizeInBytesGrowthFactor), 1) {

	}

}
