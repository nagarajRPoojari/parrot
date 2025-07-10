package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/nagarajRPoojari/lsm/storage/io"
)

const (
	MANIFEST = "manifest"
)

type ManifestOpts struct {
	Dir string
}

type Manifest struct {
	name string
	lsm0 *LSM

	opts ManifestOpts
}

func NewManifest(name string, opts ManifestOpts) *Manifest {
	return &Manifest{name: name, lsm0: NewLSM(name), opts: opts}
}

func (t *Manifest) Load() error {
	filePath := path.Join(t.opts.Dir, MANIFEST, t.name, fmt.Sprintf("%s.json", MANIFEST))
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// create an empty LSM, take a snapshot & save
			lsm := NewLSM(t.name)
			lsmView := lsm.ToView()
			emptyData, _ := json.Marshal(lsmView)

			fm := io.GetFileManager()
			fw := fm.OpenForWrite(filePath)
			fw.Write(emptyData)
			fw.Close()

			t.lsm0 = lsm
			return nil
		} else {
			return err
		}
	}

	// load lsmview/snapshot to new LSM
	lsmView := NewLSMView(t.name)
	_ = json.Unmarshal(data, lsmView)
	t.lsm0 = lsmView.ToLSM()
	return nil
}

func (t *Manifest) Sync(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			filePath := path.Join(t.opts.Dir, MANIFEST, t.name, fmt.Sprintf("%s.json", MANIFEST))

			// load consistent manifest snapshot
			// reason: json needs struct to export fields with no locks
			//		   lsm is rw protected through locks, using lsm directly might lead to data race
			lsmView := t.lsm0.ToView()

			data, err := json.Marshal(lsmView)
			if err != nil {
				return err
			}

			fw := io.GetFileManager().OpenForWrite(filePath)
			fw.Write(data)
			fw.Close()

			// @todo: remove
			// info, err := os.Stat(filePath)
			// if err != nil {
			// } else {
			// }
		}
	}
}

func (t *Manifest) GetPath(l, i int) string {
	if l < 0 || i < 0 {
		return ""
	}

	return path.Join(t.opts.Dir, t.lsm0.GetName(), fmt.Sprintf("level-%d", l), fmt.Sprintf("sst-%d.db", i))
}

func (t *Manifest) GetLevelPath(l int) string {
	if l < 0 {
		return ""
	}

	return path.Join(t.opts.Dir, t.lsm0.GetName(), fmt.Sprintf("level-%d", l))
}

// redundant: GetLSM().LevelsCount() should do this job
func (t *Manifest) LevelSize(l int) (int, error) {
	if t.lsm0.LevelsCount() <= l || l < 0 {
		return 0, fmt.Errorf("failed to get index")
	}
	level, _ := t.lsm0.GetLevel(l)
	return level.Size(), nil
}

func (t *Manifest) GetLSM() *LSM {
	// it is safe to return lsm instance, lock management is done by LSM itself
	return t.lsm0
}
