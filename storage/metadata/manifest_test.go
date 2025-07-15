package metadata

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestManifest_GetLevel(t *testing.T) {
	type fields struct {
		name string
		lsm0 *LSM
	}
	type args struct {
		l int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Level
		wantErr bool
	}{
		{
			name:    "-1 level",
			fields:  fields{name: "test", lsm0: NewLSM("test")},
			args:    args{-1},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "-1 level",
			fields:  fields{name: "test", lsm0: NewLSM("test")},
			args:    args{10},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Manifest{
				name: tt.fields.name,
				lsm0: tt.fields.lsm0,
			}
			level, err := tr.GetLSM().GetLevel(tt.args.l)
			if (err != nil) != tt.wantErr {
				t.Errorf("error() %v", err.Error())
			}
			if got := level; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Manifest.GetLevel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestManifest_Load(t *testing.T) {

	tmpDir := t.TempDir()

	const testName = "test-db"
	const manifestFile = "manifest.json"
	manifestPath := filepath.Join(tmpDir, "manifest", testName, manifestFile)

	m := NewManifest(testName, ManifestOpts{Dir: tmpDir})
	err := m.Load()
	assert.NoError(t, err)

	// File should be created
	_, statErr := os.Stat(manifestPath)
	assert.NoError(t, statErr)

	// LSM object should be non-nil
	assert.NotNil(t, m.lsm0)

	// ---- Test case 2: file exists, should load data into LSM ----
	// Simulate updated file content
	lsmDataView := NewLSMView(testName)
	lsmData := lsmDataView.ToLSM()

	jsonData, err := json.Marshal(lsmDataView)
	assert.NoError(t, err)
	err = os.WriteFile(manifestPath, jsonData, 0644)
	assert.NoError(t, err)

	// Reload
	m2 := NewManifest(testName, ManifestOpts{Dir: tmpDir})
	err = m2.Load()
	assert.NoError(t, err)
	assert.NotNil(t, m2.lsm0)
	assert.Equal(t, m2.lsm0, lsmData)

	// for cleanup
	time.Sleep(100 * time.Millisecond)
}

func TestManifest_Sync(t *testing.T) {

	tmpDir := t.TempDir()

	const testName = "test-db"
	const manifestFile = "manifest.json"
	manifestPath := filepath.Join(tmpDir, "manifest", testName, manifestFile)

	m := NewManifest(testName, ManifestOpts{Dir: tmpDir})
	err := m.Load()
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go m.Sync(ctx)

	for range 10 {
		level, _ := m.lsm0.GetLevel(0)
		nextId := level.GetNextId()
		level.SetSSTable(nextId, NewSSTable("dummy", 0))
	}

	time.Sleep(4 * time.Second)

	lsmDataView := NewLSMView(testName)
	jsonData, err := json.Marshal(lsmDataView)
	assert.NoError(t, err)
	err = os.WriteFile(manifestPath, jsonData, 0644)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
}
