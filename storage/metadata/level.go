package metadata

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type Level struct {
	tables []*SSTable

	SizeInBytes atomic.Int64

	// @todo: create separate locks for each field
	mu *sync.RWMutex
	// mutex to lock rw on levels, one must acquire respective level lock for
	// updating level
}

func NewLevel() *Level {
	return &Level{tables: []*SSTable{}, mu: &sync.RWMutex{}}
}

func (t *Level) AppendSSTable(table *SSTable) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.SizeInBytes.Add(table.SizeInBytes)
	t.tables = append(t.tables, table)
}

func (t *Level) SetSSTable(i int, table *SSTable) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tables[i] = table
}

func (t *Level) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.tables)
}

func (t *Level) GetTables() []*SSTable {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tables
}

func (t *Level) GetTable(i int) (*SSTable, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.tables) <= i || i < 0 {
		return nil, fmt.Errorf("failed to get index")
	}
	return t.tables[i], nil
}

// Level snapshot
// Note: snapshots are immutable, exported fields are kept
//		 for json marshalling
// Warning!: it is not advised to modify snapshot views

type LevelView struct {
	Tables []SSTableView `json:"tables"`
}

func NewLevelView() LevelView {
	return LevelView{Tables: []SSTableView{}}
}
