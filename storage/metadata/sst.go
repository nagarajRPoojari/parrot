package metadata

type SSTable struct {
	Path string
}

func NewSSTable(path string) *SSTable {
	return &SSTable{Path: path}
}

// SSTable snapshot
// Note: snapshots are immutable, exported fields are kept
//		 for json marshalling
// Warning!: it is not advised to modify snapshot views

type SSTableView struct {
	Path string `json:"path"`
}

func NewSSTableView(path string) SSTable {
	return SSTable{Path: path}
}
