package metadata

type SSTable struct {
	Path        string
	SizeInBytes int64
}

func NewSSTable(path string, sizeInBytes int64) *SSTable {
	return &SSTable{Path: path, SizeInBytes: sizeInBytes}
}

// SSTable snapshot
// Note: snapshots are immutable, exported fields are kept
//		 for json marshalling
// Warning!: it is not advised to modify snapshot views

type SSTableView struct {
	Path        string `json:"path"`
	SizeInBytes int64  `json:"size"`
}

func NewSSTableView(path string, sizeInBytes int64) SSTable {
	return SSTable{Path: path, SizeInBytes: sizeInBytes}
}
