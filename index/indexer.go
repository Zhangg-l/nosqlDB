package index

import "go_code/project13/rosedb/storage"

// Indexer the data index info, stored in skip list.
type Indexer struct {
	Meta    *storage.Meta
	FieldId uint32  // the file id of storing the data
	Offset  int64 // entry data query satrt position
}
