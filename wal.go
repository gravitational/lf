package lf

import (
	"context"
)

type OpType int

const (
	OpCreate OpType = iota
	OpUpdate OpType = iota
	OpPut    OpType = iota
	OpDelete OpType = iota
)

// Record is a record containing operation
type Record struct {
	// Type is operation type
	Type OpType
	// Key is a key to perform operation on
	Key []byte
	// Value is a value to perform operation on
	Value []byte
}

// Log is operation log, it serializes
// operations to the external storage and reads them
type Log interface {
	// CreateWatcher creates
	//CreateWatcher(ctx context.Context) Watcher
	// Append appends record
	Append(ctx context.Context, r Record) error
	// Close closes all associated resources
}

type Watcher interface {
}
