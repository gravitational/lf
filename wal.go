package lf

import (
	"context"

	"github.com/gravitational/lf/walpb"

	"github.com/gravitational/trace"
)

type OpType int

func (o OpType) Operation() (walpb.Operation, error) {
	switch o {
	case OpCreate:
		return walpb.Operation_CREATE, nil
	case OpUpdate:
		return walpb.Operation_UPDATE, nil
	case OpPut:
		return walpb.Operation_PUT, nil
	case OpDelete:
		return walpb.Operation_DELETE, nil
	default:
		return -1, trace.BadParameter("unsupported operation %v", o)
	}
}

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
	// Val is a value to perform operation on
	Val []byte
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
