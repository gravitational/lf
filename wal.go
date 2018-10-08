package lf

import (
	"bytes"
	"context"

	"github.com/gravitational/lf/walpb"

	"github.com/google/btree"
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
	case OpReopen:
		return walpb.Operation_REOPEN, nil
	default:
		return -1, trace.BadParameter("unsupported operation %v", o)
	}
}

func FromRecordOperation(op walpb.Operation) (OpType, error) {
	switch op {
	case walpb.Operation_CREATE:
		return OpCreate, nil
	case walpb.Operation_UPDATE:
		return OpUpdate, nil
	case walpb.Operation_PUT:
		return OpPut, nil
	case walpb.Operation_DELETE:
		return OpDelete, nil
	case walpb.Operation_REOPEN:
		return OpReopen, nil
	default:
		return 0, trace.BadParameter("unsupported operation %v", op)
	}
}

const (
	OpCreate OpType = iota
	OpUpdate OpType = iota
	OpPut    OpType = iota
	OpDelete OpType = iota
	OpReopen OpType = iota
)

// Item is a key value item
type Item struct {
	// Key is a key of the key value item
	Key []byte
	// Val is a value of the key value item
	Val []byte
	// ID is a record ID
	// that is auto incremented with every operation
	ID uint64
}

// Less is used for Btree operations,
// returns true if item is less than the other one
func (i *Item) Less(iother btree.Item) bool {
	switch other := iother.(type) {
	case *Item:
		return bytes.Compare(i.Key, other.Key) < 0
	case *prefixItem:
		return !iother.Less(i)
	default:
		return false
	}
}

// Record is a record containing operation
type Record struct {
	// Type is operation type
	Type OpType
	// Key is a key to perform operation on
	Key []byte
	// Val is a value to perform operation on
	Val []byte
	// ProcessID is a record process id
	ProcessID uint64
	// ID is internal record id
	ID uint64
}

// CheckAndSetDefaults checks record values
func (r *Record) CheckAndSetDefaults() error {
	_, err := r.Type.Operation()
	if err != nil {
		return trace.Wrap(err)
	}
	if r.Type != OpReopen && len(r.Key) == 0 {
		return trace.BadParameter("missing parameter key for record type %v", r.Type)
	}
	return nil
}

type Options struct {
	Prefix      bool
	RangeEndKey []byte
}

type Option func(o *Options) error

type GetResult struct {
	Items []Item
}

// WithPrefix enables 'Get' or'Watch' requests to operate
// on the keys with matching prefix. For example, 'Get(foo, WithPrefix())'
// can return 'foo1', 'foo2', and so on.
func WithPrefix() Option {
	return func(o *Options) error {
		o.Prefix = true
		return nil
	}
}

// Log is operation log, it serializes
// operations to the external storage and reads them
type Log interface {
	// CreateWatcher creates
	//CreateWatcher(ctx context.Context) Watcher
	// Append appends record
	Append(ctx context.Context, r Record) error
	// Close closes all associated resources
	Close() error
}

type Watcher interface {
}
