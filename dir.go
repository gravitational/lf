package lf

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	"github.com/gravitational/lf/fs"
	"github.com/gravitational/lf/walpb"

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	log "github.com/sirupsen/logrus"
)

const (
	// V1 is a current schema version
	V1 = 1
)

const (
	// stateFilename
	stateFilename = "state"
	// firstLogFileName
	firstLogFileName = "first"
	// secondLogFileName
	secondLogFileName = "second"
	// recordBatchSize is a batch up to 50 records
	// to read in watcher (to avoid holding lock for a long time)
	recordBatchSize = 50
	// componentLogFormat is a component used in logs
	componentLogFormat = "lf"
	// defaultPollPeriod is a default period between polling attempts
	defaultPollPeriod = time.Second
	// defaultCompactionPeriod is a default period between compactions
	defaultCompactionPeriod = 30 * time.Minute
	// defaultBTreeDegreee is a default degree of a B-Tree
	defaultBTreeDegree = 8
)

// DirLogConfig holds configuration for directory
type DirLogConfig struct {
	// Dir is a directory with log files
	Dir string
	// Context is a context for opening the
	// database, this process can take several seconds
	// during compactions, context allows to cancel
	// opening the database
	Context context.Context
	// PollPeriod is a period between
	// polling attempts, used in watchers
	PollPeriod time.Duration
	// CompactionPeriod is a period between compactions
	CompactionPeriod time.Duration
	// CompactionsDisabled turns compactions off
	CompactionsDisabled bool
	// BTreeDegree is a degree of B-Tree, 2 for example, will create a
	// 2-3-4 tree (each node contains 1-3 items and 2-4 children).
	BTreeDegree int
	// Repair launches repair operation on database open
	Repair bool
	// Clock is a clock for time-related operations
	Clock clockwork.Clock
}

// CheckAndSetDefaults checks and sets default values
func (cfg *DirLogConfig) CheckAndSetDefaults() error {
	if cfg.Dir == "" {
		return trace.BadParameter("missing parameter Dir")
	}
	if cfg.Context == nil {
		cfg.Context = context.Background()
	}
	if cfg.PollPeriod == 0 {
		cfg.PollPeriod = defaultPollPeriod
	}
	if cfg.CompactionPeriod == 0 {
		cfg.CompactionPeriod = defaultCompactionPeriod
	}
	if cfg.BTreeDegree <= 0 {
		cfg.BTreeDegree = defaultBTreeDegree
	}
	if cfg.Clock == nil {
		cfg.Clock = clockwork.NewRealClock()
	}
	return nil
}

// NewDirLog creates a new log entry writing files
// to the directory with given prefix
func NewDirLog(cfg DirLogConfig) (*DirLog, error) {
	if err := cfg.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}

	d := &DirLog{
		Mutex: &sync.Mutex{},
		Entry: log.WithFields(log.Fields{
			trace.Component: componentLogFormat,
		}),
		DirLogConfig: cfg,
		tree:         btree.New(cfg.BTreeDegree),
		heap:         NewMinHeap(),
		marshaler:    NewContainerMarshaler(),
	}
	d.pool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, ContainerSizeBytes)
		},
	}

	if err := d.open(); err != nil {
		return nil, trace.Wrap(err)
	}

	if d.Repair {
		if err := d.tryRepairAndReopen(cfg.Context); err != nil {
			return nil, trace.Wrap(err)
		}
	}
	if err := d.readFull(); err != nil {
		return nil, trace.Wrap(err)
	}

	if !d.CompactionsDisabled {
		go d.runPeriodicCompactions()
	}

	return d, nil
}

type DirLog struct {
	*sync.Mutex
	*log.Entry
	DirLogConfig
	// file is a currently opened log file
	file *os.File
	// pool contains slice bytes
	// re-used to marshal and unmarshal containers,
	// used to reduce memory allocations
	pool *sync.Pool
	// tree is a BTree with items
	tree *btree.BTree
	// heap is a min heap with expiry records
	heap *MinHeap
	// recordID is a current record id,
	// incremented on every record read
	recordID uint64
	// state is a current database state
	state walpb.State
	// marshaler is a container marshaler
	marshaler *ContainerMarshaler
}

// NewWatcher returns new watcher matching prefix,
// if prefix is supplied, only events matching the prefix
// will be returned, otherwise, all events will be returned
// offset is optional and is used to locate the proper offset
func (d *DirLog) NewWatcher(prefix []byte, offset *Offset) (*DirWatcher, error) {
	return NewWatcher(DirWatcherConfig{
		Dir:        d,
		Prefix:     prefix,
		Offset:     offset,
		PollPeriod: d.PollPeriod,
	})
}

func (d *DirLog) runPeriodicCompactions() {
	compactionTicker := time.NewTicker(d.CompactionPeriod)
	defer compactionTicker.Stop()

	retryTicker := time.NewTicker(time.Second)
	defer retryTicker.Stop()

	var retryChannel <-chan time.Time
compactloop:
	for {
		select {
		case <-retryChannel:
			err := d.tryCompactAndReopen(d.Context)
			if err == nil {
				retryChannel = nil
				continue compactloop
			}
			d.Debugf("Compact and reopen failed: %v, will retry %v.", err)
		case <-compactionTicker.C:
			for {
				err := d.tryCompactAndReopen(d.Context)
				if err == nil {
					continue compactloop
				}
				d.Debugf("Compact and reopen failed: %v, will retry: %v.", err)
				retryChannel = retryTicker.C
			}
		case <-d.Context.Done():
			d.Debugf("DirLog is closing, returning.")
		}
	}
}

// tryCompactAndReopen tries to compact the database,
// if it succeeds, it will reopen the database
func (d *DirLog) tryCompactAndReopen(ctx context.Context) error {
	d.Lock()
	defer d.Unlock()
	if err := d.tryCompact(ctx, false); err != nil {
		return trace.Wrap(err)
	}
	if err := d.closeWithoutLock(); err != nil {
		return trace.Wrap(err)
	}
	return d.open()
}

// tryRepairAndReopen, will attempt to repair the database
// if it succeeds, it will reopen the database
func (d *DirLog) tryRepairAndReopen(ctx context.Context) error {
	d.Lock()
	defer d.Unlock()
	if err := d.tryCompact(ctx, true); err != nil {
		return trace.Wrap(err)
	}
	if err := d.closeWithoutLock(); err != nil {
		return trace.Wrap(err)
	}
	return d.open()
}

// tryCompact attempts to grab locks and compact files
// it never waits for lock forever to avoid deadlocks (as it tries
// to grab multiple files at once)
// tryCompact assumes that the database is opened
// by this dir log
func (d *DirLog) tryCompact(ctx context.Context, repair bool) error {
	// 1. grab write locks on state file and both log files
	stateFile, err := os.OpenFile(filepath.Join(d.Dir, stateFilename), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return trace.ConvertSystemError(err)
	}
	defer stateFile.Close()
	if err := fs.TryWriteLock(stateFile); err != nil {
		return trace.Wrap(err)
	}
	defer fs.Unlock(stateFile)

	firstFile, err := os.OpenFile(filepath.Join(d.Dir, firstLogFileName), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return trace.ConvertSystemError(err)
	}
	defer firstFile.Close()
	if err := fs.TryWriteLock(firstFile); err != nil {
		return trace.Wrap(err)
	}
	defer fs.Unlock(firstFile)

	secondFile, err := os.OpenFile(filepath.Join(d.Dir, secondLogFileName), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return trace.ConvertSystemError(err)
	}
	defer secondFile.Close()
	if err := fs.TryWriteLock(secondFile); err != nil {
		return trace.Wrap(err)
	}
	defer fs.Unlock(secondFile)

	// 2. catch up on all latest reads
	err = d.readAll(readParams{
		file:          d.file,
		limit:         -1,
		recordID:      &d.recordID,
		processRecord: d.processRecord,
		repair:        repair,
	})
	if err != nil {
		return trace.Wrap(err)
	}

	d.removeExpired()

	// 3. write the compacted version of the data to the non-current file
	var compactedFile *os.File
	if filepath.Base(d.file.Name()) == firstLogFileName {
		compactedFile = secondFile
	} else {
		compactedFile = firstFile
	}
	if err := compactedFile.Truncate(0); err != nil {
		return trace.Wrap(err)
	}

	newState := walpb.State{
		SchemaVersion: V1,
		ProcessID:     1,
		CurrentFile:   filepath.Base(compactedFile.Name()),
	}

	var items []Item
	d.tree.Ascend(func(i btree.Item) bool {
		item := i.(*Item)
		items = append(items, *item)
		return true
	})

	var newID uint64
	for i := range items {
		r := Record{
			Type: OpCreate,
			Key:  items[i].Key,
			Val:  items[i].Val,
		}
		newID += 1
		useID := items[i].ID
		if repair {
			useID = newID
		}
		_, err := d.appendRecord(compactedFile, newState.ProcessID, useID, r)
		if err != nil {
			return trace.Wrap(err)
		}
	}

	// 4. append a "reopen" record to the currently active log file
	// so that other processes that have the current log file
	// opened, will reopen the database
	id := atomic.AddUint64(&d.recordID, 1)
	_, err = d.appendRecord(d.file, d.state.ProcessID, id, Record{Type: OpReopen})
	if err != nil {
		return trace.Wrap(err)
	}

	// 5. write state, so new clients will open
	// the new log file right away
	err = d.writeState(stateFile, &newState)
	if err != nil {
		return trace.Wrap(err)
	}

	return nil
}

// writeState overwrites state file,
// implies that file is already locked
func (d *DirLog) writeState(f *os.File, state *walpb.State) error {
	data, err := state.Marshal()
	if err != nil {
		return trace.Wrap(err)
	}
	containerData, err := ContainerMarshal(data)
	if err != nil {
		return trace.Wrap(err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		return trace.ConvertSystemError(err)
	}
	// containers are of the same exact length, so this is a simple
	// one to one overwrite
	_, err = f.Write(containerData)
	if err != nil {
		return trace.Wrap(err)
	}
	return nil
}

// initOrUpdateState makes sure the monotonically increasing process id
// gets picked by the starting directory log by opening a file in exclusive
// mode, reading container id with encoded binary,
// incrementing it, writing it back, releasing the lock and closing the file
func (d *DirLog) initOrUpdateState() (*walpb.State, error) {
	f, err := os.OpenFile(filepath.Join(d.Dir, stateFilename), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, trace.ConvertSystemError(err)
	}
	defer f.Close()
	if err := fs.WriteLock(f); err != nil {
		return nil, trace.Wrap(err)
	}
	defer fs.Unlock(f)
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	var state walpb.State
	if len(bytes) != 0 {
		// read container with encoded pid
		data, err := ContainerUnmarshal(bytes)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		if err := state.Unmarshal(data); err != nil {
			return nil, trace.Wrap(err)
		}
		if state.ProcessID == math.MaxUint64 {
			return nil, trace.Wrap(&CompactionRequiredError{})
		}
	} else {
		state.ProcessID = 0
		state.CurrentFile = firstLogFileName
		state.SchemaVersion = V1
	}
	state.ProcessID += 1
	if err := d.writeState(f, &state); err != nil {
		return nil, trace.Wrap(err)
	}
	return &state, nil
}

// open opens database and inits internal state
func (d *DirLog) open() error {
	state, err := d.initOrUpdateState()
	if err != nil {
		return trace.Wrap(err)
	}
	d.state = *state
	f, err := os.OpenFile(filepath.Join(d.Dir, d.state.CurrentFile), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return trace.ConvertSystemError(err)
	}
	d.file = f
	return nil
}

func (d *DirLog) Close() error {
	d.Lock()
	defer d.Unlock()
	return d.closeWithoutLock()
}

func (d *DirLog) closeWithoutLock() error {
	if d.file != nil {
		file := d.file
		d.file = nil
		return file.Close()
	}
	return nil
}

func (d *DirLog) newReader(f *os.File, repair bool) reader {
	srcBuffer := d.pool.Get().([]byte)
	defer d.pool.Put(zero(srcBuffer))

	dstBuffer := d.pool.Get().([]byte)
	defer d.pool.Put(zero(dstBuffer))

	if repair {
		return &repairReader{
			file:      f,
			srcBuffer: srcBuffer,
			dstBuffer: dstBuffer,
			records:   &repairMarshaler{},
			marshaler: d.marshaler,
		}
	}
	return &strictReader{
		file:      f,
		srcBuffer: srcBuffer,
		dstBuffer: dstBuffer,
		records:   &recordMarshaler{},
		marshaler: d.marshaler,
	}
}

// removeExpired makes a pass through map and removes expired elements
// returns the number of expired elements removed
func (d *DirLog) removeExpired() int {
	removed := 0
	now := d.Clock.Now().UTC()
	for {
		if d.tree.Len() == 0 {
			break
		}
		item := d.heap.PeekEl()
		if now.Before(item.Expires) {
			break
		}
		d.heap.PopEl()
		d.tree.Delete(item)
		removed++
	}
	return removed
}

func (d *DirLog) processRecord(record *walpb.Record) {
	switch record.Operation {
	case walpb.Operation_CREATE, walpb.Operation_UPDATE, walpb.Operation_PUT:
		item := &Item{Key: record.Key, Val: record.Val, ID: record.ID}
		if record.Expires != 0 {
			item.Expires = time.Unix(record.Expires, 0)
		}
		if item.Expires.IsZero() || d.Clock.Now().Before(item.Expires) {
			d.heap.PushEl(item)
			d.tree.ReplaceOrInsert(item)
		}
	case walpb.Operation_DELETE:
		treeItem := d.tree.Get(&Item{Key: record.Key})
		if treeItem != nil {
			item := treeItem.(*Item)
			d.tree.Delete(item)
			d.heap.RemoveEl(item)
		}
	default:
		// skip unsupported record
	}
}

// checkOperation checks wether operation will succeed without
// applying it to the tree
func (d *DirLog) checkOperation(record *Record) error {
	d.removeExpired()
	switch record.Type {
	case OpCreate:
		if d.tree.Get(&Item{Key: record.Key}) != nil {
			return trace.AlreadyExists("record already exists")
		}
		return nil
	case OpUpdate, OpDelete:
		if d.tree.Get(&Item{Key: record.Key}) == nil {
			return trace.AlreadyExists("record is not found")
		}
		return nil
	case OpPut:
		return nil
	default:
		return nil
	}
}

type processRecord func(*walpb.Record)

type readParams struct {
	file          *os.File
	limit         int
	recordID      *uint64
	processRecord processRecord
	repair        bool
}

// read reads logs from the current file log position up to the end,
// in case if limit is > 0, up to limit records io.EOF is returned at the end of read
func (d *DirLog) read(p readParams) error {
	reader := d.newReader(p.file, p.repair)
	count := 0
	for {
		record, err := reader.next()
		if err != nil {
			err = trace.Unwrap(err)
			if IsPartialReadError(err) {
				continue
			}
			if err == io.EOF {
				return nil
			}
			return trace.Wrap(err)
		}
		atomic.StoreUint64(p.recordID, record.ID)
		if record.Operation == walpb.Operation_REOPEN {
			return trace.Wrap(&ReopenDatabaseError{})
		}
		p.processRecord(record)
		count += 1
		if p.limit > 0 && count >= p.limit {
			return nil
		}
	}
}

// readAll is like read, but does not return io.EOF, and returns nil instead
func (d *DirLog) readAll(p readParams) error {
	err := d.read(p)
	if err != nil {
		if trace.Unwrap(err) == io.EOF {
			return nil
		}
		return trace.Wrap(err)
	}
	return nil
}

type CreateOption struct {
	Expires time.Time
}

type CreateOptionArg func(o *CreateOption) error

func WithExpiry(expires time.Time) CreateOptionArg {
	return func(o *CreateOption) error {
		o.Expires = expires
		return nil
	}
}

func (d *DirLog) Put(i Item) error {
	return d.Append(Record{
		Type:    OpPut,
		Key:     i.Key,
		Val:     i.Val,
		Expires: i.Expires,
	})
}

func (d *DirLog) Update(i Item) error {
	return d.Append(Record{
		Type:    OpUpdate,
		Key:     i.Key,
		Val:     i.Val,
		Expires: i.Expires,
	})
}

func (d *DirLog) Create(i Item) error {
	return d.Append(Record{
		Type:    OpCreate,
		Key:     i.Key,
		Val:     i.Val,
		Expires: i.Expires,
	})
}

func (d *DirLog) Delete(key []byte) error {
	return d.Append(Record{
		Type: OpDelete,
		Key:  key,
	})
}

// Get returns a single item or not found error
func (d *DirLog) Get(key []byte) (*Item, error) {
	r, err := d.GetRange(key, Range{})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if len(r.Items) == 0 {
		return nil, trace.NotFound("item is not found")
	}
	return &r.Items[0], nil
}

type Range struct {
	MatchPrefix bool
	LessThan    []byte
}

func (r *Range) CheckAndSetDefaults() error {
	if len(r.LessThan) != 0 && r.MatchPrefix {
		return trace.BadParameter("either LessThan or MatchPrefix can be set at the same time")
	}
	return nil
}

func (d *DirLog) GetRange(key []byte, r Range) (*GetResult, error) {
	if err := r.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}
	d.Lock()
	defer d.Unlock()
	d.removeExpired()
	result, err := d.tryGet(key, r)
	if err != nil {
		if !IsReopenDatabaseError(err) {
			return nil, trace.Wrap(err)
		}
		if err := d.closeWithoutLock(); err != nil {
			return nil, trace.Wrap(err)
		}
		if err := d.open(); err != nil {
			return nil, trace.Wrap(err)
		}
		return d.tryGet(key, r)
	}
	return result, nil
}

func (d *DirLog) readFull() error {
	if err := fs.ReadLock(d.file); err != nil {
		return trace.Wrap(err)
	}
	defer fs.Unlock(d.file)
	if err := d.readAll(readParams{file: d.file, limit: -1, recordID: &d.recordID, processRecord: d.processRecord, repair: false}); err != nil {
		return trace.Wrap(err)
	}
	return nil
}

//
func (d *DirLog) tryGet(key []byte, r Range) (*GetResult, error) {
	if err := fs.ReadLock(d.file); err != nil {
		return nil, trace.Wrap(err)
	}
	defer fs.Unlock(d.file)
	if err := d.readAll(readParams{file: d.file, limit: -1, recordID: &d.recordID, processRecord: d.processRecord, repair: false}); err != nil {
		return nil, trace.Wrap(err)
	}

	if !r.MatchPrefix && len(r.LessThan) == 0 {
		res := &GetResult{}
		item := d.tree.Get(&Item{Key: key})
		if item != nil {
			res.Items = []Item{*item.(*Item)}
		}
		return res, nil
	}
	var rangeEnd btree.Item
	if r.MatchPrefix {
		rangeEnd = &prefixItem{prefix: key}
	} else if r.LessThan != nil {
		rangeEnd = &Item{Key: r.LessThan}
	}
	res := &GetResult{}
	d.tree.AscendRange(&Item{Key: key}, rangeEnd, func(i btree.Item) bool {
		item := i.(*Item)
		res.Items = append(res.Items, *item)
		return true
	})
	return res, nil
}

type prefixItem struct {
	prefix []byte
}

// Less is used for Btree operations
func (p *prefixItem) Less(iother btree.Item) bool {
	other := iother.(*Item)
	if bytes.HasPrefix(p.prefix, other.Key) {
		return true
	}
	return false
}

func (d *DirLog) Append(r Record) error {
	if err := r.CheckAndSetDefaults(); err != nil {
		return trace.Wrap(err)
	}
	d.Lock()
	defer d.Unlock()

	err := d.tryAppend(r)
	if err != nil {
		if !IsReopenDatabaseError(err) {
			return trace.Wrap(err)
		}
		if err := d.closeWithoutLock(); err != nil {
			return trace.Wrap(err)
		}
		if err := d.open(); err != nil {
			return trace.Wrap(err)
		}
		return d.tryAppend(r)
	}
	return nil
}

func (d *DirLog) tryAppend(r Record) error {
	if err := r.CheckAndSetDefaults(); err != nil {
		return trace.Wrap(err)
	}
	// grab a lock, read and seek to the end of file and sync up the state
	if err := fs.WriteLock(d.file); err != nil {
		return trace.Wrap(err)
	}
	defer fs.Unlock(d.file)
	if err := d.readAll(readParams{file: d.file, limit: -1, recordID: &d.recordID, processRecord: d.processRecord, repair: false}); err != nil {
		return trace.Wrap(err)
	}
	// make sure operation will succeed if applied
	if err := d.checkOperation(&r); err != nil {
		return trace.Wrap(err)
	}
	id := atomic.AddUint64(&d.recordID, 1)
	fullRecord, err := d.appendRecord(d.file, d.state.ProcessID, id, r)
	if err != nil {
		return trace.Wrap(err)
	}
	d.processRecord(fullRecord)
	return nil
}

func (d *DirLog) appendRecord(f *os.File, pid uint64, recordID uint64, r Record) (*walpb.Record, error) {
	fullRecord, parts, err := d.split(r, pid, recordID)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	protoBuffer := d.pool.Get().([]byte)
	defer d.pool.Put(zero(protoBuffer))

	containerBuffer := d.pool.Get().([]byte)
	defer d.pool.Put(zero(containerBuffer))

	for _, part := range parts {
		size, err := part.MarshalTo(protoBuffer)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		err = d.marshaler.Marshal(containerBuffer, protoBuffer[:size])
		if err != nil {
			return nil, trace.Wrap(err)
		}
		_, err = f.Write(containerBuffer)
		if err != nil {
			return nil, trace.ConvertSystemError(err)
		}
	}
	return fullRecord, nil
}

func (d *DirLog) split(src Record, pid uint64, recordID uint64) (*walpb.Record, []walpb.Record, error) {
	op, err := src.Type.Operation()
	if err != nil {
		return nil, nil, trace.Wrap(err)
	}
	fullRecord := walpb.Record{
		Operation: op,
		ProcessID: uint64(pid),
		ID:        recordID,
		Key:       src.Key,
		Val:       src.Val,
		LastPart:  true,
	}
	if !src.Expires.IsZero() {
		fullRecord.Expires = src.Expires.UTC().Unix()
	}

	r := fullRecord
	maxSize := ContainerSizeBytes - headerSizeBytes
	if r.Size() <= maxSize {
		return &fullRecord, []walpb.Record{r}, nil
	}
	var records []walpb.Record
	partID := int32(-1)
	for {
		partID += 1
		// fail safe check, 1K parts is 20MB record
		if partID > 5000 {
			return nil, nil, trace.BadParameter("record is too large")
		}
		part := walpb.Record{
			Operation: op,
			ProcessID: uint64(d.state.ProcessID),
			ID:        recordID,
			LastPart:  false,
			PartID:    partID,
		}
		if len(r.Key) > 0 {
			part.Key = r.Key
			diff := part.Size() - maxSize
			if diff > 0 {
				chunk := len(part.Key) - diff
				chunk -= sliceOverhead(chunk)
				part.Key = part.Key[:chunk]
				r.Key = r.Key[chunk:]
				records = append(records, part)
				continue
			} else {
				r.Key = nil
			}
		}
		if len(r.Val) > 0 {
			part.Val = r.Val
			diff := part.Size() - maxSize
			if diff > 0 {
				chunk := len(part.Val) - diff
				chunk -= sliceOverhead(chunk)
				part.Val = part.Val[:chunk]
				r.Val = r.Val[chunk:]
				records = append(records, part)
				continue
			} else {
				r.Val = nil
			}
		}
		records = append(records, part)
		if len(r.Key) == 0 && len(r.Val) == 0 {
			records[len(records)-1].LastPart = true
			return &fullRecord, records, nil
		}
	}
}

// sliceOverhead returns the amount of extra bytes
// this slice of len l will consume when marshaled to protobuf
func sliceOverhead(l int) int {
	return 1 + sovRecord(uint64(l))
}

// sovRecord is amount of bytes uint64 value
// will consume when marshaled to protobuf
func sovRecord(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

func zero(buffer []byte) []byte {
	for i := 0; i < len(buffer); i++ {
		buffer[i] = 0
	}
	return buffer
}

type recordMarshaler struct {
	// current record that is being assembled
	current *walpb.Record
}

func (m *recordMarshaler) takeRecord() *walpb.Record {
	if m.current == nil || !m.current.LastPart {
		return nil
	}
	r := m.current
	m.current = nil
	return r
}

func (m *recordMarshaler) accept(data []byte) error {
	var r walpb.Record
	if err := r.Unmarshal(data); err != nil {
		return trace.Wrap(err)
	}
	if m.current == nil {
		// first record, but not a first part ID,
		if r.PartID != 0 {
			return trace.BadParameter("first record, but not a first part ID: %v", r.PartID)
		}
		m.current = &r
		return nil
	}
	if m.current.LastPart {
		return trace.BadParameter("take a complete record before accepting a new one")
	}
	// this is the record written by some other process, raise issue
	if r.ProcessID != m.current.ProcessID || r.ID != m.current.ID {
		return trace.BadParameter("got partial record from %v before wrapping up %v",
			r.ProcessID, m.current.ProcessID)
	}
	if r.PartID != m.current.PartID+1 {
		return trace.BadParameter("out of order part id pid: %v, id: %v, part id: %v",
			r.ProcessID, r.ID, r.PartID)
	}
	if len(r.Key) != 0 {
		m.current.Key = append(m.current.Key, r.Key...)
	}
	if len(r.Val) != 0 {
		m.current.Val = append(m.current.Val, r.Val...)
	}
	m.current.PartID = r.PartID
	m.current.LastPart = r.LastPart
	return nil
}

// reader is a wal log reader interface
type reader interface {
	// next returns the next record from the
	// wal log
	next() (*walpb.Record, error)
}

type strictReader struct {
	file      *os.File
	srcBuffer []byte
	dstBuffer []byte
	records   *recordMarshaler
	marshaler *ContainerMarshaler
}

// next returns full record or error otherwise,
// error could be EOF in case if end of file reached
// or partial record read, in this case caller is expected to call
// next again, or error indicating unmarshaling error or data corruption,
// in this case caller can either continue to skip to the next record
// or break
func (r *strictReader) next() (*walpb.Record, error) {
	srcBytes, err := r.file.Read(r.srcBuffer)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if srcBytes != ContainerSizeBytes {
		return nil, &LogReadError{
			Message: fmt.Sprintf("short read: %v bytes instead of expected %v", srcBytes, ContainerSizeBytes),
		}
	}
	dstBytes, err := r.marshaler.Unmarshal(r.dstBuffer, r.srcBuffer)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if err := r.records.accept(r.dstBuffer[:dstBytes]); err != nil {
		return nil, trace.Wrap(err)
	}
	record := r.records.takeRecord()
	if record != nil {
		return record, nil
	}

	return nil, &PartialReadError{}
}

type repairMarshaler struct {
	// current record that is being assembled
	current *walpb.Record
	// skipID is set when repair marshaler
	// is set to skip all records of a given ID
	skipID uint64
}

func (m *repairMarshaler) takeRecord() *walpb.Record {
	if m.current == nil || !m.current.LastPart {
		return nil
	}
	r := m.current
	m.current = nil
	return r
}

func (m *repairMarshaler) setSkipID(skipID uint64) {
	if m.skipID < skipID {
		m.skipID = skipID
	}
}

func (m *repairMarshaler) accept(data []byte) error {
	var r walpb.Record
	if err := r.Unmarshal(data); err != nil {
		if m.current != nil {
			// skip all records that have id <= this id
			m.setSkipID(m.current.ID)
			m.current = nil
		}
		return nil
	}
	if r.ID <= m.skipID {
		return nil
	}
	if m.current == nil {
		// first record, but not a first part ID,
		// skip this record
		if r.PartID != 0 {
			m.setSkipID(r.ID)
			return nil
		}
		m.current = &r
		return nil
	}
	if m.current.LastPart {
		return trace.BadParameter("take a complete record before accepting a new one")
	}
	// this is the record written by some other process,
	// skip both records (whatever record has bigger ID)
	if r.ProcessID != m.current.ProcessID || r.ID != m.current.ID {
		m.setSkipID(r.ID)
		m.setSkipID(m.current.ID)
		m.current = nil
		return nil
	}
	// out of order part, skip this record
	if r.PartID != m.current.PartID+1 {
		m.current = nil
		m.setSkipID(r.ID)
		return nil
	}
	if len(r.Key) != 0 {
		m.current.Key = append(m.current.Key, r.Key...)
	}
	if len(r.Val) != 0 {
		m.current.Val = append(m.current.Val, r.Val...)
	}
	m.current.PartID = r.PartID
	m.current.LastPart = r.LastPart
	return nil
}

type repairReader struct {
	file      *os.File
	srcBuffer []byte
	dstBuffer []byte
	records   *repairMarshaler
	marshaler *ContainerMarshaler
}

// next returns full record or error otherwise,
// error could be EOF in case if end of file reached
// or partial record read, in this case caller is expected to call
// next again, or error indicating unmarshaling error or data corruption,
// in this case caller can either continue to skip to the next record
// or break
func (r *repairReader) next() (*walpb.Record, error) {
	srcBytes, err := r.file.Read(r.srcBuffer)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if srcBytes != ContainerSizeBytes {
		// short reads are only OK if at the end of file
		fi, err := r.file.Stat()
		if err != nil {
			return nil, trace.ConvertSystemError(err)
		}
		offset, err := r.file.Seek(0, 1)
		if err != nil {
			return nil, trace.ConvertSystemError(err)
		}
		// if at the end of file, skip the record
		if offset == fi.Size() {
			return nil, &PartialReadError{}
		}
		// otherwise, error is unkown,
		// not sure what to do
		return nil, trace.BadParameter("short read: %v bytes instead of expected %v", srcBytes, ContainerSizeBytes)
	}
	// skip failures to unmarshal, repair marshaler will skip
	// unsupported records
	dstBytes, err := r.marshaler.Unmarshal(r.dstBuffer, r.srcBuffer)
	if err != nil {
		return nil, &PartialReadError{}
	}
	if err := r.records.accept(r.dstBuffer[:dstBytes]); err != nil {
		return nil, trace.Wrap(err)
	}
	record := r.records.takeRecord()
	if record != nil {
		return record, nil
	}

	return nil, &PartialReadError{}
}
