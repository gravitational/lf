package lf

import (
	"context"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gravitational/lf/fs"
	"github.com/gravitational/lf/walpb"

	log "github.com/gravitational/logrus"
	"github.com/gravitational/trace"
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
		kv:           make(map[string]Item),
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
	// kv is a map of keys and values
	kv map[string]Item
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
	if err := d.tryCompact(ctx); err != nil {
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
func (d *DirLog) tryCompact(ctx context.Context) error {
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
	if err := d.readAll(d.file, -1, &d.recordID, d.processRecord); err != nil {
		return trace.Wrap(err)
	}

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

	for _, item := range d.kv {
		r := Record{
			Type: OpCreate,
			Key:  item.Key,
			Val:  item.Val,
		}
		_, err := d.appendRecord(ctx, compactedFile, newState.ProcessID, item.ID, r)
		if err != nil {
			return trace.Wrap(err)
		}
	}

	// 4. append a "reopen" record to the currently active log file
	// so that other processes that have the current log file
	// opened, will reopen the database
	id := atomic.AddUint64(&d.recordID, 1)
	_, err = d.appendRecord(ctx, d.file, d.state.ProcessID, id, Record{Type: OpReopen})
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

func (d *DirLog) newReader(f *os.File) *reader {
	srcBuffer := d.pool.Get().([]byte)
	defer d.pool.Put(zero(srcBuffer))

	dstBuffer := d.pool.Get().([]byte)
	defer d.pool.Put(zero(dstBuffer))

	records := &recordMarshaler{}

	return &reader{
		file:      f,
		srcBuffer: srcBuffer,
		dstBuffer: dstBuffer,
		records:   records,
		marshaler: d.marshaler,
	}
}

func (d *DirLog) processRecord(record *walpb.Record) {
	switch record.Operation {
	case walpb.Operation_CREATE, walpb.Operation_UPDATE, walpb.Operation_PUT:
		d.kv[string(record.Key)] = Item{Key: record.Key, Val: record.Val, ID: record.ID}
	case walpb.Operation_DELETE:
		delete(d.kv, string(record.Key))
	default:
		// skip unsupported record
	}
}

type processRecord func(*walpb.Record)

// read reads logs from the current file log position up to the end,
// in case if limit is > 0, up to limit records io.EOF is returned at the end of read
func (d *DirLog) read(f *os.File, limit int, recordID *uint64, processRecord processRecord) error {
	reader := d.newReader(f)
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
		atomic.StoreUint64(recordID, record.ID)
		if record.Operation == walpb.Operation_REOPEN {
			return trace.Wrap(&ReopenDatabaseError{})
		}
		processRecord(record)
		count += 1
		if limit > 0 && count >= limit {
			return nil
		}
	}
}

// readAll is like read, but does not return io.EOF, and returns nil instead
func (d *DirLog) readAll(f *os.File, limit int, recordID *uint64, processRecord processRecord) error {
	err := d.read(f, limit, recordID, processRecord)
	if err != nil {
		if trace.Unwrap(err) == io.EOF {
			return nil
		}
		return trace.Wrap(err)
	}
	return nil
}

func (d *DirLog) Get(key string) (*Item, error) {
	d.Lock()
	defer d.Unlock()
	item, err := d.tryGet(key)
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
		return d.tryGet(key)
	}
	return item, nil
}

//
func (d *DirLog) tryGet(key string) (*Item, error) {
	if err := fs.ReadLock(d.file); err != nil {
		return nil, trace.Wrap(err)
	}
	defer fs.Unlock(d.file)
	if err := d.readAll(d.file, -1, &d.recordID, d.processRecord); err != nil {
		return nil, trace.Wrap(err)
	}
	item, ok := d.kv[key]
	if !ok {
		return nil, trace.NotFound("key is not found")
	}
	return &item, nil
}

func (d *DirLog) Append(ctx context.Context, r Record) error {
	d.Lock()
	defer d.Unlock()

	err := d.tryAppend(ctx, r)
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
		return d.tryAppend(ctx, r)
	}
	return nil
}

func (d *DirLog) tryAppend(ctx context.Context, r Record) error {
	if err := r.CheckAndSetDefaults(); err != nil {
		return trace.Wrap(err)
	}
	// grab a lock, read and seek to the end of file and sync up the state
	if err := fs.WriteLock(d.file); err != nil {
		return trace.Wrap(err)
	}
	defer fs.Unlock(d.file)
	if err := d.readAll(d.file, -1, &d.recordID, d.processRecord); err != nil {
		return trace.Wrap(err)
	}
	id := atomic.AddUint64(&d.recordID, 1)
	fullRecord, err := d.appendRecord(ctx, d.file, d.state.ProcessID, id, r)
	if err != nil {
		return trace.Wrap(err)
	}
	d.processRecord(fullRecord)
	return nil
}

func (d *DirLog) appendRecord(ctx context.Context, f *os.File, pid uint64, recordID uint64, r Record) (*walpb.Record, error) {
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

func (m *recordMarshaler) accept(data []byte, offset int64) error {
	var r walpb.Record
	if err := r.Unmarshal(data); err != nil {
		return trace.Wrap(err)
	}
	if m.current == nil {
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

type reader struct {
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
func (r *reader) next() (*walpb.Record, error) {
	offset, err := r.file.Seek(0, 1)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	srcBytes, err := r.file.Read(r.srcBuffer)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if srcBytes != ContainerSizeBytes {
		return nil, trace.BadParameter("short read: %v bytes instead of expected %v", srcBytes, ContainerSizeBytes)
	}
	dstBytes, err := r.marshaler.Unmarshal(r.dstBuffer, r.srcBuffer)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if err := r.records.accept(r.dstBuffer[:dstBytes], offset); err != nil {
		return nil, trace.Wrap(err)
	}
	record := r.records.takeRecord()
	if record != nil {
		return record, nil
	}

	return nil, &PartialReadError{}
}
