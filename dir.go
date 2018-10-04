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
	"github.com/gravitational/trace"
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
	// RetryOpenPeriod is a time between reopen attempts
	// in case if database is being compacted
	RetryOpenPeriod time.Duration
	// OpenTimeout is a default timeout
	// for the database open operation to fail
	OpenTimeout time.Duration
}

const (
	// DefaultRetryOpenPeriod is a default period between
	// open attempts
	DefaultRetryOpenPeriod = 300 * time.Millisecond
	// DefaultOpenTimeout is a default timeout
	// for open operation to time out
	DefaultOpenTimeout = 30 * time.Second
)

// CheckAndSetDefaults checks and sets default values
func (cfg *DirLogConfig) CheckAndSetDefaults() error {
	if cfg.Dir == "" {
		return trace.BadParameter("missing parameter Dir")
	}
	if cfg.Context == nil {
		cfg.Context = context.Background()
	}
	if cfg.RetryOpenPeriod == 0 {
		cfg.RetryOpenPeriod = DefaultRetryOpenPeriod
	}
	if cfg.OpenTimeout == 0 {
		cfg.OpenTimeout = DefaultOpenTimeout
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
		DirLogConfig: cfg,
		kv:           make(map[string][]byte),
		marshaler:    NewContainerMarshaler(),
	}
	d.pool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, ContainerSizeBytes)
		},
	}
	openTimeout := time.NewTimer(cfg.OpenTimeout)
	defer openTimeout.Stop()

	for {
		// try to reopen the database if compaction is required
		err := d.open()
		if err == nil {
			return d, nil
		}
		if !IsCompactionRequiredError(err) {
			return nil, trace.Wrap(err)
		}
		err = d.tryCompact(d.Context)
		if err == nil {
			return d, nil
		}
		if !trace.IsCompareFailed(err) {
			return nil, trace.Wrap(err)
		}
		select {
		case <-openTimeout.C:
			return nil, trace.ConnectionProblem(err, "database is locked by another process")
		case <-time.After(d.RetryOpenPeriod):
			continue
		case <-d.Context.Done():
			return nil, trace.ConnectionProblem(err, "database is locked by another process")
		}
	}
}

type DirLog struct {
	DirLogConfig
	// file is a currently opened log file
	file *os.File
	// pool contains slice bytes
	// re-used to marshal and unmarshal containers,
	// used to reduce memory allocations
	pool *sync.Pool
	// kv is a map of keys and values
	kv map[string][]byte
	// recordID is a current record id,
	// incremented on every record read
	recordID uint64
	// state is a current database state
	state walpb.State
	// marshaler is a container marshaler
	marshaler *ContainerMarshaler
}

const (
	// V1 is a current schema version
	V1 = iota
)

const (
	// stateFilename
	stateFilename = "state"
	// firstLogFileName
	firstLogFileName = "first"
	// secondLogFileName
	secondLogFileName = "second"
)

// tryCompactAndReopen tries to compact the database,
// if it succeeds, it will reopen the database
func (d *DirLog) tryCompactAndReopen(ctx context.Context) error {
	if err := d.tryCompact(ctx); err != nil {
		return trace.Wrap(err)
	}
	if err := d.Close(); err != nil {
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
	if err := fs.TryWriteLock(firstFile); err != nil {
		return trace.Wrap(err)
	}
	defer fs.Unlock(firstFile)

	secondFile, err := os.OpenFile(filepath.Join(d.Dir, secondLogFileName), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return trace.ConvertSystemError(err)
	}
	if err := fs.TryWriteLock(secondFile); err != nil {
		return trace.Wrap(err)
	}
	defer fs.Unlock(secondFile)

	// 2. catch up on all latest reads
	if err := d.readAll(); err != nil {
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

	var recordID uint64
	for key, val := range d.kv {
		r := Record{
			Type: OpCreate,
			Key:  []byte(key),
			Val:  val,
		}
		_, err := d.appendRecord(ctx, compactedFile, newState.ProcessID, &recordID, r)
		if err != nil {
			return trace.Wrap(err)
		}
	}

	// 4. append a "reopen" record to the currently active log file
	// so that other processes that have the current log file
	// opened, will reopen the database
	_, err = d.appendRecord(ctx, d.file, d.state.ProcessID, &d.recordID, Record{Type: OpReopen})
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
	if d.file != nil {
		file := d.file
		d.file = nil
		return file.Close()
	}
	return nil
}

func (d *DirLog) newReader() *reader {
	srcBuffer := d.pool.Get().([]byte)
	defer d.pool.Put(zero(srcBuffer))

	dstBuffer := d.pool.Get().([]byte)
	defer d.pool.Put(zero(dstBuffer))

	records := &recordMarshaler{}

	return &reader{
		file:      d.file,
		srcBuffer: srcBuffer,
		dstBuffer: dstBuffer,
		records:   records,
		marshaler: d.marshaler,
	}
}

func (d *DirLog) processRecord(record *walpb.Record) {
	switch record.Operation {
	case walpb.Operation_CREATE, walpb.Operation_UPDATE, walpb.Operation_PUT:
		d.kv[string(record.Key)] = record.Val
	case walpb.Operation_DELETE:
		delete(d.kv, string(record.Key))
	default:
		// skip unsupported record
	}
}

// readAll reads logs from the current file
// log position up to the end
func (d *DirLog) readAll() error {
	reader := d.newReader()
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
		if record.Operation == walpb.Operation_REOPEN {
			return trace.Wrap(&ReopenDatabaseError{})
		}
		d.processRecord(record)
	}
}

func (d *DirLog) Get(key string) ([]byte, error) {
	val, err := d.tryGet(key)
	if err != nil {
		if !IsReopenDatabaseError(err) {
			return nil, trace.Wrap(err)
		}
		if err := d.Close(); err != nil {
			return nil, trace.Wrap(err)
		}
		if err := d.open(); err != nil {
			return nil, trace.Wrap(err)
		}
		return d.tryGet(key)
	}
	return val, nil
}

//
func (d *DirLog) tryGet(key string) ([]byte, error) {
	if err := fs.ReadLock(d.file); err != nil {
		return nil, trace.Wrap(err)
	}
	defer fs.Unlock(d.file)
	if err := d.readAll(); err != nil {
		return nil, trace.Wrap(err)
	}
	val, ok := d.kv[key]
	if !ok {
		return nil, trace.NotFound("key is not found")
	}
	return val, nil
}

func (d *DirLog) Append(ctx context.Context, r Record) error {
	err := d.tryAppend(ctx, r)
	if err != nil {
		if !IsReopenDatabaseError(err) {
			return trace.Wrap(err)
		}
		if err := d.Close(); err != nil {
			return trace.Wrap(err)
		}
		if err := d.open(); err != nil {
			return trace.Wrap(err)
		}
		return d.Append(ctx, r)
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
	if err := d.readAll(); err != nil {
		return trace.Wrap(err)
	}
	fullRecord, err := d.appendRecord(ctx, d.file, d.state.ProcessID, &d.recordID, r)
	if err != nil {
		return trace.Wrap(err)
	}
	d.processRecord(fullRecord)
	return nil
}

func (d *DirLog) appendRecord(ctx context.Context, f *os.File, pid uint64, recordID *uint64, r Record) (*walpb.Record, error) {
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

func (d *DirLog) split(src Record, pid uint64, recordID *uint64) (*walpb.Record, []walpb.Record, error) {
	op, err := src.Type.Operation()
	if err != nil {
		return nil, nil, trace.Wrap(err)
	}
	id := atomic.AddUint64(recordID, 1)
	fullRecord := walpb.Record{
		Operation: op,
		ProcessID: uint64(pid),
		ID:        id,
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
			ID:        id,
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
	if err := r.records.accept(r.dstBuffer[:dstBytes]); err != nil {
		return nil, trace.Wrap(err)
	}
	record := r.records.takeRecord()
	if record != nil {
		return record, nil
	}
	return nil, &PartialReadError{}
}
