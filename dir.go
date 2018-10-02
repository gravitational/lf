package lf

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/gravitational/lf/fs"
	"github.com/gravitational/lf/walpb"
	"github.com/gravitational/trace"
)

type DirLogConfig struct {
	Dir    string
	Prefix string
}

// CheckAndSetDefaults checks and sets default values
func (cfg *DirLogConfig) CheckAndSetDefaults() error {
	if cfg.Dir == "" {
		return trace.BadParameter("missing parameter Dir")
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
	if err := d.open(); err != nil {
		return nil, trace.Wrap(err)
	}

	return d, nil
}

type DirLog struct {
	DirLogConfig
	file *os.File
	pid  int
	pool *sync.Pool
	// kv is a map of keys and values
	kv        map[string][]byte
	id        uint64
	marshaler *ContainerMarshaler
}

const (
	pidFilename = "pid"
)

// pickProcessID makes sure the monotonically increasing process id
// gets picked by the starting directory log by opening a file in exclusive
// mode, reading container id with encoded binary,
// incrementing it, writing it back, releasing the lock and closing the file
func (d *DirLog) pickProcessID() (uint64, error) {
	f, err := os.OpenFile(filepath.Join(d.Dir, pidFilename), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return 0, trace.ConvertSystemError(err)
	}
	defer f.Close()
	if err := fs.WriteLock(f); err != nil {
		return 0, trace.Wrap(err)
	}
	defer fs.Unlock(f)
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		return 0, trace.Wrap(err)
	}
	var pid uint64
	if len(bytes) != 0 {
		// read container with encoded pid
		data, err := ContainerUnmarshal(bytes)
		if err != nil {
			return 0, trace.Wrap(err)
		}
		pid = binary.LittleEndian.Uint64(data)
		if pid == math.MaxUint64 {
			return 0, trace.BadParameter("maximum value of %v reached for process ids", pid)
		}
	}
	pid = pid + 1
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, pid)
	data, err := ContainerMarshal(out)
	if err != nil {
		return 0, trace.Wrap(err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		return 0, trace.Wrap(err)
	}
	// containers are of the same exact length, so this is a simple
	// one to one overwrite
	_, err = f.Write(data)
	if err != nil {
		return 0, trace.Wrap(err)
	}
	return pid, nil
}

func (d *DirLog) open() error {
	f, err := os.OpenFile(filepath.Join(d.Dir, d.Prefix+"1.wal"), os.O_RDWR|os.O_CREATE, 0600)
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
			if err == errPartialRecord {
				continue
			}
			if err == io.EOF {
				return nil
			}
			return trace.Wrap(err)
		}
		d.processRecord(record)
	}
}

//
func (d *DirLog) Get(key string) ([]byte, error) {
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
	// now can write the record
	fullRecord, parts, err := d.split(r)
	if err != nil {
		return trace.Wrap(err)
	}
	protoBuffer := d.pool.Get().([]byte)
	defer d.pool.Put(zero(protoBuffer))

	containerBuffer := d.pool.Get().([]byte)
	defer d.pool.Put(zero(containerBuffer))

	for _, part := range parts {
		size, err := part.MarshalTo(protoBuffer)
		if err != nil {
			return trace.Wrap(err)
		}
		err = d.marshaler.Marshal(containerBuffer, protoBuffer[:size])
		if err != nil {
			return trace.Wrap(err)
		}
		_, err = d.file.Write(containerBuffer)
		if err != nil {
			return trace.ConvertSystemError(err)
		}
	}
	d.processRecord(fullRecord)
	return nil
}

func (d *DirLog) split(src Record) (*walpb.Record, []walpb.Record, error) {
	op, err := src.Type.Operation()
	if err != nil {
		return nil, nil, trace.Wrap(err)
	}
	id := atomic.AddUint64(&d.id, 1)
	fullRecord := walpb.Record{
		Operation: op,
		ProcessID: uint64(d.pid),
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
			ProcessID: uint64(d.pid),
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
	return nil, errPartialRecord
}

var errPartialRecord = errors.New("partial record read")
