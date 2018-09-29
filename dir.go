package lf

import (
	"context"
	"encoding/binary"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"

	"github.com/gravitational/lf/fs"
	//	"github.com/gravitational/lf/walpb"
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
func NewDirLog(cfg DirLogConfig) (Log, error) {
	if err := cfg.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}
	d := &DirLog{
		DirLogConfig: cfg,
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
	f, err := os.OpenFile(filepath.Join(d.Dir, d.Prefix+"1.wal"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return trace.ConvertSystemError(err)
	}
	d.file = f
	return nil
}

func (d *DirLog) Close() error {
	if d.file != nil {
		return d.file.Close()
	}
	return nil
}

func (d *DirLog) Append(ctx context.Context, r Record) error {
	// grab a lock, read and seek to the end of file and sync up the state,
	// then if it's a DELETE
	//
	return nil
}
