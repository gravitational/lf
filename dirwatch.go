package lf

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/gravitational/lf/fs"
	"github.com/gravitational/lf/walpb"

	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
)

// NewWatcher returns new watcher matching prefix,
// if prefix is supplied, only events matching the prefix
// will be returned, otherwise, all events will be returned
// offset is optional and is used to locate the proper offset
// if offset is nil, watch started from scratch
func NewWatcher(cfg DirWatcherConfig) (*DirWatcher, error) {
	if err := cfg.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}
	// grab state lock to make sure watcher is tracking the right file
	stateFile, err := os.OpenFile(filepath.Join(cfg.Dir.Dir, stateFilename), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, trace.ConvertSystemError(err)
	}
	defer stateFile.Close()
	if err := fs.WriteLock(stateFile); err != nil {
		return nil, trace.Wrap(err)
	}
	defer fs.Unlock(stateFile)
	logFile, err := os.OpenFile(cfg.Dir.file.Name(), os.O_RDWR, 0600)
	if err != nil {
		return nil, trace.ConvertSystemError(err)
	}
	logStat, err := logFile.Stat()
	if err != nil {
		return nil, trace.ConvertSystemError(err)
	}

	ctx, cancel := context.WithCancel(cfg.Dir.Context)
	dirWatcher := &DirWatcher{
		Entry: log.WithFields(log.Fields{
			trace.Component: componentLogFormat,
		}),
		DirWatcherConfig: cfg,
		eventsC:          make(chan Record),
		cancel:           cancel,
		ctx:              ctx,
		logFile:          logFile,
		logStat:          logStat,
	}
	if dirWatcher.Offset == nil {
		// seek to 0 to relative position gives the current offset
		// so watcher will start from the current position of the current
		// backend
		pos, err := dirWatcher.Dir.file.Seek(0, 1)
		if pos%ContainerSizeBytes != 0 {
			dirWatcher.Close()
			return nil, trace.BadParameter("bad offset: %q, should be multiple of %q", pos, ContainerSizeBytes)
		}
		if err != nil {
			dirWatcher.Close()
			return nil, trace.ConvertSystemError(err)
		}
		_, err = logFile.Seek(pos, 0)
		if err != nil {
			dirWatcher.Close()
			return nil, trace.ConvertSystemError(err)
		}
	} else {
		// skip to the record that has the required record ID
		// or fail with error indicating that history is lost
		// after compaction
		if err := dirWatcher.seekTo(dirWatcher.Offset.RecordID); err != nil {
			return nil, trace.ConvertSystemError(err)
		}
	}

	go dirWatcher.watch()
	return dirWatcher, nil
}

type DirWatcherConfig struct {
	// Dir is a directory
	Dir *DirLog
	// Prefix is a prefix to match
	Prefix []byte
	// Offset is optional offset
	Offset *Offset
	// PollPeriod is a polling period for log file
	PollPeriod time.Duration
}

func (d *DirWatcherConfig) CheckAndSetDefaults() error {
	if d.Dir == nil {
		return trace.BadParameter("missing parameter Dir")
	}
	if d.PollPeriod == 0 {
		d.PollPeriod = defaultPollPeriod
	}
	return nil
}

// DirWatcher is directory log event watcher
type DirWatcher struct {
	DirWatcherConfig
	*log.Entry
	eventsC chan Record
	cancel  context.CancelFunc
	ctx     context.Context
	logFile *os.File
	logStat os.FileInfo
}

// Close closes watcher and associated files
func (d *DirWatcher) Close() error {
	d.cancel()
	if err := d.logFile.Close(); err != nil {
		return trace.Wrap(err)
	}
	return nil
}

// Done returns channel that is closed
// when watcher is closed
func (d *DirWatcher) Done() <-chan struct{} {
	return d.ctx.Done()
}

// Events returns channel with records
func (d *DirWatcher) Events() <-chan Record {
	return d.eventsC
}

// seekTo skips the log file to the record with a given ID,
// returns error if the record is not found
func (d *DirWatcher) seekTo(recordID uint64) error {
	var lastRecordID, tmp uint64
	for {
		err := d.Dir.read(d.logFile, 1, &tmp, func(record *walpb.Record) {
			lastRecordID = record.ID
		})
		if err != nil {
			if trace.Unwrap(err) == io.EOF {
				return trace.NotFound("record %v is not found, restart watch", recordID)
			}
			return trace.Wrap(err)
		}
		if lastRecordID == recordID {
			return nil
		}
		if lastRecordID > recordID {
			return trace.NotFound("record %v is not found, restart watch", recordID)
		}
	}
}

func (d *DirWatcher) readLimit(batchSize int) ([]Record, error) {
	var records []Record
	var recordID uint64
	err := d.Dir.readAll(d.logFile, batchSize, &recordID, func(record *walpb.Record) {
		op, err := FromRecordOperation(record.Operation)
		if err != nil {
			return
		}
		if op != OpReopen && len(d.Prefix) != 0 && !bytes.HasPrefix(record.Key, d.Prefix) {
			return
		}
		records = append(records, Record{
			Type:      op,
			Key:       record.Key,
			Val:       record.Val,
			ProcessID: record.ProcessID,
			ID:        record.ID,
		})
	})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return records, nil
}

func (d *DirWatcher) lockAndReadLimit() ([]Record, error) {
	if err := fs.ReadLock(d.logFile); err != nil {
		return nil, trace.Wrap(err)
	}
	defer fs.Unlock(d.logFile)
	return d.readLimit(recordBatchSize)
}

func (d *DirWatcher) watch() {
	ticker := time.NewTicker(d.PollPeriod)
	defer ticker.Stop()
selectloop:
	for {
		select {
		case <-d.ctx.Done():
			d.Debugf("Watcher is closing, returning.")
			d.Close()
			return
		case <-ticker.C:
			logStat, err := d.logFile.Stat()
			if err != nil {
				log.Warningf("Failed to stat file: %v.", err)
				continue selectloop
			}
			if logStat.Size() == d.logStat.Size() {
				d.logStat = logStat
				continue selectloop
			}
			d.logStat = logStat
			for {
				records, err := d.lockAndReadLimit()
				if err != nil {
					d.Debugf("Read limit failed with error (%v), returning.", err)
					d.Close()
					return
				}
				if len(records) == 0 {
					continue selectloop
				}
				for _, record := range records {
					select {
					case d.eventsC <- record:
					case <-d.ctx.Done():
						d.Debugf("Watcher is closing, returning.", err)
						d.Close()
						return
					}
				}
			}
		}
	}
}
