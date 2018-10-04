package lf

import (
	"context"
	"os"

	"github.com/gravitational/lf/fs"
	"github.com/gravitational/lf/walpb"

	"github.com/fsnotify/fsnotify"
	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
)

// DirWatcher is directory log event watcher
type DirWatcher struct {
	*log.Entry
	prefix  []byte
	watcher *fsnotify.Watcher
	eventsC chan Record
	cancel  context.CancelFunc
	ctx     context.Context
	logFile *os.File
	dir     *DirLog
}

// Close closes watcher and associated files
func (d *DirWatcher) Close() error {
	d.logFile.Close()
	d.cancel()
	return d.watcher.Close()
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

func (d *DirWatcher) readLimit() ([]Record, error) {
	var records []Record
	err := d.dir.readAll(d.logFile, recordBatchSize, func(record *walpb.Record) {
		op, err := FromRecordOperation(record.Operation)
		if err != nil {
			return
		}
		records = append(records, Record{
			Type: op,
			Key:  record.Key,
			Val:  record.Val,
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
	return d.readLimit()
}

func (d *DirWatcher) watch() {
selectloop:
	for {
		select {
		case <-d.ctx.Done():
			d.Debugf("Watcher is closing, returning.")
			d.Close()
			return
		case event, ok := <-d.watcher.Events:
			if !ok {
				return
			}
			if event.Op&fsnotify.Write != fsnotify.Write {
				continue selectloop
			}
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
		case err, ok := <-d.watcher.Errors:
			if !ok {
				d.Debugf("Watch failed with error (%v), returning.", err)
				d.Close()
				return
			}
		}
	}
}
