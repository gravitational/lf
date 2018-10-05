package lf

import (
	"context"
	"time"

	"github.com/gravitational/trace"
	check "gopkg.in/check.v1"
)

// TestWatchSimple tests simple watch scenarios
func (s *DirSuite) TestWatchSimple(c *check.C) {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir:        dir,
		PollPeriod: 10 * time.Millisecond,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	watcher, err := l.NewWatcher(nil, nil)
	c.Assert(err, check.IsNil)
	defer watcher.Close()

	record := Record{Type: OpCreate, Key: []byte("hello"), Val: []byte("world")}
	err = l.Append(context.TODO(), record)
	c.Assert(err, check.IsNil)

	record.ID = 1
	record.ProcessID = l.state.ProcessID
	select {
	case event := <-watcher.Events():
		c.Assert(event, check.DeepEquals, record)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for a record")
	}

	// start another watcher from another backend with offset
	l2, err := NewDirLog(DirLogConfig{
		Dir:        dir,
		PollPeriod: 10 * time.Millisecond,
	})
	c.Assert(err, check.IsNil)
	defer l2.Close()

	watcherOffset, err := l2.NewWatcher(nil, &Offset{RecordID: 1})
	c.Assert(err, check.IsNil)
	defer watcherOffset.Close()

	record = Record{Type: OpUpdate, Key: []byte("hello"), Val: []byte("there")}
	err = l.Append(context.TODO(), record)
	c.Assert(err, check.IsNil)

	record.ID = 2
	record.ProcessID = l.state.ProcessID
	select {
	case event := <-watcher.Events():
		c.Assert(event, check.DeepEquals, record)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for a record")
	}

	// watcher at offset only gets a record starting last id - 1
	select {
	case event := <-watcherOffset.Events():
		c.Assert(event, check.DeepEquals, record)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for a record")
	}
}

// TestWatchPrefix tests watch scenario watching for a prefix
func (s *DirSuite) TestWatchPrefix(c *check.C) {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir:        dir,
		PollPeriod: 10 * time.Millisecond,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	watcher, err := l.NewWatcher([]byte("/test/prefix"), nil)
	c.Assert(err, check.IsNil)
	defer watcher.Close()

	record := Record{Type: OpCreate, Key: []byte("/other/hello"), Val: []byte("world")}
	err = l.Append(context.TODO(), record)
	c.Assert(err, check.IsNil)

	record = Record{Type: OpCreate, Key: []byte("/test/prefix"), Val: []byte("world")}
	err = l.Append(context.TODO(), record)
	c.Assert(err, check.IsNil)

	record.ID = 2
	record.ProcessID = l.state.ProcessID
	select {
	case event := <-watcher.Events():
		c.Assert(event, check.DeepEquals, record)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for a record")
	}
}

// TestWatchCompaction test watcher behavior during compactions,
// after the compaction occurs, watcher should receive the compaction record
func (s *DirSuite) TestWatchCompaction(c *check.C) {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir:        dir,
		PollPeriod: 10 * time.Millisecond,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	watcher, err := l.NewWatcher(nil, nil)
	c.Assert(err, check.IsNil)
	defer watcher.Close()

	record := Record{Type: OpCreate, Key: []byte("hello"), Val: []byte("world")}
	err = l.Append(context.TODO(), record)
	c.Assert(err, check.IsNil)

	record.ID = 1
	record.ProcessID = l.state.ProcessID
	select {
	case <-watcher.Done():
		c.Fatalf("watcher unexpectedly exited")
	case event := <-watcher.Events():
		c.Assert(event, check.DeepEquals, record)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for a record")
	}

	record = Record{Type: OpUpdate, Key: []byte("hello"), Val: []byte("v2")}
	err = l.Append(context.TODO(), record)
	c.Assert(err, check.IsNil)

	record = Record{Type: OpUpdate, Key: []byte("hello"), Val: []byte("v3")}
	err = l.Append(context.TODO(), record)
	c.Assert(err, check.IsNil)

	for i := 0; i < 3; i++ {
		err = l.tryCompactAndReopen(context.TODO())
		if err == nil || !trace.IsCompareFailed(err) {
			break
		}
	}
	c.Assert(err, check.IsNil)
	// expect watcher to exit
	select {
	case <-watcher.Done():
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for watcher to exit")
	}

	// watcher can't continue watch from record 2 because it
	// was compacted
	_, err = l.NewWatcher(nil, &Offset{RecordID: 2})
	c.Assert(err, check.NotNil)

	// new watcher could continue from record id 3 though
	watcher2, err := l.NewWatcher(nil, &Offset{RecordID: 3})
	c.Assert(err, check.IsNil)
	defer watcher2.Close()

	record = Record{Type: OpUpdate, Key: []byte("hello"), Val: []byte("v4")}
	err = l.Append(context.TODO(), record)
	c.Assert(err, check.IsNil)

	record.ID = 4
	record.ProcessID = l.state.ProcessID
	select {
	case <-watcher2.Done():
		c.Fatalf("watcher unexpectedly exited")
	case event := <-watcher2.Events():
		c.Assert(event, check.DeepEquals, record)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for a record")
	}
}
