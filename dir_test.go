/*
Copyright 2018 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package lf

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
	check "gopkg.in/check.v1"
)

type DirSuite struct{}

var _ = check.Suite(&DirSuite{})

func (s *DirSuite) SetUpSuite(c *check.C) {
	log.StandardLogger().Hooks = make(log.LevelHooks)
	formatter := &trace.TextFormatter{DisableTimestamp: false}
	log.SetFormatter(formatter)
	if testing.Verbose() {
		log.SetLevel(log.DebugLevel)
		log.SetOutput(os.Stdout)
	}
}

// TestConcurrentCRUD tests simple scenario
// when two concurrent processes create and read
// records
func (s *DirSuite) TestConcurrentCRUD(c *check.C) {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir: dir,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	err = l.Append(context.TODO(),
		Record{Type: OpCreate, Key: []byte("hello"), Val: []byte("world")})
	c.Assert(err, check.IsNil)

	out, err := l.Get("hello")
	c.Assert(err, check.IsNil)
	c.Assert(out.Val, check.DeepEquals, []byte("world"))
	c.Assert(out.ID, check.Equals, uint64(1))

	l2, err := NewDirLog(DirLogConfig{
		Dir: dir,
	})
	c.Assert(err, check.IsNil)
	defer l2.Close()

	out, err = l2.Get("hello")
	c.Assert(err, check.IsNil)
	c.Assert(out.Val, check.DeepEquals, []byte("world"))
	c.Assert(out.ID, check.Equals, uint64(1))

	err = l2.Append(context.TODO(),
		Record{Type: OpCreate, Key: []byte("another"), Val: []byte("record")})
	c.Assert(err, check.IsNil)

	out, err = l.Get("another")
	c.Assert(err, check.IsNil)
	c.Assert(out.Val, check.DeepEquals, []byte("record"))
	c.Assert(out.ID, check.Equals, uint64(2))

	// update existing record
	err = l.Append(context.TODO(),
		Record{Type: OpUpdate, Key: []byte("another"), Val: []byte("value 2")})
	c.Assert(err, check.IsNil)

	out, err = l.Get("another")
	c.Assert(err, check.IsNil)
	c.Assert(out.Val, check.DeepEquals, []byte("value 2"))
	c.Assert(out.ID, check.Equals, uint64(3))

	out, err = l2.Get("another")
	c.Assert(err, check.IsNil)
	c.Assert(out.Val, check.DeepEquals, []byte("value 2"))
	c.Assert(out.ID, check.Equals, uint64(3))

	// delete a record
	err = l.Append(context.TODO(),
		Record{Type: OpDelete, Key: []byte("another")})
	c.Assert(err, check.IsNil)

	_, err = l.Get("another")
	c.Assert(trace.IsNotFound(err), check.Equals, true)

	_, err = l2.Get("another")
	c.Assert(trace.IsNotFound(err), check.Equals, true)
}

// TestLargeRecord tests scenario
// when large record is marshaled and unmarshaled
func (s *DirSuite) recordSizes(c *check.C, keySize, valSize int) error {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir: dir,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	r := Record{Type: OpCreate}
	r.Key = make([]byte, keySize)
	r.Val = make([]byte, valSize)
	for i := 0; i < len(r.Key); i++ {
		r.Key[i] = byte(i % 255)
	}
	for i := 0; i < len(r.Val); i++ {
		r.Val[i] = byte(i % 255)
	}

	err = l.Append(context.TODO(), r)
	if err != nil {
		return trace.Wrap(err)
	}

	l2, err := NewDirLog(DirLogConfig{
		Dir: dir,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	defer l2.Close()

	out, err := l2.Get(string(r.Key))
	if err != nil {
		return trace.Wrap(err)
	}
	if !bytes.Equal(out.Val, r.Val) {
		return trace.CompareFailed("%v != %v", len(r.Val), len(out.Val))
	}
	return nil
}

// TestRecords tests records of various key and value sizes
func (s *DirSuite) TestRecords(c *check.C) {
	type testCase struct {
		info    string
		keySize int
		valSize int
		err     error
	}
	testCases := []testCase{
		{
			info:    "both keys and vals exceed container size",
			keySize: ContainerSizeBytes + 2,
			valSize: ContainerSizeBytes + 15,
		},
		{
			info:    "key is on the boundary",
			keySize: ContainerSizeBytes - headerSizeBytes,
			valSize: ContainerSizeBytes,
		},
		{
			info:    "key val is on the boundary",
			keySize: ContainerSizeBytes - headerSizeBytes,
			valSize: ContainerSizeBytes - headerSizeBytes,
		},
		{
			info:    "key val is almost the boundary",
			keySize: ContainerSizeBytes - headerSizeBytes - 1,
			valSize: ContainerSizeBytes - headerSizeBytes - 1,
		},
		{
			info:    "key val exceed the boundary",
			keySize: ContainerSizeBytes - headerSizeBytes + 1,
			valSize: ContainerSizeBytes - headerSizeBytes + 1,
		},
		{
			info:    "zero keys are not allowed",
			keySize: 0,
			valSize: ContainerSizeBytes,
			err:     trace.BadParameter("bad parameter"),
		},
		{
			info:    "zero values are not ok",
			keySize: 5,
			valSize: 0,
		},
	}
	for i, tc := range testCases {
		comment := check.Commentf("test case %v: %q", i, tc.info)
		err := s.recordSizes(c, tc.keySize, tc.valSize)
		if tc.err != nil {
			c.Assert(err, check.FitsTypeOf, tc.err)

		} else {
			if err != nil {
				c.Assert(err, check.IsNil, comment)
			}
		}
	}
}

func (s *DirSuite) expectRecord(c *check.C, l *DirLog, key string, val []byte, id uint64) {
	out, err := l.Get(key)
	c.Assert(err, check.IsNil)
	c.Assert(out.Val, check.DeepEquals, val)
	c.Assert(out.ID, check.Equals, id)
}

// TestCompaction verifies compaction and concurrent
// operations
func (s *DirSuite) TestCompaction(c *check.C) {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir: dir,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	c.Assert(filepath.Base(l.file.Name()), check.Equals, firstLogFileName)
	c.Assert(l.state.ProcessID, check.Equals, uint64(1))

	l2, err := NewDirLog(DirLogConfig{
		Dir: dir,
	})
	c.Assert(err, check.IsNil)
	defer l2.Close()

	c.Assert(filepath.Base(l2.file.Name()), check.Equals, firstLogFileName)
	c.Assert(l2.state.ProcessID, check.Equals, uint64(2))

	err = l.Append(context.TODO(),
		Record{Type: OpCreate, Key: []byte("hello"), Val: []byte("world")})
	c.Assert(err, check.IsNil)

	err = l.Append(context.TODO(),
		Record{Type: OpCreate, Key: []byte("another"), Val: []byte("value")})
	c.Assert(err, check.IsNil)

	// compact and reopen the database
	err = l.tryCompactAndReopen(context.TODO())
	c.Assert(err, check.IsNil)

	c.Assert(filepath.Base(l.file.Name()), check.Equals, secondLogFileName)
	c.Assert(l.state.ProcessID, check.Equals, uint64(2))

	// both values should be there for both l1 and l2
	// and record IDs should be preserved
	s.expectRecord(c, l, "hello", []byte("world"), 1)
	s.expectRecord(c, l, "another", []byte("value"), 2)

	s.expectRecord(c, l2, "hello", []byte("world"), 1)
	s.expectRecord(c, l2, "another", []byte("value"), 2)

	c.Assert(filepath.Base(l2.file.Name()), check.Equals, secondLogFileName)
	c.Assert(l2.state.ProcessID, check.Equals, uint64(3))
}

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

	// start another watcher at offset
	watcherOffset, err := l.NewWatcher(nil, &Offset{RecordID: 1})
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

func (d *DirSuite) BenchmarkOperations(c *check.C) {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir:        dir,
		PollPeriod: 10 * time.Millisecond,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	ctx := context.TODO()
	keys := []string{"/bench/bucket/key1", "/bench/bucket/key2", "/bench/bucket/key3", "/bench/bucket/key4", "/bench/bucket/key5"}
	value1 := "some backend value, not large enough, but not small enough"
	for i := 0; i < c.N; i++ {
		for _, key := range keys {
			err := l.Append(ctx, Record{Type: OpPut, Key: []byte(key), Val: []byte(value1)})
			c.Assert(err, check.IsNil)
			item, err := l.Get(key)
			c.Assert(err, check.IsNil)
			c.Assert(string(item.Val), check.Equals, value1)
			err = l.Append(ctx, Record{Type: OpDelete, Key: []byte(key), Val: []byte(value1)})
			c.Assert(err, check.IsNil)
		}
	}
}
