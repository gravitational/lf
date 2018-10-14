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
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	err = l.Create(Item{Key: []byte("hello"), Val: []byte("world")})
	c.Assert(err, check.IsNil)

	out, err := l.Get([]byte("hello"))
	c.Assert(err, check.IsNil)
	c.Assert(out.Val, check.DeepEquals, []byte("world"))
	c.Assert(out.ID, check.Equals, uint64(1))

	l2, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(err, check.IsNil)
	defer l2.Close()

	out, err = l2.Get([]byte("hello"))
	c.Assert(err, check.IsNil)
	c.Assert(out.Val, check.DeepEquals, []byte("world"))
	c.Assert(out.ID, check.Equals, uint64(1))

	err = l2.Create(Item{Key: []byte("another"), Val: []byte("record")})
	c.Assert(err, check.IsNil)

	out, err = l.Get([]byte("another"))
	c.Assert(err, check.IsNil)
	c.Assert(out.Val, check.DeepEquals, []byte("record"))
	c.Assert(out.ID, check.Equals, uint64(2))

	// update existing record
	err = l.Update(Item{Key: []byte("another"), Val: []byte("value 2")})
	c.Assert(err, check.IsNil)

	out, err = l.Get([]byte("another"))
	c.Assert(err, check.IsNil)
	c.Assert(out.Val, check.DeepEquals, []byte("value 2"))
	c.Assert(out.ID, check.Equals, uint64(3))

	out, err = l2.Get([]byte("another"))
	c.Assert(err, check.IsNil)
	c.Assert(out.Val, check.DeepEquals, []byte("value 2"))
	c.Assert(out.ID, check.Equals, uint64(3))

	// delete a record
	err = l.Delete([]byte("another"))
	c.Assert(err, check.IsNil)

	_, err = l.Get([]byte("another"))
	c.Assert(trace.IsNotFound(err), check.Equals, true)

	_, err = l2.Get([]byte("another"))
	c.Assert(trace.IsNotFound(err), check.Equals, true)

	// concurrent create will fail
	err = l.Create(Item{Key: []byte("third record"), Val: []byte("value 3")})
	c.Assert(err, check.IsNil)

	err = l2.Create(Item{Key: []byte("third record"), Val: []byte("value 4")})
	c.Assert(trace.IsAlreadyExists(err), check.Equals, true)

	err = l2.Put(Item{Key: []byte("third record"), Val: []byte("value 4")})
	c.Assert(err, check.IsNil)

	out, err = l.Get([]byte("third record"))
	c.Assert(err, check.IsNil)
	c.Assert(out.Val, check.DeepEquals, []byte("value 4"))
	// there were 6 operations on the database
	c.Assert(out.ID, check.Equals, uint64(6))
}

// TestRanges tests range queries
func (s *DirSuite) TestRanges(c *check.C) {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	err = l.Create(Item{Key: []byte("/prefix/a"), Val: []byte("val a")})
	c.Assert(err, check.IsNil)

	err = l.Create(Item{Key: []byte("/prefix/b"), Val: []byte("val b")})
	c.Assert(err, check.IsNil)

	err = l.Create(Item{Key: []byte("/prefix/c/c1"), Val: []byte("val c1")})
	c.Assert(err, check.IsNil)

	err = l.Create(Item{Key: []byte("/prefix/c/c2"), Val: []byte("val c2")})
	c.Assert(err, check.IsNil)

	// add element that does not match the range to make
	// sure it won't get included in the list
	err = l.Create(Item{Key: []byte("a"), Val: []byte("no match a")})
	c.Assert(err, check.IsNil)

	// prefix range fetch
	result, err := l.GetRange([]byte("/prefix"), Range{MatchPrefix: true})
	c.Assert(err, check.IsNil)
	expected := []Item{
		{Key: []byte("/prefix/a"), Val: []byte("val a")},
		{Key: []byte("/prefix/b"), Val: []byte("val b")},
		{Key: []byte("/prefix/c/c1"), Val: []byte("val c1")},
		{Key: []byte("/prefix/c/c2"), Val: []byte("val c2")},
	}
	expectItems(c, result.Items, expected)

	l2, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(err, check.IsNil)
	defer l2.Close()

	result, err = l2.GetRange([]byte("/prefix"), Range{MatchPrefix: true})
	expectItems(c, result.Items, expected)

	// sub prefix range fetch
	result, err = l.GetRange([]byte("/prefix/c"), Range{MatchPrefix: true})
	c.Assert(err, check.IsNil)
	expected = []Item{
		{Key: []byte("/prefix/c/c1"), Val: []byte("val c1")},
		{Key: []byte("/prefix/c/c2"), Val: []byte("val c2")},
	}
	expectItems(c, result.Items, expected)

	result, err = l2.GetRange([]byte("/prefix/c"), Range{MatchPrefix: true})
	expectItems(c, result.Items, expected)

	// range match
	result, err = l.GetRange([]byte("/prefix/c/c1"), Range{LessThan: []byte("/prefix/c/cz")})
	expectItems(c, result.Items, expected)

	result, err = l2.GetRange([]byte("/prefix/c/c1"), Range{LessThan: []byte("/prefix/c/cz")})
	expectItems(c, result.Items, expected)
}

// TestLargeRecord tests scenario
// when large record is marshaled and unmarshaled
func (s *DirSuite) recordSizes(c *check.C, keySize, valSize int) error {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
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

	err = l.Append(r)
	if err != nil {
		return trace.Wrap(err)
	}

	l2, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	defer l2.Close()

	out, err := l2.Get(r.Key)
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

func expectRecord(c *check.C, l *DirLog, key string, val []byte, id uint64) {
	out, err := l.Get([]byte(key))
	c.Assert(err, check.IsNil)
	c.Assert(out.Val, check.DeepEquals, val)
	c.Assert(out.ID, check.Equals, id)
}

func expectItems(c *check.C, items, expected []Item) {
	if len(items) != len(expected) {
		c.Fatalf("Expected %v items, got %v.", len(expected), len(items))
	}
	for i := range items {
		c.Assert(string(items[i].Key), check.Equals, string(expected[i].Key))
		c.Assert(string(items[i].Val), check.Equals, string(expected[i].Val))
	}
}

// TestCompaction verifies compaction and concurrent
// operations
func (s *DirSuite) TestCompaction(c *check.C) {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	c.Assert(filepath.Base(l.file.Name()), check.Equals, "0.lf")
	c.Assert(l.state.ProcessID, check.Equals, uint64(1))

	l2, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(err, check.IsNil)
	defer l2.Close()

	c.Assert(filepath.Base(l2.file.Name()), check.Equals, "0.lf")
	c.Assert(l2.state.ProcessID, check.Equals, uint64(2))

	err = l.Put(Item{Key: []byte("hello"), Val: []byte("world")})
	c.Assert(err, check.IsNil)

	err = l.Put(Item{Key: []byte("another"), Val: []byte("value")})
	c.Assert(err, check.IsNil)

	// compact and reopen the database
	err = l.tryCompactAndReopen(context.TODO())
	c.Assert(err, check.IsNil)

	c.Assert(filepath.Base(l.file.Name()), check.Equals, "1.lf")
	c.Assert(l.state.ProcessID, check.Equals, uint64(2))

	// both values should be there for both l1 and l2
	// and record IDs should be preserved
	expectRecord(c, l, "hello", []byte("world"), 1)
	expectRecord(c, l, "another", []byte("value"), 2)

	expectRecord(c, l2, "hello", []byte("world"), 1)
	expectRecord(c, l2, "another", []byte("value"), 2)

	c.Assert(filepath.Base(l2.file.Name()), check.Equals, "1.lf")
	c.Assert(l2.state.ProcessID, check.Equals, uint64(3))
}

func (d *DirSuite) BenchmarkOperations(c *check.C) {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		PollPeriod:          10 * time.Millisecond,
		CompactionsDisabled: true,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	keys := []string{"/bench/bucket/key1", "/bench/bucket/key2", "/bench/bucket/key3", "/bench/bucket/key4", "/bench/bucket/key5"}
	value1 := "some backend value, not large enough, but not small enough"
	for i := 0; i < c.N; i++ {
		for _, key := range keys {
			err := l.Put(Item{Key: []byte(key), Val: []byte(value1)})
			c.Assert(err, check.IsNil)
			item, err := l.Get([]byte(key))
			c.Assert(err, check.IsNil)
			c.Assert(string(item.Val), check.Equals, value1)
			err = l.Delete([]byte(key))
			c.Assert(err, check.IsNil)
		}
	}
}

// TestRepairCreate tests repair operation in case if the first value
// used to create a value have been corrupted
func (s *DirSuite) TestRepairCreate(c *check.C) {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	err = l.Create(Item{Key: []byte("hello"), Val: []byte("world")})
	c.Assert(err, check.IsNil)

	err = l.Update(Item{Key: []byte("hello"), Val: []byte("value 2")})
	c.Assert(err, check.IsNil)

	err = l.Put(Item{Key: []byte("hello2"), Val: []byte("value 3")})
	c.Assert(err, check.IsNil)

	_, err = l.file.Seek(400, 0)
	c.Assert(err, check.IsNil)

	_, err = l.file.Write([]byte("damage!"))
	c.Assert(err, check.IsNil)

	l.Close()
	_, err = NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(IsDataCorruptionError(err), check.Equals, true)

	l, err = NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(IsDataCorruptionError(err), check.Equals, true)

	// initiate repair procedure
	l, err = NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
		Repair:              true,
	})
	c.Assert(err, check.IsNil)

	// after repair, value 2 is preserved, however record IDs have been shifted
	// because repair rewrote the history and renumbered IDs
	out, err := l.Get([]byte("hello"))
	c.Assert(err, check.IsNil)
	c.Assert(string(out.Val), check.Equals, "value 2")
	c.Assert(out.ID, check.Equals, uint64(1))

	out, err = l.Get([]byte("hello2"))
	c.Assert(err, check.IsNil)
	c.Assert(string(out.Val), check.Equals, "value 3")
	c.Assert(out.ID, check.Equals, uint64(2))
}

// TestRepairShortWrite tests repair operation in case if the last value
// was not written in full
func (s *DirSuite) TestRepairShortWrite(c *check.C) {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	err = l.Create(Item{Key: []byte("hello"), Val: []byte("world")})
	c.Assert(err, check.IsNil)

	err = l.Update(Item{Key: []byte("hello"), Val: []byte("value 2")})
	c.Assert(err, check.IsNil)

	err = l.Put(Item{Key: []byte("hello2"), Val: []byte("value 3")})
	c.Assert(err, check.IsNil)

	fi, err := l.file.Stat()
	c.Assert(err, check.IsNil)

	err = l.file.Truncate(fi.Size() - 1)
	c.Assert(err, check.IsNil)
	l.Close()

	_, err = NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(IsDataCorruptionError(err), check.Equals, true)

	l, err = NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(IsDataCorruptionError(err), check.Equals, true)

	// initiate repair procedure
	l, err = NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
		Repair:              true,
	})
	c.Assert(err, check.IsNil)

	// after repair, value 2 is preserved, however record IDs have been shifted
	// because repair rewrote the history and renumbered IDs
	out, err := l.Get([]byte("hello"))
	c.Assert(err, check.IsNil)
	c.Assert(string(out.Val), check.Equals, "value 2")
	c.Assert(out.ID, check.Equals, uint64(1))

	// value 3 is lost
	_, err = l.Get([]byte("hello2"))
	c.Assert(trace.IsNotFound(err), check.Equals, true)
}

func makeVal(length int) []byte {
	out := make([]byte, length)
	for i := 0; i < length; i++ {
		out[i] = byte(i % 255)
	}
	return out
}

type repairCase struct {
	info            string
	keySize         int
	valSize         int
	corruptAtOffset int
	corruptData     []byte
}

// TestRepairLargeRecord tests scenario when
// a record is damaged in the start, middle and end
func (s *DirSuite) TestRepairLargeRecord(c *check.C) {
	testCases := []repairCase{
		{
			info:            "corrupted first record",
			keySize:         ContainerSizeBytes * 3,
			valSize:         ContainerSizeBytes*3 + 1,
			corruptAtOffset: ContainerSizeBytes + 1,
			corruptData:     []byte("damage"),
		},
		{
			info:            "corrupted middle",
			keySize:         ContainerSizeBytes * 3,
			valSize:         ContainerSizeBytes*3 + 1,
			corruptAtOffset: ContainerSizeBytes*2 + 1,
			corruptData:     []byte("damage"),
		},
		{
			info:            "corrupted value",
			keySize:         ContainerSizeBytes * 3,
			valSize:         ContainerSizeBytes*3 + 1,
			corruptAtOffset: ContainerSizeBytes*5 + 1,
			corruptData:     []byte("damage"),
		},
	}
	for _, tc := range testCases {
		s.repairLargeRecord(c, tc)
	}
}

func (s *DirSuite) repairLargeRecord(c *check.C, tc repairCase) {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	key := makeVal(tc.keySize)
	val := makeVal(tc.valSize)

	// first, put a small record
	err = l.Put(Item{Key: []byte("a"), Val: []byte("a val")})
	c.Assert(err, check.IsNil)

	err = l.Create(Item{Key: key, Val: val})
	c.Assert(err, check.IsNil)

	err = l.Put(Item{Key: []byte("b"), Val: []byte("b val")})
	c.Assert(err, check.IsNil)

	_, err = l.file.Seek(int64(tc.corruptAtOffset), 0)
	c.Assert(err, check.IsNil)

	_, err = l.file.Write(tc.corruptData)
	c.Assert(err, check.IsNil)

	l.Close()

	_, err = NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(IsDataCorruptionError(err), check.Equals, true)

	l, err = NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(IsDataCorruptionError(err), check.Equals, true)

	// initiate repair procedure
	l, err = NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
		Repair:              true,
	})
	c.Assert(err, check.IsNil)

	// after repair, values 1 and 3 are preserved
	out, err := l.Get([]byte("a"))
	c.Assert(err, check.IsNil)
	c.Assert(string(out.Val), check.Equals, "a val")
	c.Assert(out.ID, check.Equals, uint64(1))

	out, err = l.Get([]byte("b"))
	c.Assert(err, check.IsNil)
	c.Assert(string(out.Val), check.Equals, "b val")
	c.Assert(out.ID, check.Equals, uint64(2))

	// value 3 is lost
	_, err = l.Get(key)
	c.Assert(trace.IsNotFound(err), check.Equals, true)
}

// CompareAndSwap tests compare and swap functionality
func (s *DirSuite) TestCompareAndSwap(c *check.C) {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	// compare and swap on non existing value will fail
	err = l.CompareAndSwap(Item{Key: []byte("one"), Val: []byte("1")}, Item{Key: []byte("one"), Val: []byte("2")})
	c.Assert(trace.IsCompareFailed(err), check.Equals, true)

	err = l.Create(Item{Key: []byte("one"), Val: []byte("1")})
	c.Assert(err, check.IsNil)

	l2, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(err, check.IsNil)
	defer l2.Close()

	// success CAS!
	err = l2.CompareAndSwap(Item{Key: []byte("one"), Val: []byte("1")}, Item{Key: []byte("one"), Val: []byte("2")})
	c.Assert(err, check.IsNil)

	out, err := l.Get([]byte("one"))
	c.Assert(err, check.IsNil)
	c.Assert(string(out.Val), check.Equals, "2")

	// value has been updated - not '1' any more
	err = l.CompareAndSwap(Item{Key: []byte("one"), Val: []byte("1")}, Item{Key: []byte("one"), Val: []byte("3")})
	c.Assert(trace.IsCompareFailed(err), check.Equals, true)

	// existing value has not been changed by the failed CAS operation
	out, err = l.Get([]byte("one"))
	c.Assert(err, check.IsNil)
	c.Assert(string(out.Val), check.Equals, "2")
}

func (s *DirSuite) TestConcurrentOperations(c *check.C) {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	l2, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
	})
	c.Assert(err, check.IsNil)
	defer l2.Close()

	value1 := "this first value should not be corrupted by concurrent ops"
	value2 := "this second value should not be corrupted too"
	const attempts = 50
	resultsC := make(chan struct{}, attempts*4)
	for i := 0; i < attempts; i++ {
		go func(cnt int) {
			err := l.Put(Item{Key: []byte("key"), Val: []byte(value1)})
			resultsC <- struct{}{}
			c.Assert(err, check.IsNil)
		}(i)

		go func(cnt int) {
			err := l2.CompareAndSwap(Item{Key: []byte("key"), Val: []byte(value2)}, Item{Key: []byte("key"), Val: []byte(value1)})
			resultsC <- struct{}{}
			if err != nil && !trace.IsCompareFailed(err) {
				c.Assert(err, check.IsNil)
			}
		}(i)

		go func(cnt int) {
			err := l2.Create(Item{Key: []byte("key"), Val: []byte(value2)})
			resultsC <- struct{}{}
			if err != nil && !trace.IsAlreadyExists(err) {
				c.Assert(err, check.IsNil)
			}
		}(i)

		go func(cnt int) {
			item, err := l.Get([]byte("key"))
			resultsC <- struct{}{}
			if err != nil && !trace.IsNotFound(err) {
				c.Assert(err, check.IsNil)
			}
			// make sure data is not corrupted along the way
			if err == nil {
				val := string(item.Val)
				if val != value1 && val != value2 {
					c.Fatalf("expected one of %q or %q and got %q", value1, value2, val)
				}
			}
		}(i)

		go func(cnt int) {
			err := l2.Delete([]byte("key"))
			if err != nil && !trace.IsNotFound(err) {
				c.Assert(err, check.IsNil)
			}
			resultsC <- struct{}{}
		}(i)
	}
	timeoutC := time.After(3 * time.Second)
	for i := 0; i < attempts*5; i++ {
		select {
		case <-resultsC:
		case <-timeoutC:
			c.Fatalf("timeout waiting for goroutines to finish")
		}
	}
}
