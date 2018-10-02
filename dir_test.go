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

	"github.com/gravitational/trace"
	check "gopkg.in/check.v1"
)

type DirSuite struct{}

var _ = check.Suite(&DirSuite{})

func (s *DirSuite) SetUpSuite(c *check.C) {
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
	c.Assert(out, check.DeepEquals, []byte("world"))

	l2, err := NewDirLog(DirLogConfig{
		Dir: dir,
	})
	c.Assert(err, check.IsNil)
	defer l2.Close()

	out, err = l2.Get("hello")
	c.Assert(err, check.IsNil)
	c.Assert(out, check.DeepEquals, []byte("world"))

	err = l2.Append(context.TODO(),
		Record{Type: OpCreate, Key: []byte("another"), Val: []byte("record")})
	c.Assert(err, check.IsNil)

	out, err = l.Get("another")
	c.Assert(err, check.IsNil)
	c.Assert(out, check.DeepEquals, []byte("record"))

	// update existing record
	err = l.Append(context.TODO(),
		Record{Type: OpUpdate, Key: []byte("another"), Val: []byte("value 2")})
	c.Assert(err, check.IsNil)

	out, err = l.Get("another")
	c.Assert(err, check.IsNil)
	c.Assert(out, check.DeepEquals, []byte("value 2"))

	out, err = l2.Get("another")
	c.Assert(err, check.IsNil)
	c.Assert(out, check.DeepEquals, []byte("value 2"))

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
	if !bytes.Equal(out, r.Val) {
		return trace.CompareFailed("%v != %v", len(r.Val), len(out))
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
