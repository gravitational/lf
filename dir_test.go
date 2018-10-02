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
	"context"

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
}

// TestLargeRecord tests scenario
// when large record is marshaled
func (s *DirSuite) TestLargeRecord(c *check.C) {
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir: dir,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	r := Record{Type: OpCreate}
	r.Key = make([]byte, ContainerSizeBytes+2)
	r.Val = make([]byte, ContainerSizeBytes+15)
	for i := 0; i < len(r.Key); i++ {
		r.Key[i] = byte(i % 255)
	}
	for i := 0; i < len(r.Val); i++ {
		r.Val[i] = byte(i % 255)
	}

	err = l.Append(context.TODO(), r)
	c.Assert(err, check.IsNil)

	l2, err := NewDirLog(DirLogConfig{
		Dir: dir,
	})
	c.Assert(err, check.IsNil)
	defer l2.Close()

	out, err := l2.Get(string(r.Key))
	c.Assert(err, check.IsNil)
	c.Assert(out, check.DeepEquals, r.Val)
}
