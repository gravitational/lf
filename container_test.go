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
	"testing"

	"github.com/gravitational/trace"
	check "gopkg.in/check.v1"
)

func TestInit(t *testing.T) { check.TestingT(t) }

type ContainerSuite struct{}

var _ = check.Suite(&ContainerSuite{})

func (s *ContainerSuite) SetUpSuite(c *check.C) {
}

// TestMarshalUnmarshal tests marshal unmarshal loop
// and various edge cases
func (s *ContainerSuite) TestMarshalUnmarshal(c *check.C) {
	type testCase struct {
		info string
		in   []byte
		out  []byte
		err  error
	}
	testCases := []testCase{
		{
			info: "empty in - empty out",
			in:   []byte{},
			out:  []byte{},
		},
		{
			info: "simple message",
			in:   []byte("hello, world!"),
		},
		{
			info: "data is too long",
			in:   make([]byte, ContainerSizeBytes-headerSizeBytes+1),
			err:  trace.BadParameter("too long"),
		},
	}
	m := NewContainerMarshaler()
	data, out := make([]byte, ContainerSizeBytes), make([]byte, ContainerSizeBytes)
	for i, tc := range testCases {
		comment := check.Commentf("test case %v %v", i, tc.info)
		err := m.Marshal(data, tc.in)
		if tc.err != nil {
			c.Assert(err, check.NotNil, comment)
			c.Assert(tc.err, check.FitsTypeOf, err, comment)
		} else {
			length, err := m.Unmarshal(out, data)
			c.Assert(err, check.IsNil, comment)
			c.Assert(length, check.Equals, len(tc.in))
			c.Assert(out[:length], check.DeepEquals, tc.in)
		}
	}
}

// TestFormat makes sure that on disk format matches our
// expectations
func (s *ContainerSuite) TestFormat(c *check.C) {
	message := "hello, world"
	data, err := ContainerMarshalString(message)
	c.Assert(err, check.IsNil)
	// make sure the data is there
	c.Assert(string(data[headerSizeBytes:len(message)+headerSizeBytes]), check.Equals, message)
	// make sure the length is little endian length of the message
	c.Assert(data[4:6], check.DeepEquals, []byte{byte(len(message)), 0})
	// make sure the padding is there
	for i := 0; i < ContainerSizeBytes-len(message)-headerSizeBytes; i++ {
		c.Assert(data[i+headerSizeBytes+len(message)], check.Equals, byte(0))
	}
}

// TestDataCorruption tests data corruption logic
func (s *ContainerSuite) TestDataCorruption(c *check.C) {
	m := NewContainerMarshaler()
	out := make([]byte, ContainerSizeBytes)
	message := []byte("testing data corruption")
	err := m.Marshal(out, message)
	c.Assert(err, check.IsNil)

	// first, unmarshal message, and make sure it's ok
	outString, err := ContainerUnmarshalString(out)
	c.Assert(err, check.IsNil)
	c.Assert(outString, check.Equals, string(message))

	// flip any bit in any code and make sure corruption is detected
	for i := 0; i < ContainerSizeBytes; i++ {
		for bit := byte(0); bit < 8; bit++ {
			// corrupt CRC32 code by flipping one bit in CRC32 code
			data := copyContainer(out)
			data[i] = data[i] ^ 1<<bit
			result := make([]byte, ContainerSizeBytes)
			_, err = m.Unmarshal(result, data)
			c.Assert(trace.Unwrap(err), check.FitsTypeOf, &DataCorruptionError{})
			c.Assert(IsDataCorruptionError(err), check.Equals, true)
		}
	}
}

func copyContainer(in []byte) []byte {
	out := make([]byte, ContainerSizeBytes)
	copy(out, in)
	return out
}
