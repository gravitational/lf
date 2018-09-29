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
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/gravitational/trace"
)

const (
	// ContainerSizeBytes is a size in bytes of a container
	ContainerSizeBytes = 4096
	// headerSizeBytes is a size in bytes of a serialized container header
	headerSizeBytes = 6
)

// ContainerMarshaler marshals data in "containers" - 4K data blocks
// with crc32 checksum and length
type ContainerMarshaler struct {
	table *crc32.Table
}

// NewContainerMarshaler returns a new instance of container marshaler
func NewContainerMarshaler() *ContainerMarshaler {
	return &ContainerMarshaler{
		// table for fast crc32 calculation,
		// has to be created only once
		table: crc32.MakeTable(crc32.IEEE),
	}
}

// Marshal marshals container to 4K byte chunk,
// calculates CRC and pads extra bytes with zeros,
// will return error if container data does not fit,
// if dest does not have enough bytes, it will panic with out of boundaries
func (c *ContainerMarshaler) Marshal(dest []byte, source []byte) error {
	// check boundaries
	if len(dest) < ContainerSizeBytes {
		return trace.BadParameter("dest should be at least %v bytes", ContainerSizeBytes)
	}

	if len(source) > ContainerSizeBytes-headerSizeBytes {
		return trace.BadParameter("source does not fit in the %v bytes", ContainerSizeBytes-headerSizeBytes)
	}

	// 2 bytes for length
	binary.LittleEndian.PutUint16(dest[4:6], uint16(len(source)))

	// rest for the data
	if copied := copy(dest[6:], source); copied < len(source) {
		return trace.BadParameter("something went wrong, copied not enough data")
	}

	// zero extra bytes
	padBytes := (ContainerSizeBytes - headerSizeBytes) - len(source)
	for i := 0; i < padBytes; i++ {
		dest[ContainerSizeBytes-i-1] = 0
	}

	// write checksum
	checksum := crc32.Checksum(dest[4:], c.table)
	binary.LittleEndian.PutUint32(dest[:4], checksum)

	return nil
}

// Unmarshal unmarshals 4K byte source slice, and copies result into dest,
// returns bytes copied in case of success, -1 and error in case of error
func (c *ContainerMarshaler) Unmarshal(dest []byte, source []byte) (int, error) {
	if len(source) < ContainerSizeBytes {
		return -1, trace.BadParameter("source should at least %v bytes", ContainerSizeBytes)
	}

	// read CRC32 checksum
	expectedChecksum := binary.LittleEndian.Uint32(source)

	// compute checksum over data
	computedChecksum := crc32.Checksum(source[4:], c.table)
	if expectedChecksum != computedChecksum {
		return -1, trace.Wrap(&DataCorruptionError{
			ExpectedChecksum: expectedChecksum,
			ComputedChecksum: computedChecksum,
		})
	}

	// now, checksum has been validated, we can trust the rest
	dataLength := int(binary.LittleEndian.Uint16(source[4:6]))
	if dataLength > ContainerSizeBytes-headerSizeBytes {
		return -1, trace.BadParameter("length of container data should be <= ", ContainerSizeBytes-headerSizeBytes)
	}
	// check all various conditions
	if len(dest) < dataLength {
		return -1, trace.BadParameter("dest should be at least %v bytes", ContainerSizeBytes)
	}

	copy(dest, source[headerSizeBytes:dataLength+headerSizeBytes])

	return int(dataLength), nil
}

// DataCorruptionError indicates that data has been corrupted
type DataCorruptionError struct {
	// ExpectedChecksum is an expected checksum value
	ExpectedChecksum uint32
	// ComputedChecksum is a computed checksum value
	ComputedChecksum uint32
}

// Error returns a formatted user-friendly message
func (e DataCorruptionError) Error() string {
	return fmt.Sprintf("checksum mismatch, expected checksum %v != computed checksum %v",
		e.ExpectedChecksum, e.ComputedChecksum)
}

// IsDataCorruptionError returns true if given error is data corruption error
func IsDataCorruptionError(e error) bool {
	_, ok := trace.Unwrap(e).(*DataCorruptionError)
	return ok
}

// ContainerMarshalString is a utility function to marshal string of bytes
// to a log container
func ContainerMarshalString(in string) ([]byte, error) {
	return ContainerMarshal([]byte(in))
}

// ContainerUnmarshalString is a utility function to unmarshal string of bytes
// from a marshaled container
func ContainerUnmarshalString(in []byte) (string, error) {
	out, err := ContainerUnmarshal(in)
	if err != nil {
		return "", trace.Wrap(err)
	}
	return string(out), nil
}

// ContainerMarshal is a utility function to marshal a slice of bytes
// to a log container
func ContainerMarshal(in []byte) ([]byte, error) {
	m := NewContainerMarshaler()
	out := make([]byte, ContainerSizeBytes)
	if err := m.Marshal(out, in); err != nil {
		return nil, trace.Wrap(err)
	}
	return out, nil
}

// ContainerUnmarshal is a utility function to unmarshal a slice of bytes
// from a marshaled container
func ContainerUnmarshal(in []byte) ([]byte, error) {
	out := make([]byte, ContainerSizeBytes)
	m := NewContainerMarshaler()
	length, err := m.Unmarshal(out, in)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return out[:length], nil
}
