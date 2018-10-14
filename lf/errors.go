package lf

import (
	"fmt"

	"github.com/gravitational/trace"
)

// ChecksumError indicates that data has been corrupted
type ChecksumError struct {
	// ExpectedChecksum is an expected checksum value
	ExpectedChecksum uint32
	// ComputedChecksum is a computed checksum value
	ComputedChecksum uint32
}

// Error returns a formatted user-friendly message
func (e ChecksumError) Error() string {
	return fmt.Sprintf("checksum mismatch, expected checksum %v != computed checksum %v for container",
		e.ExpectedChecksum, e.ComputedChecksum)
}

// LogReadError indicates that data has been corrupted,
// and reader could not read the log
type LogReadError struct {
	Message string
}

// Error returns a formatted user-friendly message
func (e LogReadError) Error() string {
	return e.Message
}

// IsDataCorruptionError returns true if given error is data corruption error
func IsDataCorruptionError(e error) bool {
	switch trace.Unwrap(e).(type) {
	case *ChecksumError, *LogReadError:
		return true
	}
	return false
}

// CompactionRequiredError indicates that log requires compaction
type CompactionRequiredError struct {
}

// Error returns a formatted user-friendly message
func (e CompactionRequiredError) Error() string {
	return fmt.Sprintf("compaction is required")
}

// IsCompactionRequiredError returns true if given error indicates that compaction
// is required
func IsCompactionRequiredError(e error) bool {
	_, ok := trace.Unwrap(e).(*CompactionRequiredError)
	return ok
}

// PartialReadError indicates that record was read partially
type PartialReadError struct {
}

// Error returns a formatted user-friendly message
func (e PartialReadError) Error() string {
	return fmt.Sprintf("compaction is required")
}

// IsPartialReadError returns true if given error indicates that
// record has been partially read
func IsPartialReadError(e error) bool {
	_, ok := trace.Unwrap(e).(*PartialReadError)
	return ok
}

// ReopenDatabaseError indicates that database has to be reopened
// after compaction
type ReopenDatabaseError struct {
}

// Error returns a formatted user-friendly message
func (e ReopenDatabaseError) Error() string {
	return "reopen the database"
}

// IsReopenDatabaseError returns true if given error indicates that database
// has to be reopened
func IsReopenDatabaseError(e error) bool {
	_, ok := trace.Unwrap(e).(*ReopenDatabaseError)
	return ok
}
