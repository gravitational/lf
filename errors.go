package lf

import (
	"fmt"

	"github.com/gravitational/trace"
)

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
