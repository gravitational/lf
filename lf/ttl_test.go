package lf

import (
	"context"
	"time"

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	check "gopkg.in/check.v1"
)

// TestTTLCRUD tests concurrent reads/writes
// with TTL enabled
func (s *DirSuite) TestTTLCRUD(c *check.C) {
	dir := c.MkDir()
	clock := clockwork.NewFakeClock()
	l, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		PollPeriod:          10 * time.Millisecond,
		CompactionsDisabled: true,
		Clock:               clock,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	// put one record with TTL of 1 second
	now := clock.Now().UTC()
	err = l.Put(Item{Key: []byte("a"), Val: []byte("a val"), Expires: now.Add(time.Second)})
	c.Assert(err, check.IsNil)

	// another with TTL of 2 seconds
	err = l.Put(Item{Key: []byte("b"), Val: []byte("b val"), Expires: now.Add(3 * time.Second)})
	c.Assert(err, check.IsNil)

	clock.Advance(2 * time.Second)

	l2, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
		Clock:               clock,
	})
	c.Assert(err, check.IsNil)
	defer l2.Close()

	// both backend see object a as expired, object b as present
	_, err = l.Get([]byte("a"))
	c.Assert(trace.IsNotFound(err), check.Equals, true)

	_, err = l2.Get([]byte("a"))
	c.Assert(trace.IsNotFound(err), check.Equals, true)

	out, err := l.Get([]byte("b"))
	c.Assert(err, check.IsNil)
	c.Assert(string(out.Val), check.Equals, "b val")

	out, err = l2.Get([]byte("b"))
	c.Assert(err, check.IsNil)
	c.Assert(string(out.Val), check.Equals, "b val")

	// advance clock to 4 seconds, b is gone too
	clock.Advance(2 * time.Second)

	_, err = l.Get([]byte("b"))
	c.Assert(trace.IsNotFound(err), check.Equals, true)

	_, err = l2.Get([]byte("b"))
	c.Assert(trace.IsNotFound(err), check.Equals, true)
}

// TestTTLCreate test scenario when create succeeds
// when record is expired
func (s *DirSuite) TestTTLCreate(c *check.C) {
	dir := c.MkDir()
	clock := clockwork.NewFakeClock()
	l, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		PollPeriod:          10 * time.Millisecond,
		CompactionsDisabled: true,
		Clock:               clock,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	// put one record with TTL of 1 second
	now := clock.Now().UTC()
	err = l.Put(Item{Key: []byte("a"), Val: []byte("a val"), Expires: now.Add(time.Second)})
	c.Assert(err, check.IsNil)

	clock.Advance(2 * time.Second)

	// both backend see object a as expired, object b as present
	err = l.Create(Item{Key: []byte("a"), Val: []byte("a val 2")})
	c.Assert(err, check.IsNil)

	out, err := l.Get([]byte("a"))
	c.Assert(err, check.IsNil)
	c.Assert(string(out.Val), check.Equals, "a val 2")
}

// TestTTLCompaction tests scenarios of compacted
// log with TTL entries
func (s *DirSuite) TestTTLCompaction(c *check.C) {
	dir := c.MkDir()
	clock := clockwork.NewFakeClock()
	l, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		PollPeriod:          10 * time.Millisecond,
		CompactionsDisabled: true,
		Clock:               clock,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	// put one record with TTL of 1 second
	now := clock.Now().UTC()
	err = l.Put(Item{Key: []byte("/a"), Val: []byte("a val"), Expires: now.Add(time.Second)})
	c.Assert(err, check.IsNil)

	err = l.Put(Item{Key: []byte("/b"), Val: []byte("b val"), Expires: now.Add(3 * time.Second)})
	c.Assert(err, check.IsNil)

	err = l.Put(Item{Key: []byte("/c"), Val: []byte("c val"), Expires: now.Add(3 * time.Second)})
	c.Assert(err, check.IsNil)

	clock.Advance(2 * time.Second)

	// compact and reopen the database
	err = l.tryCompactAndReopen(context.TODO())
	c.Assert(err, check.IsNil)

	result, err := l.GetRange([]byte("/"), Range{MatchPrefix: true})
	c.Assert(err, check.IsNil)

	expected := []Item{
		{Key: []byte("/b"), Val: []byte("b val")},
		{Key: []byte("/c"), Val: []byte("c val")},
	}
	expectItems(c, result.Items, expected)
}

// TestTTLOverwrite tests scenario when TTL is
// overrided before expiry
func (s *DirSuite) TestTTLOverride(c *check.C) {
	dir := c.MkDir()
	clock := clockwork.NewFakeClock()
	l, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		PollPeriod:          10 * time.Millisecond,
		CompactionsDisabled: true,
		Clock:               clock,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	// put one record with TTL of 1 second
	now := clock.Now().UTC()
	err = l.Put(Item{Key: []byte("a"), Val: []byte("a val"), Expires: now.Add(time.Second)})
	c.Assert(err, check.IsNil)

	// Update with value that does not expire
	err = l.Put(Item{Key: []byte("a"), Val: []byte("a val 2")})
	c.Assert(err, check.IsNil)

	// Put another record with TTL of 1 second
	err = l.Put(Item{Key: []byte("b"), Val: []byte("b val"), Expires: now.Add(time.Second)})
	c.Assert(err, check.IsNil)

	// Update with value that expires, but later
	err = l.Put(Item{Key: []byte("b"), Val: []byte("b val 2"), Expires: now.Add(3 * time.Second)})
	c.Assert(err, check.IsNil)

	clock.Advance(2 * time.Second)

	l2, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
		Clock:               clock,
	})
	c.Assert(err, check.IsNil)
	defer l2.Close()

	// both backends see both objects as present and returning last values
	out, err := l.Get([]byte("a"))
	c.Assert(err, check.IsNil)
	c.Assert(string(out.Val), check.Equals, "a val 2")

	out, err = l2.Get([]byte("a"))
	c.Assert(err, check.IsNil)
	c.Assert(string(out.Val), check.Equals, "a val 2")

	out, err = l.Get([]byte("b"))
	c.Assert(err, check.IsNil)
	c.Assert(string(out.Val), check.Equals, "b val 2")

	out, err = l2.Get([]byte("b"))
	c.Assert(err, check.IsNil)
	c.Assert(string(out.Val), check.Equals, "b val 2")
}

// TestTTLCompareAndSwap tests compare and swap functionality
func (s *DirSuite) TestTTLCompareAndSwap(c *check.C) {
	clock := clockwork.NewFakeClock()
	dir := c.MkDir()
	l, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
		Clock:               clock,
	})
	c.Assert(err, check.IsNil)
	defer l.Close()

	now := clock.Now().UTC()

	err = l.Create(Item{Key: []byte("one"), Val: []byte("1"), Expires: now.Add(time.Second)})
	c.Assert(err, check.IsNil)

	clock.Advance(2 * time.Second)

	// compare and swap on non existing value will fail
	err = l.CompareAndSwap(Item{Key: []byte("one"), Val: []byte("1")}, Item{Key: []byte("one"), Val: []byte("2")})
	c.Assert(trace.IsCompareFailed(err), check.Equals, true)

	now = clock.Now().UTC()
	err = l.Create(Item{Key: []byte("one"), Val: []byte("1"), Expires: now.Add(time.Second)})
	c.Assert(err, check.IsNil)

	l2, err := NewDirLog(DirLogConfig{
		Dir:                 dir,
		CompactionsDisabled: true,
		Clock:               clock,
	})
	c.Assert(err, check.IsNil)
	defer l2.Close()

	// success CAS!
	err = l2.CompareAndSwap(Item{Key: []byte("one"), Val: []byte("1")}, Item{Key: []byte("one"), Val: []byte("2"), Expires: now.Add(2 * time.Second)})
	c.Assert(err, check.IsNil)

	out, err := l.Get([]byte("one"))
	c.Assert(err, check.IsNil)
	c.Assert(string(out.Val), check.Equals, "2")

	clock.Advance(3 * time.Second)

	// value has expired, compare and swap will fail
	err = l2.CompareAndSwap(Item{Key: []byte("one"), Val: []byte("2")}, Item{Key: []byte("one"), Val: []byte("3")})
	c.Assert(trace.IsCompareFailed(err), check.Equals, true)

	// value has expired, not found
	_, err = l.Get([]byte("one"))
	c.Assert(trace.IsNotFound(err), check.Equals, true)
}
