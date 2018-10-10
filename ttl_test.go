package lf

import (
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
