package miniredis

// Tests for the unified transaction, expiration and observation semantics of
// MULTI/EXEC, WATCH and the clock.

import (
	"bufio"
	"bytes"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2/proto"
	"github.com/alicebob/miniredis/v2/server"
)

// A transaction reads all keys at one fixed logical instant: with ClockTTL a
// key which expires between the QUEUE and the EXEC is still visible to the
// whole transaction, and expires only after it.
func TestTxFixedLogicalTime(t *testing.T) {
	s, c := runWithClient(t)
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	s.SetTime(start)
	s.ClockTTL(true)

	mustOK(t, c, "SET", "foo", "bar", "EX", "10")
	mustOK(t, c, "MULTI")
	mustDo(t, c, "GET", "foo", proto.Inline("QUEUED"))

	// The clock moves past the TTL before EXEC runs. Starting the transaction
	// applies the expiry which is already due.
	s.SetTime(start.Add(time.Minute))
	mustDo(t, c, "EXEC", proto.Array(proto.Nil))

	// After the transaction the clock is live again, and the key is gone.
	mustNil(t, c, "GET", "foo")
	mustDo(t, c, "EXISTS", "foo", proto.Int(0))
}

// Moving the clock during EXEC itself doesn't change what a command sees. The
// EXEC callback runs while the clock is frozen, so a command that would expire
// a key halfway through must not.
func TestTxClockFrozenDuringExec(t *testing.T) {
	s, c := runWithClient(t)
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	s.SetTime(start)
	s.ClockTTL(true)

	mustOK(t, c, "SET", "a", "1", "EX", "10")
	mustOK(t, c, "SET", "b", "2", "EX", "10")

	mustOK(t, c, "MULTI")
	mustDo(t, c, "GET", "a", proto.Inline("QUEUED"))
	mustDo(t, c, "GET", "b", proto.Inline("QUEUED"))

	// EXPIREAT in the middle of the transaction uses the transaction's time,
	// not a time that has jumped ahead.
	s.SetTime(start.Add(time.Second))
	mustDo(t, c, "EXEC", proto.Array(proto.String("1"), proto.String("2")))
}

// A command which fails restores the writes and the TTL of that command, and
// the following commands still run.
func TestTxFailureRestoresWritesAndTTL(t *testing.T) {
	s, c := runWithClient(t)

	// Value and TTL of the failing command's own write.
	s.Set("h", "v")
	s.SetTTL("h", time.Minute)

	// The failing INCR writes nothing itself, so give it a command which fails
	// after it wrote: BITOP validates its sources after building a result.
	s.Set("dst", "keep")
	s.SetTTL("dst", time.Minute)
	s.Set("src", "abc")

	mustOK(t, c, "MULTI")
	mustDo(t, c, "SET", "h", "other", proto.Inline("QUEUED"))
	mustDo(t, c, "BITOP", "NOT", "dst", "src", "src", proto.Inline("QUEUED"))
	mustDo(t, c, "SET", "h", "last", proto.Inline("QUEUED"))
	mustDo(t, c, "EXEC",
		proto.Array(
			proto.Inline("OK"),
			proto.Error("ERR BITOP NOT must be called with a single source key."),
			proto.Inline("OK"),
		))

	// The failed BITOP left its destination and TTL untouched; the commands
	// before and after it did run.
	s.CheckGet(t, "dst", "keep")
	equals(t, time.Minute, s.TTL("dst"))
	s.CheckGet(t, "h", "last")
}

// With the error-tracking peer, a command which only writes after failing can
// be told apart from one which failed halfway: both leave no trace.
func TestTxPeerErrorTracking(t *testing.T) {
	var buf bytes.Buffer
	peer := server.NewPeer(bufio.NewWriter(&buf))
	peer.ResetError()
	if peer.Failed() {
		t.Fatal("a fresh peer has no failed command")
	}
	peer.WriteOK()
	if peer.Failed() {
		t.Fatal("OK is not an error")
	}
	peer.WriteError("ERR broken")
	if !peer.Failed() {
		t.Fatal("WriteError must mark the command failed")
	}
	peer.ResetError()
	if peer.Failed() {
		t.Fatal("ResetError must forget the failure")
	}
}

// A failed command doesn't bump the key version, so a WATCH taken before it
// still allows the next transaction.
func TestTxFailureKeepsKeyVersion(t *testing.T) {
	s, c := runWithClient(t)

	s.Set("k", "not a number")
	mustOK(t, c, "MULTI")
	mustDo(t, c, "INCR", "k", proto.Inline("QUEUED"))
	mustDo(t, c, "EXEC", proto.Array(proto.Error(msgInvalidInt)))

	// The failed INCR must not have changed the key, so a fresh WATCH succeeds.
	mustOK(t, c, "WATCH", "k")
	mustOK(t, c, "MULTI")
	mustDo(t, c, "PING", proto.Inline("QUEUED"))
	mustDo(t, c, "EXEC", proto.Array(proto.Inline("PONG")))
}

// A command which writes before it fails is rolled back as a whole, so it
// doesn't leave a partial value behind.
func TestTxPartialWriteRolledBack(t *testing.T) {
	s, c := runWithClient(t)

	s.Set("dst", "keep")
	s.Set("src", "abc")

	// BITOP NOT with two sources fails, but only after it used to have changed
	// the destination.
	mustOK(t, c, "MULTI")
	mustDo(t, c, "BITOP", "NOT", "dst", "src", "src", proto.Inline("QUEUED"))
	mustDo(t, c, "EXEC", proto.Array(proto.Error("ERR BITOP NOT must be called with a single source key.")))
	s.CheckGet(t, "dst", "keep")

	// An XADD with an invalid entry ID must not create an empty stream.
	mustOK(t, c, "MULTI")
	mustDo(t, c, "XADD", "stream", "0-0", "f", "v", proto.Inline("QUEUED"))
	mustDo(t, c, "EXEC", proto.Array(proto.Error(msgStreamIDZero)))
	equals(t, "", s.Type("stream"))
}

// The same sorts of failures outside a transaction also leave the database
// alone.
func TestFailedCommandLeavesDatabaseAlone(t *testing.T) {
	s, c := runWithClient(t)

	s.Set("dst", "keep")
	s.Set("src", "abc")
	mustDo(t, c, "BITOP", "NOT", "dst", "src", "src",
		proto.Error("ERR BITOP NOT must be called with a single source key."))
	s.CheckGet(t, "dst", "keep")

	// RESTORE parses its TTL before it writes.
	mustDo(t, c, "RESTORE", "key", "not-a-ttl", "value", proto.Error(msgInvalidInt))
	equals(t, false, s.Exists("key"))

	// XADD with an invalid ID doesn't leave a stream behind.
	mustDo(t, c, "XADD", "stream", "0-0", "f", "v", proto.Error(msgStreamIDZero))
	equals(t, "", s.Type("stream"))
}

// WATCH and commit share one version boundary: a failing transaction doesn't
// consume a version number, and a successful transaction's version is what a
// concurrent WATCH sees change.
func TestTxWatchSharesVersionBoundary(t *testing.T) {
	s, c := runWithClient(t)
	c2, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c2.Close()

	s.Set("k", "v")

	// A failed transaction leaves the version where it was...
	mustOK(t, c, "MULTI")
	mustDo(t, c, "INCR", "k", proto.Inline("QUEUED"))
	mustDo(t, c, "EXEC", proto.Array(proto.Error(msgInvalidInt)))
	mustOK(t, c, "WATCH", "k")
	mustOK(t, c, "MULTI")
	mustDo(t, c, "PING", proto.Inline("QUEUED"))
	mustDo(t, c, "EXEC", proto.Array(proto.Inline("PONG")))

	// ...and a successful one moves it, which aborts a watching transaction.
	mustOK(t, c, "WATCH", "k")
	mustOK(t, c2, "MULTI")
	mustDo(t, c2, "SET", "k", "new", proto.Inline("QUEUED"))
	mustDo(t, c2, "EXEC", proto.Array(proto.Inline("OK")))
	mustOK(t, c, "MULTI")
	mustDo(t, c, "PING", proto.Inline("QUEUED"))
	mustNilList(t, c, "EXEC")
}

// A failing command is undone without disturbing the successful commands
// before it, including a SWAPDB.
func TestTxRollbackKeepsEarlierCommands(t *testing.T) {
	s, c := runWithClient(t)

	mustOK(t, c, "SET", "a", "db0")
	mustOK(t, c, "SELECT", "1")
	mustOK(t, c, "SET", "b", "db1")
	mustOK(t, c, "SELECT", "0")

	mustOK(t, c, "MULTI")
	mustDo(t, c, "SWAPDB", "0", "1", proto.Inline("QUEUED"))
	// After the swap, selected DB 0 is the one holding "b", whose value isn't
	// an integer.
	mustDo(t, c, "INCR", "b", proto.Inline("QUEUED"))
	mustDo(t, c, "EXEC", proto.Array(proto.Inline("OK"), proto.Error(msgInvalidInt)))

	// The SWAPDB happened; the failed INCR wrote nothing.
	db0, db1 := s.DB(0), s.DB(1)
	equals(t, []string{"b"}, db0.Keys())
	equals(t, []string{"a"}, db1.Keys())
	v, err := db0.Get("b")
	ok(t, err)
	equals(t, "db1", v)
	v, err = db1.Get("a")
	ok(t, err)
	equals(t, "db0", v)
}

// The direct interface and the clock interface keep working with transactions
// around them.
func TestTxDirectAndClockCompatibility(t *testing.T) {
	s, c := runWithClient(t)
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	s.SetTime(start)
	s.ClockTTL(true)

	mustOK(t, c, "SET", "foo", "bar", "EX", "100")
	s.FastForward(40 * time.Second)
	equals(t, 60*time.Second, s.TTL("foo"))

	mustOK(t, c, "MULTI")
	mustDo(t, c, "GET", "foo", proto.Inline("QUEUED"))
	mustDo(t, c, "EXEC", proto.Array(proto.String("bar")))

	s.FastForward(61 * time.Second)
	equals(t, false, s.Exists("foo"))
}
