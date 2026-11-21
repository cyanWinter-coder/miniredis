package miniredis

// Tests for the cancellation and ownership semantics of blocking commands
// (BLPOP &c., XREAD BLOCK): every wait ends in exactly one terminal state
// (delivered, timeout, cancelled, shutdown), pushes match waiters in wait
// order, and a disconnect cleans up the wait.

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2/proto"
)

// waitFor polls cond until it holds, with a second to spare.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met: %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func (m *Miniredis) waiterCount() int {
	m.Lock()
	defer m.Unlock()
	return len(m.waiters)
}

// A timeout ends the wait with the usual null reply, and the wait is gone.
func TestBlockingTimeout(t *testing.T) {
	s, c := runWithClient(t)

	mustNilList(t, c, "BLPOP", "key", "0.05")
	equals(t, 0, s.waiterCount())

	// the connection is still usable afterwards
	mustDo(t, c, "PING", proto.Inline("PONG"))
}

// Pushes are delivered to waiters in the order they started waiting.
func TestBlockingWaitOrder(t *testing.T) {
	s := RunT(t)

	first := goStrings(t, s, "BLPOP", "key", "0")
	waitFor(t, "first waiter", func() bool { return s.waiterCount() == 1 })
	second := goStrings(t, s, "BLPOP", "key", "0")
	waitFor(t, "second waiter", func() bool { return s.waiterCount() == 2 })

	s.Push("key", "one")
	select {
	case have := <-first:
		equals(t, proto.Strings("key", "one"), have)
	case <-time.After(time.Second):
		t.Fatal("first waiter got nothing")
	}

	s.Push("key", "two")
	select {
	case have := <-second:
		equals(t, proto.Strings("key", "two"), have)
	case <-time.After(time.Second):
		t.Fatal("second waiter got nothing")
	}
	equals(t, 0, s.waiterCount())
}

// A wait ends when the client goes away; its slot is removed and a later push
// is delivered to the next live waiter, not to the dead connection.
func TestBlockingClientCancel(t *testing.T) {
	s := RunT(t)

	dead, err := proto.Dial(s.Addr())
	ok(t, err)
	errch := make(chan error, 1)
	go func() {
		_, err := dead.Do("BLPOP", "key", "0")
		errch <- err
	}()
	waitFor(t, "waiter registered", func() bool { return s.waiterCount() == 1 })

	// client goes away: the wait is cancelled and removed from the queue
	ok(t, dead.Close())
	if err := <-errch; err == nil {
		t.Fatal("expected an error on the closed connection")
	}
	waitFor(t, "waiter removed", func() bool { return s.waiterCount() == 0 })

	// the push is not swallowed by the cancelled waiter's slot
	s.Push("key", "value")
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()
	mustDo(t, c,
		"BLPOP", "key", "1",
		proto.Strings("key", "value"),
	)
}

// Closing the server ends all pending waits, without a reply and without
// delivering anything twice.
func TestBlockingShutdown(t *testing.T) {
	s := RunT(t)

	c, err := proto.Dial(s.Addr())
	ok(t, err)
	errch := make(chan error, 1)
	go func() {
		_, err := c.Do("BLPOP", "key", "0")
		errch <- err
	}()
	waitFor(t, "waiter registered", func() bool { return s.waiterCount() == 1 })

	s.Close()
	if err := <-errch; err == nil {
		t.Fatal("expected an error on the closed connection")
	}
	equals(t, 0, s.waiterCount())
}

// A delivered result stays delivered when the server closes; it is not
// delivered again.
func TestBlockingDeliveredBeforeShutdown(t *testing.T) {
	s := RunT(t)

	got := goStrings(t, s, "BLPOP", "key", "0")
	waitFor(t, "waiter registered", func() bool { return s.waiterCount() == 1 })
	s.Push("key", "value")
	select {
	case have := <-got:
		equals(t, proto.Strings("key", "value"), have)
	case <-time.After(time.Second):
		t.Fatal("waiter got nothing")
	}

	// Close with the result already delivered: no duplicate, no hang.
	s.Close()
	equals(t, 0, s.waiterCount())
}

// A closed server rejects new waits; a restarted server accepts them again.
func TestBlockingRestart(t *testing.T) {
	s := RunT(t)
	s.Close()
	ok(t, s.Restart())

	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// times out normally, with the usual null reply
	mustNilList(t, c, "BLPOP", "key", "0.05")

	// and a push is delivered
	got := goStrings(t, s, "BLPOP", "key", "0")
	waitFor(t, "waiter registered", func() bool { return s.waiterCount() == 1 })
	s.Push("key", "value")
	select {
	case have := <-got:
		equals(t, proto.Strings("key", "value"), have)
	case <-time.After(time.Second):
		t.Fatal("waiter got nothing")
	}
}

// A BLMOVE pushes to its destination key itself; a waiter on that key is
// unblocked right away, in wait order.
func TestBlockingBlmoveWakesWaiter(t *testing.T) {
	s, c := runWithClient(t)

	s.Push("src", "value")
	got := goStrings(t, s, "BLPOP", "dst", "0")
	waitFor(t, "waiter registered", func() bool { return s.waiterCount() == 1 })

	mustDo(t, c,
		"BLMOVE", "src", "dst", "LEFT", "RIGHT", "0",
		proto.String("value"),
	)
	select {
	case have := <-got:
		equals(t, proto.Strings("dst", "value"), have)
	case <-time.After(time.Second):
		t.Fatal("BLPOP on the BLMOVE destination got nothing")
	}
	equals(t, 0, s.waiterCount())
}

// A disconnect cleans up both the blocking wait and the pubsub
// subscriptions of the connection.
func TestDisconnectCleansUp(t *testing.T) {
	s := RunT(t)

	c, err := proto.Dial(s.Addr())
	ok(t, err)
	mustDo(t, c,
		"SUBSCRIBE", "news",
		proto.Array(
			proto.String("subscribe"),
			proto.String("news"),
			proto.Int(1),
		),
	)
	ok(t, c.Close())
	waitFor(t, "subscriber removed", func() bool {
		s.Lock()
		defer s.Unlock()
		return len(s.subscribers) == 0
	})

	waiter, err := proto.Dial(s.Addr())
	ok(t, err)
	go waiter.Do("BLPOP", "key", "0")
	waitFor(t, "waiter registered", func() bool { return s.waiterCount() == 1 })
	ok(t, waiter.Close())
	waitFor(t, "waiter removed", func() bool { return s.waiterCount() == 0 })
}
