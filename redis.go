package miniredis

import (
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/alicebob/miniredis/v2/server"
)

const (
	keyTypeString    = "string"
	keyTypeHash      = "hash"
	keyTypeList      = "list"
	keyTypeSet       = "set"
	keyTypeHll       = "hll"
	keyTypeSortedSet = "zset"
	keyTypeStream    = "stream"
)

const (
	msgWrongType            = "WRONGTYPE Operation against a key holding the wrong kind of value"
	msgNotValidHllValue     = "WRONGTYPE Key is not a valid HyperLogLog string value."
	msgInvalidInt           = "ERR value is not an integer or out of range"
	msgIntOverflow          = "ERR increment or decrement would overflow"
	msgStringTooLong        = "ERR string exceeds maximum allowed size (proto-max-bulk-len)"
	msgInvalidFloat         = "ERR value is not a valid float"
	msgInvalidMinMax        = "ERR min or max is not a float"
	msgInvalidRangeItem     = "ERR min or max not valid string range item"
	msgInvalidTimeout       = "ERR timeout is not a float or out of range"
	msgInvalidRange         = "ERR value is out of range, must be positive"
	msgSyntaxError          = "ERR syntax error"
	msgKeyNotFound          = "ERR no such key"
	msgOutOfRange           = "ERR index out of range"
	msgInvalidCursor        = "ERR invalid cursor"
	msgXXandNX              = "ERR XX and NX options at the same time are not compatible"
	msgTimeoutNegative      = "ERR timeout is negative"
	msgTimeoutIsOutOfRange  = "ERR timeout is out of range"
	msgInvalidSETime        = "ERR invalid expire time in set"
	msgInvalidSETEXTime     = "ERR invalid expire time in setex"
	msgInvalidPSETEXTime    = "ERR invalid expire time in psetex"
	msgInvalidKeysNumber    = "ERR Number of keys can't be greater than number of args"
	msgNegativeKeysNumber   = "ERR Number of keys can't be negative"
	msgFScriptUsage         = "ERR unknown subcommand or wrong number of arguments for '%s'. Try SCRIPT HELP."
	msgFScriptUsageSimple   = "ERR unknown subcommand '%s'. Try SCRIPT HELP."
	msgFPubsubUsage         = "ERR unknown subcommand or wrong number of arguments for '%s'. Try PUBSUB HELP."
	msgFPubsubUsageSimple   = "ERR unknown subcommand '%s'. Try PUBSUB HELP."
	msgFObjectUsage         = "ERR unknown subcommand '%s'. Try OBJECT HELP."
	msgScriptFlush          = "ERR SCRIPT FLUSH only support SYNC|ASYNC option"
	msgSingleElementPair    = "ERR INCR option supports a single increment-element pair"
	msgGTLTandNX            = "ERR GT, LT, and/or NX options at the same time are not compatible"
	msgInvalidStreamID      = "ERR Invalid stream ID specified as stream command argument"
	msgStreamIDTooSmall     = "ERR The ID specified in XADD is equal or smaller than the target stream top item"
	msgStreamIDZero         = "ERR The ID specified in XADD must be greater than 0-0"
	msgNoScriptFound        = "NOSCRIPT No matching script. Please use EVAL."
	msgUnsupportedUnit      = "ERR unsupported unit provided. please use M, KM, FT, MI"
	msgXreadUnbalanced      = "ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified."
	msgXgroupKeyNotFound    = "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically."
	msgXtrimInvalidStrategy = "ERR unsupported XTRIM strategy. Please use MAXLEN, MINID"
	msgXtrimInvalidMaxLen   = "ERR value is not an integer or out of range"
	msgXtrimInvalidLimit    = "ERR syntax error, LIMIT cannot be used without the special ~ option"
	msgDBIndexOutOfRange    = "ERR DB index is out of range"
	msgLimitCombination     = "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"
	msgRankIsZero           = "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list"
	msgCountIsNegative      = "ERR COUNT can't be negative"
	msgMaxLengthIsNegative  = "ERR MAXLEN can't be negative"
	msgLimitIsNegative      = "ERR LIMIT can't be negative"
	msgMemorySubcommand     = "ERR unknown subcommand '%s'. Try MEMORY HELP."
	msgNumFieldsParameter   = "ERR The `numfields` parameter must match the number of arguments"
	msgNumFieldsInvalid     = "ERR Parameter `numFields` should be greater than 0"
	msgNumFieldsPositive    = "ERR Number of fields must be a positive integer"
	msgMandatoryArgument    = "ERR Mandatory argument %s is missing or not at the right position"
	msgGTandLT              = "ERR GT and LT options at the same time are not compatible"
	msgNXandXXGTLT          = "ERR NX and XX, GT or LT options at the same time are not compatible"
)

func errWrongNumber(cmd string) string {
	return fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmd))
}

func errLuaParseError(err error) string {
	return fmt.Sprintf("ERR Error compiling script (new function): %s", err.Error())
}

func errReadgroup(key, group string) error {
	return fmt.Errorf("NOGROUP No such key '%s' or consumer group '%s'", key, group)
}

func errXreadgroup(key, group string) error {
	return fmt.Errorf("NOGROUP No such key '%s' or consumer group '%s' in XREADGROUP with GROUP option", key, group)
}

func msgNotFromScripts(sha string) string {
	return fmt.Sprintf("This Redis command is not allowed from script script: %s, &c", sha)
}

// withTx wraps the non-argument-checking part of command handling code in
// transaction logic.
func withTx(
	m *Miniredis,
	c *server.Peer,
	cb txCmd,
) {
	ctx := getCtx(c)

	if ctx.nested {
		// this is a call via Lua's .call(). It's already locked.
		cb(c, ctx)
		m.wakeWaitersLocked()
		return
	}

	if inTx(ctx) {
		addTxCmd(ctx, cb)
		c.WriteInline("QUEUED")
		return
	}
	m.Lock()
	cb(c, ctx)
	// done, wake up anyone who waits on anything.
	m.wakeWaitersLocked()
	m.Unlock()
}

// blockCmd is executed returns whether it is done
type blockCmd func(*server.Peer, *connCtx) bool

// waitState is the state of a blocking wait. Every wait ends in exactly one
// of the terminal states.
type waitState int

const (
	waitPending   waitState = iota // still waiting
	waitDelivered                  // a push delivered a result
	waitTimeout                    // the wait timed out
	waitCancelled                  // the client connection went away
	waitShutdown                   // the server was closed
)

// blockWaiter is a single client blocked in BLPOP, XREAD BLOCK, and friends.
type blockWaiter struct {
	peer  *server.Peer
	ctx   *connCtx
	cb    blockCmd
	state waitState     // guarded by the Miniredis lock
	wake  chan struct{} // closed once state leaves waitPending
}

// resolveWaiterLocked moves a waiter to a terminal state and drops it from
// the queue. Only the first call for a waiter has an effect: a waiter is
// unblocked exactly once.
func (m *Miniredis) resolveWaiterLocked(w *blockWaiter, st waitState) {
	if w.state != waitPending {
		return
	}
	w.state = st
	for i, x := range m.waiters {
		if x == w {
			m.waiters = append(m.waiters[:i], m.waiters[i+1:]...)
			break
		}
	}
	close(w.wake)
}

// wakeWaitersLocked gives every pending waiter a chance to consume, in the
// order they started waiting. Waiters whose connection went away are dropped
// from the queue, so a push is never delivered to a gone client.
func (m *Miniredis) wakeWaitersLocked() {
	// A delivered waiter can push to a key itself (BLMOVE &c.), which can
	// unblock a waiter earlier in the queue. Repeat until no progress.
	for len(m.waiters) > 0 {
		// resolving mutates m.waiters, so iterate over a copy.
		queue := append([]*blockWaiter(nil), m.waiters...)
		progress := false
		for _, w := range queue {
			if w.state != waitPending {
				continue
			}
			if w.peer.Closed() {
				m.resolveWaiterLocked(w, waitCancelled)
				continue
			}
			if w.cb(w.peer, w.ctx) {
				m.resolveWaiterLocked(w, waitDelivered)
				progress = true
			}
		}
		if !progress {
			return
		}
	}
}

// blocking keeps trying a command until the callback returns true. Calls
// onTimeout after the timeout (or when we call this in a transaction).
func blocking(
	m *Miniredis,
	c *server.Peer,
	timeout time.Duration,
	cb blockCmd,
	onTimeout func(*server.Peer),
) {
	ctx := getCtx(c)
	if inTx(ctx) {
		addTxCmd(ctx, func(c *server.Peer, ctx *connCtx) {
			if !cb(c, ctx) {
				onTimeout(c)
			}
		})
		c.WriteInline("QUEUED")
		return
	}

	if ctx.nested {
		// this is a call via Lua's .call(). It's already locked, and never
		// blocks.
		if !cb(c, ctx) {
			onTimeout(c)
		}
		return
	}

	m.Lock()
	if c.Closed() || m.Ctx.Err() != nil {
		// the connection is gone, or the server is closed: no new waits
		m.Unlock()
		return
	}
	if cb(c, ctx) {
		// didn't block. BLMOVE &c. push to a key themselves, which can
		// unblock a waiter.
		m.wakeWaitersLocked()
		m.Unlock()
		return
	}
	w := &blockWaiter{
		peer: c,
		ctx:  ctx,
		cb:   cb,
		wake: make(chan struct{}),
	}
	m.waiters = append(m.waiters, w)
	m.Unlock()

	var timer *time.Timer
	if timeout != 0 {
		timer = time.AfterFunc(timeout, func() {
			m.Lock()
			m.resolveWaiterLocked(w, waitTimeout)
			m.Unlock()
		})
		defer timer.Stop()
	}
	// cancel the wait when the client goes away or the server closes.
	go func() {
		select {
		case <-c.Done():
			m.Lock()
			m.resolveWaiterLocked(w, waitCancelled)
			m.Unlock()
		case <-m.Ctx.Done():
			m.Lock()
			m.resolveWaiterLocked(w, waitShutdown)
			m.Unlock()
		case <-w.wake:
		}
	}()

	<-w.wake

	m.Lock()
	st := w.state
	m.Unlock()
	if st == waitTimeout {
		onTimeout(c)
	}
}

// formatBig formats a float the way redis does
func formatBig(v *big.Float) string {
	// Format with %f and strip trailing 0s.
	if v.IsInf() {
		return "inf"
	}
	// if math.IsInf(v, -1) {
	// return "-inf"
	// }
	return stripZeros(fmt.Sprintf("%.17f", v))
}

func stripZeros(sv string) string {
	for strings.Contains(sv, ".") {
		if sv[len(sv)-1] != '0' {
			break
		}
		// Remove trailing 0s.
		sv = sv[:len(sv)-1]
		// Ends with a '.'.
		if sv[len(sv)-1] == '.' {
			sv = sv[:len(sv)-1]
			break
		}
	}
	return sv
}

// redisRange gives Go offsets for something l long with start/end in
// Redis semantics. Both start and end can be negative.
// Used for string range and list range things.
// The results can be used as: v[start:end]
// Note that GETRANGE (on a string key) never returns an empty string when end
// is a large negative number.
func redisRange(l, start, end int, stringSymantics bool) (int, int) {
	if start < 0 {
		start = l + start
		if start < 0 {
			start = 0
		}
	}
	if start > l {
		start = l
	}

	if end < 0 {
		end = l + end
		if end < 0 {
			end = -1
			if stringSymantics {
				end = 0
			}
		}
	}
	if end < math.MaxInt32 {
		end++ // end argument is inclusive in Redis.
	}
	if end > l {
		end = l
	}

	if end < start {
		return 0, 0
	}
	return start, end
}
