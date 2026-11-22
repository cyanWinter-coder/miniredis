package miniredis

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"strings"
	"sync"
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
		m.signal.Broadcast()
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
	m.signal.Broadcast()
	m.Unlock()
}

// txNowLocked returns the logical time the current transaction runs at. During
// an EXEC every command sees the time EXEC started at, so a key's visibility
// can't change halfway through a transaction. Outside a transaction it is the
// current time.
func (m *Miniredis) txNowLocked() time.Time {
	if !m.txNow.IsZero() {
		return m.txNow
	}
	return m.effectiveNow()
}

// executeTx runs the queued commands of an EXEC. Every command gets a
// savepoint: it works on the real databases, but the state before it ran is
// kept, and a command which ends with an error is undone in full -- its writes,
// TTL changes and key versions -- so only its error reply remains. A failed
// command therefore doesn't leave a partial write behind, and doesn't advance
// the version which WATCH observes.
//
// All commands run at one fixed logical time (see txNowLocked), so a key can't
// expire halfway through, and the key versions which WATCH compared against are
// the same versions the committed result gets.
func (m *Miniredis) executeTx(c *server.Peer, ctx *connCtx) {
	// Apply expiry which is due before the transaction starts, then freeze the
	// logical time for the rest of it.
	m.tickLocked()
	m.txNow = m.effectiveNow()
	defer func() { m.txNow = time.Time{} }()

	c.WriteLen(len(ctx.transaction))
	for _, cmd := range ctx.transaction {
		snap := snapshotDBs(m.dbs)
		c.ResetError()
		cmd(c, ctx)
		// A command which wrote an error has failed, and has to be undone
		// (including the key versions it bumped).
		if c.Failed() {
			m.restoreSnapshot(snap)
		}
	}
}

// dbSnapshotState holds the state of all databases at some instant during EXEC.
// The entries are keyed by the database itself, not by its current id, so a
// SWAPDB inside the transaction doesn't make the restore put the contents in
// the wrong place.
type dbSnapshotState struct {
	entries []*dbSnapshotEntry
	ids     map[*RedisDB]int
}

type dbSnapshotEntry struct {
	db    *RedisDB
	clone *RedisDB
}

// dbSnapshot records the current state of all databases, and which id each of
// them is stored under.
func snapshotDBs(dbs map[int]*RedisDB) *dbSnapshotState {
	snap := &dbSnapshotState{
		entries: make([]*dbSnapshotEntry, 0, len(dbs)),
		ids:     make(map[*RedisDB]int, len(dbs)),
	}
	for id, db := range dbs {
		snap.entries = append(snap.entries, &dbSnapshotEntry{db: db, clone: db.clone()})
		snap.ids[db] = id
	}
	return snap
}

// restoreSnapshot puts every database back the way it was, including its place
// in the id map (undoing a SWAPDB) and its values, TTLs and key versions. The
// databases are restored in place, so pointers to them stay valid, and any
// database created since the snapshot is dropped.
func (m *Miniredis) restoreSnapshot(snap *dbSnapshotState) {
	dbs := make(map[int]*RedisDB, len(snap.entries))
	for _, e := range snap.entries {
		id := snap.ids[e.db]
		dbs[id] = e.db
		e.db.id = id
		e.db.restoreFrom(e.clone)
	}
	m.dbs = dbs
}

// restoreFrom makes db the same as other, in place.
func (db *RedisDB) restoreFrom(other *RedisDB) {
	db.keys = other.keys
	db.stringKeys = other.stringKeys
	db.hashKeys = other.hashKeys
	db.listKeys = other.listKeys
	db.setKeys = other.setKeys
	db.hllKeys = other.hllKeys
	db.sortedsetKeys = other.sortedsetKeys
	db.streamKeys = other.streamKeys
	db.ttl = other.ttl
	db.hashTTLs = other.hashTTLs
	db.lru = other.lru
	db.keyVersion = other.keyVersion
	// A stream's groups hold a backreference to the stream.
	for _, s := range db.streamKeys {
		for _, g := range s.groups {
			g.stream = s
		}
	}
}

// clone gives a deep copy of a database.
func (db *RedisDB) clone() *RedisDB {
	cpy := &RedisDB{
		master:        db.master,
		id:            db.id,
		keys:          make(map[string]string, len(db.keys)),
		stringKeys:    make(map[string]string, len(db.stringKeys)),
		hashKeys:      make(map[string]hashKey, len(db.hashKeys)),
		listKeys:      make(map[string]listKey, len(db.listKeys)),
		setKeys:       make(map[string]setKey, len(db.setKeys)),
		hllKeys:       make(map[string]*hll, len(db.hllKeys)),
		sortedsetKeys: make(map[string]sortedSet, len(db.sortedsetKeys)),
		streamKeys:    make(map[string]*streamKey, len(db.streamKeys)),
		ttl:           make(map[string]time.Duration, len(db.ttl)),
		hashTTLs:      make(map[string]map[string]time.Duration, len(db.hashTTLs)),
		lru:           make(map[string]time.Time, len(db.lru)),
		keyVersion:    make(map[string]uint, len(db.keyVersion)),
	}
	for k, v := range db.keys {
		cpy.keys[k] = v
	}
	for k, v := range db.stringKeys {
		cpy.stringKeys[k] = v
	}
	for k, v := range db.hashKeys {
		cpy.hashKeys[k] = copyHashKey(v)
	}
	for k, v := range db.listKeys {
		cpy.listKeys[k] = copyListKey(v)
	}
	for k, v := range db.setKeys {
		cpy.setKeys[k] = copySetKey(v)
	}
	for k, v := range db.hllKeys {
		cpy.hllKeys[k] = v.copy()
	}
	for k, v := range db.sortedsetKeys {
		cpy.sortedsetKeys[k] = copySortedSet(v)
	}
	for k, v := range db.streamKeys {
		cpy.streamKeys[k] = v.copy()
	}
	for k, v := range db.ttl {
		cpy.ttl[k] = v
	}
	for k, v := range db.hashTTLs {
		fields := make(map[string]time.Duration, len(v))
		for f, ttl := range v {
			fields[f] = ttl
		}
		cpy.hashTTLs[k] = fields
	}
	for k, v := range db.lru {
		cpy.lru[k] = v
	}
	for k, v := range db.keyVersion {
		cpy.keyVersion[k] = v
	}
	return cpy
}

// blockCmd is executed returns whether it is done
type blockCmd func(*server.Peer, *connCtx) bool

// blocking keeps trying a command until the callback returns true. Calls
// onTimeout after the timeout (or when we call this in a transaction).
func blocking(
	m *Miniredis,
	c *server.Peer,
	timeout time.Duration,
	cb blockCmd,
	onTimeout func(*server.Peer),
) {
	var (
		ctx = getCtx(c)
	)
	if inTx(ctx) {
		addTxCmd(ctx, func(c *server.Peer, ctx *connCtx) {
			if !cb(c, ctx) {
				onTimeout(c)
			}
		})
		c.WriteInline("QUEUED")
		return
	}

	localCtx, cancel := context.WithCancel(m.Ctx)
	defer cancel()
	timedOut := false
	if timeout != 0 {
		go setCondTimer(localCtx, m.signal, &timedOut, timeout)
	}
	go func() {
		<-localCtx.Done()
		m.signal.Broadcast() // main loop might miss this signal
	}()

	if !ctx.nested {
		// this is a call via Lua's .call(). It's already locked.
		m.Lock()
		defer m.Unlock()
	}
	for {
		if c.Closed() {
			return
		}

		if m.Ctx.Err() != nil {
			return
		}

		done := cb(c, ctx)
		if done {
			return
		}

		if timedOut {
			onTimeout(c)
			return
		}

		m.signal.Wait()
	}
}

func setCondTimer(ctx context.Context, sig *sync.Cond, timedOut *bool, timeout time.Duration) {
	dl := time.NewTimer(timeout)
	defer dl.Stop()
	select {
	case <-dl.C:
		sig.L.Lock() // for timedOut
		*timedOut = true
		sig.Broadcast() // main loop might miss this signal
		sig.L.Unlock()
	case <-ctx.Done():
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
