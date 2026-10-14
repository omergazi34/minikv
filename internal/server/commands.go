package server

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/omergazi34/minikv/internal/resp"
	"github.com/omergazi34/minikv/internal/store"
)

// cmdCtx carries one command through its handler.
type cmdCtx struct {
	s      *Server
	args   [][]byte
	w      *resp.Writer
	replay bool // applying the AOF at startup: no logging, replies are discarded
	quit   bool
	connID int64
}

type command struct {
	arity int // Redis convention: N means exactly N args (including the name), -N at least N
	write bool
	fn    func(*cmdCtx)
}

var commands map[string]command

func init() {
	commands = map[string]command{
		// connection & server
		"ping":         {-1, false, cmdPing},
		"echo":         {2, false, func(c *cmdCtx) { c.w.Bulk(c.args[1]) }},
		"quit":         {1, false, func(c *cmdCtx) { c.w.Simple("OK"); c.quit = true }},
		"select":       {2, false, cmdSelect},
		"command":      {-1, false, func(c *cmdCtx) { c.w.ArrayHeader(0) }},
		"client":       {-2, false, cmdClient},
		"config":       {-2, false, cmdConfig},
		"info":         {-1, false, cmdInfo},
		"dbsize":       {1, false, cmdDBSize},
		"flushdb":      {-1, true, cmdFlush},
		"flushall":     {-1, true, cmdFlush},
		"bgrewriteaof": {1, false, cmdBgRewrite},
		// keys
		"del":       {-2, true, cmdDel},
		"exists":    {-2, false, cmdExists},
		"type":      {2, false, cmdType},
		"keys":      {2, false, cmdKeys},
		"expire":    {3, true, expireCmd(1000, false)},
		"pexpire":   {3, true, expireCmd(1, false)},
		"expireat":  {3, true, expireCmd(1000, true)},
		"pexpireat": {3, true, expireCmd(1, true)},
		"ttl":       {2, false, ttlCmd(1000)},
		"pttl":      {2, false, ttlCmd(1)},
		"persist":   {2, true, cmdPersist},
		// strings
		"get":    {2, false, cmdGet},
		"set":    {-3, true, cmdSet},
		"setnx":  {3, true, cmdSetNX},
		"setex":  {4, true, cmdSetEX},
		"mget":   {-2, false, cmdMGet},
		"mset":   {-3, true, cmdMSet},
		"incr":   {2, true, incrCmd(1, false)},
		"decr":   {2, true, incrCmd(-1, false)},
		"incrby": {3, true, incrCmd(1, true)},
		"decrby": {3, true, incrCmd(-1, true)},
		"append": {3, true, cmdAppend},
		"strlen": {2, false, cmdStrlen},
		// lists
		"lpush":  {-3, true, pushCmd(true)},
		"rpush":  {-3, true, pushCmd(false)},
		"lpop":   {-2, true, popCmd(true)},
		"rpop":   {-2, true, popCmd(false)},
		"llen":   {2, false, cmdLLen},
		"lrange": {4, false, cmdLRange},
		"lindex": {3, false, cmdLIndex},
		// hashes
		"hset":    {-4, true, cmdHSet},
		"hget":    {3, false, cmdHGet},
		"hdel":    {-3, true, cmdHDel},
		"hgetall": {2, false, hashListCmd(true, true)},
		"hkeys":   {2, false, hashListCmd(true, false)},
		"hvals":   {2, false, hashListCmd(false, true)},
		"hlen":    {2, false, cmdHLen},
		"hexists": {3, false, cmdHExists},
		"hincrby": {4, true, cmdHIncrBy},
	}
}

// ---------------------------------------------------------------------------- helpers

func (c *cmdCtx) key(i int) string { return string(c.args[i]) }

func (c *cmdCtx) keys(from, step int) []string {
	out := make([]string, 0, (len(c.args)-from+step-1)/step)
	for i := from; i < len(c.args); i += step {
		out = append(out, string(c.args[i]))
	}
	return out
}

// log appends a write to the AOF. It is called inside Store.Update, while the keys' shard
// locks are held, so the file order matches the order writes were applied.
func (c *cmdCtx) log(args ...[]byte) {
	if c.replay || c.s.aof == nil {
		return
	}
	if err := c.s.aof.Append(args...); err != nil {
		c.s.logger.Error("AOF append failed; refusing further writes", "err", err)
	}
}

func (c *cmdCtx) logOriginal() { c.log(c.args...) }

func (c *cmdCtx) err(err error) {
	msg := err.Error()
	if !strings.HasPrefix(msg, "ERR ") && !strings.HasPrefix(msg, "WRONGTYPE ") {
		msg = "ERR " + msg
	}
	c.w.Error(msg)
}

func (c *cmdCtx) syntaxErr() { c.w.Error("ERR syntax error") }

func itob(n int64) []byte { return strconv.AppendInt(nil, n, 10) }

func parseInt(b []byte) (int64, error) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, store.ErrNotInteger
	}
	return n, nil
}

func checkKind(e *store.Entry, kind store.Kind) error {
	if e != nil && e.Kind != kind {
		return store.ErrWrongType
	}
	return nil
}

// ---------------------------------------------------------------------------- connection & server

func cmdPing(c *cmdCtx) {
	switch len(c.args) {
	case 1:
		c.w.Simple("PONG")
	case 2:
		c.w.Bulk(c.args[1])
	default:
		c.w.Error("ERR wrong number of arguments for 'ping' command")
	}
}

func cmdSelect(c *cmdCtx) {
	if string(c.args[1]) != "0" {
		c.w.Error("ERR DB index is out of range")
		return
	}
	c.w.Simple("OK")
}

func cmdClient(c *cmdCtx) {
	switch strings.ToLower(string(c.args[1])) {
	case "setname", "setinfo":
		c.w.Simple("OK")
	case "getname":
		c.w.Null()
	case "id":
		c.w.Int(c.connID)
	default:
		c.w.Error(fmt.Sprintf("ERR unknown subcommand '%s'", c.args[1]))
	}
}

// cmdConfig supports the CONFIG GET calls that redis-cli and redis-benchmark make on startup.
func cmdConfig(c *cmdCtx) {
	if strings.ToLower(string(c.args[1])) != "get" || len(c.args) != 3 {
		c.w.Error("ERR only CONFIG GET <pattern> is supported")
		return
	}
	params := [][2]string{
		{"appendonly", map[bool]string{true: "yes", false: "no"}[c.s.aof != nil]},
		{"appendfsync", c.s.cfg.Fsync.String()},
		{"save", ""},
		{"databases", "1"},
	}
	var out [][]byte
	for _, p := range params {
		if store.Match(strings.ToLower(string(c.args[2])), p[0]) {
			out = append(out, []byte(p[0]), []byte(p[1]))
		}
	}
	c.w.BulkArray(out)
}

func cmdDBSize(c *cmdCtx) {
	n, _ := c.s.store.Len()
	c.w.Int(int64(n))
}

func cmdFlush(c *cmdCtx) {
	if len(c.args) > 2 {
		c.syntaxErr()
		return
	}
	c.s.store.Flush(func() { c.log([]byte("FLUSHDB")) })
	c.w.Simple("OK")
}

func cmdBgRewrite(c *cmdCtx) {
	if c.s.aof == nil {
		c.w.Error("ERR AOF is not enabled")
		return
	}
	if c.s.rewriting.Load() {
		c.w.Error("ERR Background append only file rewriting already in progress")
		return
	}
	c.s.bg.Add(1)
	go func() {
		defer c.s.bg.Done()
		if err := c.s.rewriteAOF(); err != nil && !errors.Is(err, errRewriteInProgress) {
			c.s.logger.Error("AOF rewrite failed", "err", err)
		}
	}()
	c.w.Simple("Background append only file rewriting started")
}

// ---------------------------------------------------------------------------- keys

func cmdDel(c *cmdCtx) {
	keys := c.keys(1, 1)
	var n int64
	_ = c.s.store.Update(keys, func(tx *store.Tx) error {
		for _, k := range keys {
			if tx.Delete(k) {
				n++
			}
		}
		if n > 0 {
			c.logOriginal()
		}
		return nil
	})
	c.w.Int(n)
}

func cmdExists(c *cmdCtx) {
	keys := c.keys(1, 1)
	var n int64
	_ = c.s.store.View(keys, func(tx *store.Tx) error {
		for _, k := range keys {
			if tx.Get(k) != nil {
				n++
			}
		}
		return nil
	})
	c.w.Int(n)
}

func cmdType(c *cmdCtx) {
	_ = c.s.store.View([]string{c.key(1)}, func(tx *store.Tx) error {
		if e := tx.Get(c.key(1)); e != nil {
			c.w.Simple(e.Kind.String())
		} else {
			c.w.Simple("none")
		}
		return nil
	})
}

func cmdKeys(c *cmdCtx) {
	keys := c.s.store.Keys(string(c.args[1]))
	c.w.ArrayHeader(len(keys))
	for _, k := range keys {
		c.w.BulkString(k)
	}
}

// expireCmd builds EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT. Every variant is logged to the AOF as
// PEXPIREAT with an absolute time, so replaying the log later yields the same expiry instant
// instead of restarting the countdown.
func expireCmd(unitMs int64, absolute bool) func(*cmdCtx) {
	return func(c *cmdCtx) {
		n, err := parseInt(c.args[2])
		if err != nil {
			c.err(err)
			return
		}
		if n > math.MaxInt64/unitMs || n < math.MinInt64/unitMs {
			c.w.Error(fmt.Sprintf("ERR invalid expire time in '%s' command", strings.ToLower(string(c.args[0]))))
			return
		}
		key := c.key(1)
		_ = c.s.store.Update([]string{key}, func(tx *store.Tx) error {
			e := tx.Get(key)
			if e == nil {
				c.w.Int(0)
				return nil
			}
			at := n * unitMs
			if !absolute {
				if at > math.MaxInt64-tx.Now() {
					c.w.Error("ERR invalid expire time")
					return nil
				}
				at += tx.Now()
			}
			if at <= tx.Now() {
				tx.Delete(key)
				c.log([]byte("DEL"), c.args[1])
			} else {
				tx.SetExpire(key, e, at)
				c.log([]byte("PEXPIREAT"), c.args[1], itob(at))
			}
			c.w.Int(1)
			return nil
		})
	}
}

func ttlCmd(unitMs int64) func(*cmdCtx) {
	return func(c *cmdCtx) {
		_ = c.s.store.View([]string{c.key(1)}, func(tx *store.Tx) error {
			e := tx.Get(c.key(1))
			switch {
			case e == nil:
				c.w.Int(-2)
			case e.ExpireAt == 0:
				c.w.Int(-1)
			default:
				remaining := e.ExpireAt - tx.Now()
				c.w.Int((remaining + unitMs/2) / unitMs) // rounded, like Redis
			}
			return nil
		})
	}
}

func cmdPersist(c *cmdCtx) {
	key := c.key(1)
	_ = c.s.store.Update([]string{key}, func(tx *store.Tx) error {
		e := tx.Get(key)
		if e == nil || e.ExpireAt == 0 {
			c.w.Int(0)
			return nil
		}
		tx.SetExpire(key, e, 0)
		c.logOriginal()
		c.w.Int(1)
		return nil
	})
}

// ---------------------------------------------------------------------------- strings

func cmdGet(c *cmdCtx) {
	_ = c.s.store.View([]string{c.key(1)}, func(tx *store.Tx) error {
		e := tx.Lookup(c.key(1))
		switch {
		case e == nil:
			c.w.Null()
		case e.Kind != store.KindString:
			c.err(store.ErrWrongType)
		default:
			c.w.Bulk(e.Str)
		}
		return nil
	})
}

type setOpts struct {
	expireAt   int64 // absolute ms, 0 = none
	relativeMs int64 // EX/PX, resolved inside the transaction
	keepTTL    bool
	nx, xx     bool
	get        bool
}

func parseSetOpts(c *cmdCtx, args [][]byte) (setOpts, bool) {
	var o setOpts
	expiries := 0
	for i := 0; i < len(args); i++ {
		switch opt := strings.ToUpper(string(args[i])); opt {
		case "NX":
			o.nx = true
		case "XX":
			o.xx = true
		case "GET":
			o.get = true
		case "KEEPTTL":
			o.keepTTL = true
			expiries++
		case "EX", "PX", "EXAT", "PXAT":
			if i+1 >= len(args) {
				c.syntaxErr()
				return o, false
			}
			i++
			n, err := parseInt(args[i])
			unit := int64(1)
			if opt == "EX" || opt == "EXAT" {
				unit = 1000
			}
			if err != nil || n <= 0 || n > math.MaxInt64/unit/2 {
				c.w.Error("ERR invalid expire time in 'set' command")
				return o, false
			}
			if opt == "EX" || opt == "PX" {
				o.relativeMs = n * unit
			} else {
				o.expireAt = n * unit
			}
			expiries++
		default:
			c.syntaxErr()
			return o, false
		}
	}
	if (o.nx && o.xx) || expiries > 1 {
		c.syntaxErr()
		return o, false
	}
	return o, true
}

func cmdSet(c *cmdCtx) {
	opts, ok := parseSetOpts(c, c.args[3:])
	if !ok {
		return
	}
	key, val := c.key(1), c.args[2]
	_ = c.s.store.Update([]string{key}, func(tx *store.Tx) error {
		old := tx.Get(key)
		if opts.get {
			if err := checkKind(old, store.KindString); err != nil {
				c.err(err)
				return nil
			}
		}
		reply := func(applied bool) {
			switch {
			case opts.get && old != nil:
				c.w.Bulk(old.Str)
			case opts.get, !applied:
				c.w.Null()
			default:
				c.w.Simple("OK")
			}
		}
		if (opts.nx && old != nil) || (opts.xx && old == nil) {
			reply(false)
			return nil
		}
		at := opts.expireAt
		if opts.relativeMs > 0 {
			at = tx.Now() + opts.relativeMs
		}
		if opts.keepTTL && old != nil {
			at = old.ExpireAt
		}
		tx.Put(key, &store.Entry{Kind: store.KindString, Str: val, ExpireAt: at})
		if at != 0 {
			c.log([]byte("SET"), c.args[1], val, []byte("PXAT"), itob(at))
		} else {
			c.log([]byte("SET"), c.args[1], val)
		}
		reply(true)
		return nil
	})
}

func cmdSetNX(c *cmdCtx) {
	key := c.key(1)
	_ = c.s.store.Update([]string{key}, func(tx *store.Tx) error {
		if tx.Get(key) != nil {
			c.w.Int(0)
			return nil
		}
		tx.Put(key, &store.Entry{Kind: store.KindString, Str: c.args[2]})
		c.log([]byte("SET"), c.args[1], c.args[2])
		c.w.Int(1)
		return nil
	})
}

func cmdSetEX(c *cmdCtx) {
	secs, err := parseInt(c.args[2])
	if err != nil || secs <= 0 || secs > math.MaxInt64/2000 {
		c.w.Error("ERR invalid expire time in 'setex' command")
		return
	}
	key := c.key(1)
	_ = c.s.store.Update([]string{key}, func(tx *store.Tx) error {
		at := tx.Now() + secs*1000
		tx.Put(key, &store.Entry{Kind: store.KindString, Str: c.args[3], ExpireAt: at})
		c.log([]byte("SET"), c.args[1], c.args[3], []byte("PXAT"), itob(at))
		c.w.Simple("OK")
		return nil
	})
}

func cmdMGet(c *cmdCtx) {
	keys := c.keys(1, 1)
	_ = c.s.store.View(keys, func(tx *store.Tx) error {
		c.w.ArrayHeader(len(keys))
		for _, k := range keys {
			if e := tx.Lookup(k); e != nil && e.Kind == store.KindString {
				c.w.Bulk(e.Str)
			} else {
				c.w.Null()
			}
		}
		return nil
	})
}

func cmdMSet(c *cmdCtx) {
	if len(c.args)%2 != 1 {
		c.w.Error("ERR wrong number of arguments for 'mset' command")
		return
	}
	keys := c.keys(1, 2)
	_ = c.s.store.Update(keys, func(tx *store.Tx) error {
		for i := 1; i < len(c.args); i += 2 {
			tx.Put(string(c.args[i]), &store.Entry{Kind: store.KindString, Str: c.args[i+1]})
		}
		c.logOriginal()
		c.w.Simple("OK")
		return nil
	})
}

// incrCmd builds INCR/DECR/INCRBY/DECRBY. The value keeps its TTL, as in Redis.
func incrCmd(sign int64, hasArg bool) func(*cmdCtx) {
	return func(c *cmdCtx) {
		delta := sign
		if hasArg {
			n, err := parseInt(c.args[2])
			if err != nil {
				c.err(err)
				return
			}
			if sign < 0 {
				if n == math.MinInt64 {
					c.err(store.ErrOverflow)
					return
				}
				n = -n
			}
			delta = n
		}
		key := c.key(1)
		_ = c.s.store.Update([]string{key}, func(tx *store.Tx) error {
			e := tx.Get(key)
			if err := checkKind(e, store.KindString); err != nil {
				c.err(err)
				return nil
			}
			var cur, expireAt int64
			if e != nil {
				v, err := parseInt(e.Str)
				if err != nil {
					c.err(err)
					return nil
				}
				cur, expireAt = v, e.ExpireAt
			}
			if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
				c.err(store.ErrOverflow)
				return nil
			}
			next := cur + delta
			tx.Put(key, &store.Entry{Kind: store.KindString, Str: itob(next), ExpireAt: expireAt})
			c.logOriginal()
			c.w.Int(next)
			return nil
		})
	}
}

func cmdAppend(c *cmdCtx) {
	key := c.key(1)
	_ = c.s.store.Update([]string{key}, func(tx *store.Tx) error {
		e := tx.Get(key)
		if err := checkKind(e, store.KindString); err != nil {
			c.err(err)
			return nil
		}
		if e == nil {
			e = &store.Entry{Kind: store.KindString}
			tx.Put(key, e)
		}
		// Appending past len never touches bytes a snapshot may still be reading.
		e.Str = append(e.Str, c.args[2]...)
		c.logOriginal()
		c.w.Int(int64(len(e.Str)))
		return nil
	})
}

func cmdStrlen(c *cmdCtx) {
	_ = c.s.store.View([]string{c.key(1)}, func(tx *store.Tx) error {
		e := tx.Get(c.key(1))
		if err := checkKind(e, store.KindString); err != nil {
			c.err(err)
		} else if e == nil {
			c.w.Int(0)
		} else {
			c.w.Int(int64(len(e.Str)))
		}
		return nil
	})
}

// ---------------------------------------------------------------------------- lists

func pushCmd(left bool) func(*cmdCtx) {
	return func(c *cmdCtx) {
		key := c.key(1)
		_ = c.s.store.Update([]string{key}, func(tx *store.Tx) error {
			e := tx.Get(key)
			if err := checkKind(e, store.KindList); err != nil {
				c.err(err)
				return nil
			}
			if e == nil {
				e = &store.Entry{Kind: store.KindList, List: store.NewList()}
				tx.Put(key, e)
			}
			for _, v := range c.args[2:] { // LPUSH a b c leaves [c b a ...]
				if left {
					e.List.PushFront(v)
				} else {
					e.List.PushBack(v)
				}
			}
			c.logOriginal()
			c.w.Int(int64(e.List.Len()))
			return nil
		})
	}
}

func popCmd(left bool) func(*cmdCtx) {
	return func(c *cmdCtx) {
		if len(c.args) > 3 {
			c.syntaxErr()
			return
		}
		count, withCount := int64(1), len(c.args) == 3
		if withCount {
			n, err := parseInt(c.args[2])
			if err != nil || n < 0 {
				c.w.Error("ERR value is out of range, must be positive")
				return
			}
			count = n
		}
		key := c.key(1)
		_ = c.s.store.Update([]string{key}, func(tx *store.Tx) error {
			e := tx.Get(key)
			if err := checkKind(e, store.KindList); err != nil {
				c.err(err)
				return nil
			}
			if e == nil {
				if withCount {
					c.w.BulkArray(nil)
				} else {
					c.w.Null()
				}
				return nil
			}
			n := int(min(count, int64(e.List.Len())))
			popped := make([][]byte, n)
			for i := range n {
				if left {
					popped[i] = e.List.PopFront()
				} else {
					popped[i] = e.List.PopBack()
				}
			}
			if e.List.Len() == 0 {
				tx.Delete(key)
			}
			if n > 0 {
				c.log(c.args[0], c.args[1], itob(int64(n)))
			}
			if withCount {
				c.w.BulkArray(popped)
			} else {
				c.w.Bulk(popped[0])
			}
			return nil
		})
	}
}

func listOf(c *cmdCtx, tx *store.Tx) (*store.List, bool) {
	e := tx.Get(c.key(1))
	if err := checkKind(e, store.KindList); err != nil {
		c.err(err)
		return nil, false
	}
	if e == nil {
		return store.NewList(), true
	}
	return e.List, true
}

func cmdLLen(c *cmdCtx) {
	_ = c.s.store.View([]string{c.key(1)}, func(tx *store.Tx) error {
		if list, ok := listOf(c, tx); ok {
			c.w.Int(int64(list.Len()))
		}
		return nil
	})
}

// normalizeRange converts Redis' inclusive, possibly negative [start, stop] into a Go slice range.
func normalizeRange(start, stop, n int64) (int64, int64) {
	if start < 0 {
		start = max(n+start, 0)
	}
	if stop < 0 {
		stop = n + stop
	}
	stop = min(stop, n-1)
	if start > stop || start >= n {
		return 0, 0
	}
	return start, stop + 1
}

func cmdLRange(c *cmdCtx) {
	start, err1 := parseInt(c.args[2])
	stop, err2 := parseInt(c.args[3])
	if err := errors.Join(err1, err2); err != nil {
		c.err(store.ErrNotInteger)
		return
	}
	_ = c.s.store.View([]string{c.key(1)}, func(tx *store.Tx) error {
		list, ok := listOf(c, tx)
		if !ok {
			return nil
		}
		lo, hi := normalizeRange(start, stop, int64(list.Len()))
		c.w.BulkArray(list.Range(int(lo), int(hi)))
		return nil
	})
}

func cmdLIndex(c *cmdCtx) {
	idx, err := parseInt(c.args[2])
	if err != nil {
		c.err(err)
		return
	}
	_ = c.s.store.View([]string{c.key(1)}, func(tx *store.Tx) error {
		list, ok := listOf(c, tx)
		if !ok {
			return nil
		}
		if idx < 0 {
			idx += int64(list.Len())
		}
		if idx < 0 || idx >= int64(list.Len()) {
			c.w.Null()
		} else {
			c.w.Bulk(list.At(int(idx)))
		}
		return nil
	})
}

// ---------------------------------------------------------------------------- hashes

func hashOf(c *cmdCtx, tx *store.Tx) (map[string][]byte, bool) {
	e := tx.Get(c.key(1))
	if err := checkKind(e, store.KindHash); err != nil {
		c.err(err)
		return nil, false
	}
	if e == nil {
		return nil, true
	}
	return e.Hash, true
}

func cmdHSet(c *cmdCtx) {
	if len(c.args)%2 != 0 {
		c.w.Error("ERR wrong number of arguments for 'hset' command")
		return
	}
	key := c.key(1)
	_ = c.s.store.Update([]string{key}, func(tx *store.Tx) error {
		e := tx.Get(key)
		if err := checkKind(e, store.KindHash); err != nil {
			c.err(err)
			return nil
		}
		if e == nil {
			e = &store.Entry{Kind: store.KindHash, Hash: make(map[string][]byte)}
			tx.Put(key, e)
		}
		var added int64
		for i := 2; i < len(c.args); i += 2 {
			if _, exists := e.Hash[string(c.args[i])]; !exists {
				added++
			}
			e.Hash[string(c.args[i])] = c.args[i+1]
		}
		c.logOriginal()
		c.w.Int(added)
		return nil
	})
}

func cmdHGet(c *cmdCtx) {
	_ = c.s.store.View([]string{c.key(1)}, func(tx *store.Tx) error {
		h, ok := hashOf(c, tx)
		if !ok {
			return nil
		}
		if v, found := h[string(c.args[2])]; found {
			c.w.Bulk(v)
		} else {
			c.w.Null()
		}
		return nil
	})
}

func cmdHDel(c *cmdCtx) {
	key := c.key(1)
	_ = c.s.store.Update([]string{key}, func(tx *store.Tx) error {
		h, ok := hashOf(c, tx)
		if !ok {
			return nil
		}
		var removed int64
		for _, f := range c.args[2:] {
			if _, found := h[string(f)]; found {
				delete(h, string(f))
				removed++
			}
		}
		if h != nil && len(h) == 0 {
			tx.Delete(key)
		}
		if removed > 0 {
			c.logOriginal()
		}
		c.w.Int(removed)
		return nil
	})
}

func hashListCmd(fields, values bool) func(*cmdCtx) {
	return func(c *cmdCtx) {
		_ = c.s.store.View([]string{c.key(1)}, func(tx *store.Tx) error {
			h, ok := hashOf(c, tx)
			if !ok {
				return nil
			}
			out := make([][]byte, 0, len(h)*2)
			for f, v := range h {
				if fields {
					out = append(out, []byte(f))
				}
				if values {
					out = append(out, v)
				}
			}
			c.w.BulkArray(out)
			return nil
		})
	}
}

func cmdHLen(c *cmdCtx) {
	_ = c.s.store.View([]string{c.key(1)}, func(tx *store.Tx) error {
		if h, ok := hashOf(c, tx); ok {
			c.w.Int(int64(len(h)))
		}
		return nil
	})
}

func cmdHExists(c *cmdCtx) {
	_ = c.s.store.View([]string{c.key(1)}, func(tx *store.Tx) error {
		if h, ok := hashOf(c, tx); ok {
			_, found := h[string(c.args[2])]
			c.w.Int(map[bool]int64{true: 1, false: 0}[found])
		}
		return nil
	})
}

func cmdHIncrBy(c *cmdCtx) {
	delta, err := parseInt(c.args[3])
	if err != nil {
		c.err(err)
		return
	}
	key := c.key(1)
	_ = c.s.store.Update([]string{key}, func(tx *store.Tx) error {
		e := tx.Get(key)
		if err := checkKind(e, store.KindHash); err != nil {
			c.err(err)
			return nil
		}
		if e == nil {
			e = &store.Entry{Kind: store.KindHash, Hash: make(map[string][]byte)}
			tx.Put(key, e)
		}
		var cur int64
		if v, found := e.Hash[string(c.args[2])]; found {
			if cur, err = parseInt(v); err != nil {
				c.w.Error("ERR hash value is not an integer")
				return nil
			}
		}
		if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
			c.err(store.ErrOverflow)
			return nil
		}
		e.Hash[string(c.args[2])] = itob(cur + delta)
		c.logOriginal()
		c.w.Int(cur + delta)
		return nil
	})
}
