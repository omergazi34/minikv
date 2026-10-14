package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omergazi34/minikv/internal/aof"
	"github.com/omergazi34/minikv/internal/resp"
)

type fakeClock struct{ ms atomic.Int64 }

func newClock() *fakeClock {
	c := &fakeClock{}
	c.ms.Store(1_760_000_000_000)
	return c
}

func (c *fakeClock) now() time.Time          { return time.UnixMilli(c.ms.Load()) }
func (c *fakeClock) advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

// start runs a server on a random local port and stops it when the test ends.
func start(t *testing.T, cfg Config) (*Server, string) {
	t.Helper()
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return srv, ln.Addr().String()
}

func dial(t *testing.T, addr string) *resp.Client {
	t.Helper()
	c, err := resp.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// do runs a command and renders the reply compactly: "OK", "(nil)", "(int) 3", "[a b]", "ERR ...".
func do(t *testing.T, c *resp.Client, args ...string) string {
	t.Helper()
	v, err := c.Do(args...)
	if err != nil {
		t.Fatal(err)
	}
	return render(v)
}

func render(v resp.Value) string {
	switch {
	case v.Null:
		return "(nil)"
	case v.Kind == ':':
		return fmt.Sprintf("(int) %d", v.Int)
	case v.Kind == '*':
		parts := make([]string, len(v.Array))
		for i, e := range v.Array {
			parts[i] = render(e)
		}
		return "[" + strings.Join(parts, " ") + "]"
	}
	return v.Str
}

type step struct {
	args []string
	want string
}

func run(t *testing.T, c *resp.Client, steps []step) {
	t.Helper()
	for _, s := range steps {
		if got := do(t, c, s.args...); got != s.want {
			t.Errorf("%v = %q, want %q", s.args, got, s.want)
		}
	}
}

func cmd(args ...string) []string { return args }

func TestConnectionCommands(t *testing.T) {
	_, addr := start(t, Config{})
	c := dial(t, addr)
	run(t, c, []step{
		{cmd("PING"), "PONG"},
		{cmd("ping", "hi"), "hi"},
		{cmd("ECHO", "x y"), "x y"},
		{cmd("SELECT", "0"), "OK"},
		{cmd("SELECT", "1"), "ERR DB index is out of range"},
		{cmd("NOPE", "a"), "ERR unknown command 'NOPE'"},
		{cmd("GET"), "ERR wrong number of arguments for 'get' command"},
		{cmd("CONFIG", "GET", "appendonly"), "[appendonly no]"},
		{cmd("COMMAND", "DOCS"), "[]"},
		{cmd("CLIENT", "SETNAME", "x"), "OK"},
	})
	if info := do(t, c, "INFO", "keyspace"); !strings.Contains(info, "db0:keys=0,expires=0") {
		t.Fatalf("INFO keyspace: %q", info)
	}
}

func TestStrings(t *testing.T) {
	_, addr := start(t, Config{})
	run(t, dial(t, addr), []step{
		{cmd("GET", "k"), "(nil)"},
		{cmd("SET", "k", "v"), "OK"},
		{cmd("GET", "k"), "v"},
		{cmd("SET", "k", "w", "NX"), "(nil)"},
		{cmd("SET", "new", "1", "XX"), "(nil)"},
		{cmd("SET", "k", "w", "XX", "GET"), "v"},
		{cmd("SET", "k", "x", "NX", "XX"), "ERR syntax error"},
		{cmd("SET", "k", "x", "EX", "0"), "ERR invalid expire time in 'set' command"},
		{cmd("SET", "k", "x", "BOGUS"), "ERR syntax error"},
		{cmd("SETNX", "k", "z"), "(int) 0"},
		{cmd("APPEND", "k", "123"), "(int) 4"},
		{cmd("STRLEN", "k"), "(int) 4"},
		{cmd("INCR", "n"), "(int) 1"},
		{cmd("INCRBY", "n", "41"), "(int) 42"},
		{cmd("DECRBY", "n", "2"), "(int) 40"},
		{cmd("DECR", "n"), "(int) 39"},
		{cmd("INCR", "k"), "ERR value is not an integer or out of range"},
		{cmd("SET", "big", "9223372036854775807"), "OK"},
		{cmd("INCR", "big"), "ERR increment or decrement would overflow"},
		{cmd("MSET", "a", "1", "b", "2"), "OK"},
		{cmd("MSET", "a", "1", "b"), "ERR wrong number of arguments for 'mset' command"},
		{cmd("MGET", "a", "missing", "b"), "[1 (nil) 2]"},
	})
}

func TestExpiry(t *testing.T) {
	clock := newClock()
	// Active expiry is pushed out of the way so the test sees only lazy expiry.
	_, addr := start(t, Config{Clock: clock.now, ExpireEvery: time.Hour})
	c := dial(t, addr)
	run(t, c, []step{
		{cmd("SET", "s", "v", "EX", "10"), "OK"},
		{cmd("TTL", "s"), "(int) 10"},
		{cmd("PTTL", "s"), "(int) 10000"},
		{cmd("INCR", "counter"), "(int) 1"},
		{cmd("EXPIRE", "counter", "5"), "(int) 1"},
		{cmd("INCR", "counter"), "(int) 2"}, // INCR keeps the TTL
		{cmd("TTL", "counter"), "(int) 5"},
		{cmd("SET", "s", "v2", "KEEPTTL"), "OK"},
		{cmd("TTL", "s"), "(int) 10"},
		{cmd("SET", "p", "v", "PX", "500"), "OK"},
		{cmd("PERSIST", "p"), "(int) 1"},
		{cmd("TTL", "p"), "(int) -1"},
		{cmd("PERSIST", "p"), "(int) 0"},
		{cmd("EXPIRE", "missing", "5"), "(int) 0"},
		{cmd("TTL", "missing"), "(int) -2"},
	})
	clock.advance(5 * time.Second)
	run(t, c, []step{
		{cmd("GET", "counter"), "(nil)"},
		{cmd("TTL", "s"), "(int) 5"},
		{cmd("SETEX", "e", "1", "v"), "OK"},
		{cmd("PEXPIREAT", "p", "1"), "(int) 1"}, // a time in the past deletes immediately
		{cmd("EXISTS", "p"), "(int) 0"},
	})
	clock.advance(5 * time.Second)
	run(t, c, []step{
		{cmd("GET", "s"), "(nil)"},
		{cmd("GET", "e"), "(nil)"},
		// s, e and counter are expired but only read so far: reads never delete, so DBSIZE
		// still counts them until a write or the active expiry cycle reclaims them.
		{cmd("DBSIZE"), "(int) 3"},
	})
}

func TestLists(t *testing.T) {
	_, addr := start(t, Config{})
	run(t, dial(t, addr), []step{
		{cmd("RPUSH", "l", "a", "b"), "(int) 2"},
		{cmd("LPUSH", "l", "y", "z"), "(int) 4"},
		{cmd("LRANGE", "l", "0", "-1"), "[z y a b]"},
		{cmd("LRANGE", "l", "-2", "100"), "[a b]"},
		{cmd("LRANGE", "l", "5", "10"), "[]"},
		{cmd("LINDEX", "l", "-1"), "b"},
		{cmd("LINDEX", "l", "9"), "(nil)"},
		{cmd("LPOP", "l"), "z"},
		{cmd("RPOP", "l", "2"), "[b a]"},
		{cmd("LLEN", "l"), "(int) 1"},
		{cmd("RPOP", "l"), "y"},
		{cmd("EXISTS", "l"), "(int) 0"}, // empty lists are deleted
		{cmd("LPOP", "l"), "(nil)"},
		{cmd("LPOP", "l", "-1"), "ERR value is out of range, must be positive"},
		{cmd("SET", "s", "v"), "OK"},
		{cmd("LPUSH", "s", "x"), "WRONGTYPE Operation against a key holding the wrong kind of value"},
		{cmd("TYPE", "s"), "string"},
	})
}

func TestHashes(t *testing.T) {
	_, addr := start(t, Config{})
	c := dial(t, addr)
	run(t, c, []step{
		{cmd("HSET", "h", "f1", "a", "f2", "b"), "(int) 2"},
		{cmd("HSET", "h", "f1", "A"), "(int) 0"},
		{cmd("HGET", "h", "f1"), "A"},
		{cmd("HGET", "h", "nope"), "(nil)"},
		{cmd("HLEN", "h"), "(int) 2"},
		{cmd("HEXISTS", "h", "f2"), "(int) 1"},
		{cmd("HINCRBY", "h", "n", "5"), "(int) 5"},
		{cmd("HINCRBY", "h", "f1", "1"), "ERR hash value is not an integer"},
		{cmd("HDEL", "h", "f1", "f2", "n", "zz"), "(int) 3"},
		{cmd("EXISTS", "h"), "(int) 0"},
		{cmd("HSET", "h", "odd"), "ERR wrong number of arguments for 'hset' command"},
		{cmd("TYPE", "h"), "none"},
	})
	do(t, c, "HSET", "h", "x", "1", "y", "2")
	all := do(t, c, "HGETALL", "h")
	if all != "[x 1 y 2]" && all != "[y 2 x 1]" {
		t.Fatalf("HGETALL = %s", all)
	}
}

func TestKeysDelFlush(t *testing.T) {
	_, addr := start(t, Config{})
	c := dial(t, addr)
	do(t, c, "MSET", "user:1", "a", "user:2", "b", "order:1", "c")
	v, err := c.Do("KEYS", "user:*")
	if err != nil {
		t.Fatal(err)
	}
	keys := v.Strings()
	sort.Strings(keys)
	if !reflect.DeepEqual(keys, []string{"user:1", "user:2"}) {
		t.Fatalf("KEYS = %v", keys)
	}
	run(t, c, []step{
		{cmd("DEL", "user:1", "user:2", "nope"), "(int) 2"},
		{cmd("EXISTS", "order:1", "order:1", "x"), "(int) 2"},
		{cmd("FLUSHDB"), "OK"},
		{cmd("DBSIZE"), "(int) 0"},
	})
}

func TestPipeliningPreservesOrder(t *testing.T) {
	_, addr := start(t, Config{})
	c := dial(t, addr)
	const n = 2000
	for i := range n {
		if err := c.Send("INCR", "p"); err != nil {
			t.Fatal(err)
		}
		_ = i
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= n; i++ {
		v, err := c.Receive()
		if err != nil || v.Int != int64(i) {
			t.Fatalf("reply %d = %+v, %v", i, v, err)
		}
	}
}

func TestInlineAndProtocolErrors(t *testing.T) {
	_, addr := start(t, Config{})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	r := bufio.NewReader(conn)
	fmt.Fprint(conn, "SET greeting hello\r\nGET greeting\r\n")
	for _, want := range []string{"+OK\r\n", "$5\r\n", "hello\r\n"} {
		if line, _ := r.ReadString('\n'); line != want {
			t.Fatalf("got %q, want %q", line, want)
		}
	}
	fmt.Fprint(conn, "*1\r\n$abc\r\n")
	line, _ := r.ReadString('\n')
	if !strings.HasPrefix(line, "-ERR Protocol error: invalid bulk length") {
		t.Fatalf("got %q", line)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := r.ReadByte(); err == nil {
		t.Fatal("server should close the connection after a protocol error")
	}
}

func TestConcurrentClients(t *testing.T) {
	_, addr := start(t, Config{Shards: 8})
	const clients, perClient = 32, 300
	var wg sync.WaitGroup
	for range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := resp.Dial(addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			for range perClient {
				if _, err := c.Do("INCR", "shared"); err != nil {
					t.Error(err)
					return
				}
				if _, err := c.Do("MSET", "x", "1", "y", "1"); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := do(t, dial(t, addr), "GET", "shared"); got != fmt.Sprint(clients*perClient) {
		t.Fatalf("lost updates: %s", got)
	}
}

func TestMaxClients(t *testing.T) {
	_, addr := start(t, Config{MaxClients: 1})
	first := dial(t, addr)
	do(t, first, "PING") // ensure the first connection is registered
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	line, _ := bufio.NewReader(conn).ReadString('\n')
	if line != "-ERR max number of clients reached\r\n" {
		t.Fatalf("got %q", line)
	}
}

func TestQuitClosesConnection(t *testing.T) {
	_, addr := start(t, Config{})
	c := dial(t, addr)
	if got := do(t, c, "QUIT"); got != "OK" {
		t.Fatal(got)
	}
	if _, err := c.Do("PING"); err == nil {
		t.Fatal("connection should be closed")
	}
}

// ---------------------------------------------------------------------------- persistence

// state dumps the keyspace in a canonical text form, for comparing two servers.
func state(t *testing.T, srv *Server) string {
	t.Helper()
	var lines []string
	for _, ke := range srv.Store().Snapshot(nil) {
		e := ke.Entry
		var v string
		switch {
		case e.Str != nil:
			v = string(e.Str)
		case e.List != nil:
			parts := make([]string, e.List.Len())
			for i, x := range e.List.Range(0, e.List.Len()) {
				parts[i] = string(x)
			}
			v = strings.Join(parts, ",")
		default:
			var fields []string
			for f, x := range e.Hash {
				fields = append(fields, f+"="+string(x))
			}
			sort.Strings(fields)
			v = strings.Join(fields, ",")
		}
		lines = append(lines, fmt.Sprintf("%s %s %s exp=%d", ke.Key, e.Kind, v, e.ExpireAt))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func restart(t *testing.T, cfg Config, srv *Server) (*Server, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	return start(t, cfg)
}

func TestAOFRestoresState(t *testing.T) {
	clock := newClock()
	cfg := Config{AOFPath: filepath.Join(t.TempDir(), "db.aof"), Fsync: aof.EverySec, Clock: clock.now}
	srv, addr := start(t, cfg)
	c := dial(t, addr)
	for _, args := range [][]string{
		{"SET", "s", "hello"}, {"APPEND", "s", " world"},
		{"INCRBY", "n", "10"}, {"DECR", "n"},
		{"RPUSH", "l", "a", "b", "c"}, {"LPUSH", "l", "z"}, {"LPOP", "l"}, {"RPOP", "l", "1"},
		{"HSET", "h", "f", "1", "g", "2"}, {"HDEL", "h", "g"}, {"HINCRBY", "h", "f", "4"},
		{"SET", "ttl", "v", "EX", "100"}, {"EXPIRE", "n", "50"}, {"SET", "tmp", "v"},
		{"DEL", "tmp"}, {"MSET", "a", "1", "b", "2"}, {"SET", "gone", "v", "PX", "10"},
	} {
		do(t, c, args...)
	}
	before := state(t, srv)
	reloaded, _ := restart(t, cfg, srv)
	after := state(t, reloaded)
	if before != after {
		t.Fatalf("state differs after restart:\nbefore:\n%s\nafter:\n%s", before, after)
	}
	if !strings.Contains(after, "s string hello world") || !strings.Contains(after, "l list a,b") {
		t.Fatalf("unexpected state:\n%s", after)
	}
}

func TestAOFStoresAbsoluteExpiry(t *testing.T) {
	clock := newClock()
	cfg := Config{AOFPath: filepath.Join(t.TempDir(), "db.aof"), Clock: clock.now}
	srv, addr := start(t, cfg)
	do(t, dial(t, addr), "SET", "k", "v", "EX", "100")

	clock.advance(60 * time.Second) // "downtime": the TTL keeps running while the server is off
	srv, addr = restart(t, cfg, srv)
	if got := do(t, dial(t, addr), "TTL", "k"); got != "(int) 40" {
		t.Fatalf("TTL after restart = %s, want 40", got)
	}
	clock.advance(41 * time.Second)
	_, addr = restart(t, cfg, srv)
	if got := do(t, dial(t, addr), "GET", "k"); got != "(nil)" {
		t.Fatalf("key should have expired, got %s", got)
	}
}

func TestAOFRecoversFromTornWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.aof")
	srv, addr := start(t, Config{AOFPath: path, Fsync: aof.Always})
	do(t, dial(t, addr), "SET", "safe", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	// A crash in the middle of an append leaves half a command at the end of the file.
	if _, err := f.WriteString("*3\r\n$3\r\nSET\r\n$4\r\ntorn"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	srv2, addr2 := start(t, Config{AOFPath: path})
	c := dial(t, addr2)
	run(t, c, []step{{cmd("GET", "safe"), "1"}, {cmd("GET", "torn"), "(nil)"}, {cmd("SET", "after", "ok"), "OK"}})
	_, addr3 := restart(t, Config{AOFPath: path}, srv2)
	if got := do(t, dial(t, addr3), "GET", "after"); got != "ok" {
		t.Fatalf("writes after recovery were lost: %s", got)
	}
}

func TestAOFCorruptionFailsStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.aof")
	if err := os.WriteFile(path, []byte("*1\r\n$4\r\nPING\r\n*1\r\n!bad\r\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{AOFPath: path}); err == nil {
		t.Fatal("expected startup to fail on a corrupt AOF")
	}
}

func waitRewrite(t *testing.T, srv *Server) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for srv.rewriting.Load() {
		if time.Now().After(deadline) {
			t.Fatal("rewrite did not finish")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestBGRewriteCompactsWithoutLosingConcurrentWrites(t *testing.T) {
	cfg := Config{AOFPath: filepath.Join(t.TempDir(), "db.aof"), Shards: 8}
	srv, addr := start(t, cfg)
	c := dial(t, addr)
	for range 2000 {
		do(t, c, "INCR", "counter")
	}
	for i := range 300 {
		do(t, c, "RPUSH", "list", fmt.Sprint(i))
		do(t, c, "HSET", "hash", fmt.Sprint("f", i), fmt.Sprint(i))
	}
	sizeBefore := srv.aof.Size()

	// Keep writing from several clients while the rewrite runs.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wc, err := resp.Dial(addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer wc.Close()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := wc.Do("INCR", "counter"); err != nil {
					t.Error(err)
					return
				}
				if _, err := wc.Do("SET", fmt.Sprint("w", w, "-", i%50), fmt.Sprint(i)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	time.Sleep(20 * time.Millisecond)
	if got := do(t, c, "BGREWRITEAOF"); got != "Background append only file rewriting started" {
		t.Fatal(got)
	}
	time.Sleep(20 * time.Millisecond)
	waitRewrite(t, srv)
	close(stop)
	wg.Wait()

	if after := srv.aof.Size(); after >= sizeBefore {
		t.Fatalf("rewrite did not shrink the AOF: %d -> %d", sizeBefore, after)
	}
	before := state(t, srv)
	reloaded, _ := restart(t, cfg, srv)
	if after := state(t, reloaded); before != after {
		t.Fatalf("state differs after rewrite + restart:\nlive:\n%s\nreloaded:\n%s", before, after)
	}
}

func TestShutdownFlushesUnsyncedWrites(t *testing.T) {
	cfg := Config{AOFPath: filepath.Join(t.TempDir(), "db.aof"), Fsync: aof.No}
	srv, addr := start(t, cfg)
	do(t, dial(t, addr), "SET", "k", "v")
	_, addr = restart(t, cfg, srv)
	if got := do(t, dial(t, addr), "GET", "k"); got != "v" {
		t.Fatalf("got %s", got)
	}
}
