// Package server is minikv's network front end: it accepts TCP connections, parses RESP
// commands, executes them against the store and persists writes to the AOF.
package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/omergazi34/minikv/internal/aof"
	"github.com/omergazi34/minikv/internal/resp"
	"github.com/omergazi34/minikv/internal/store"
)

// Version is reported by INFO.
const Version = "0.1.0"

// Config configures a Server. Zero values get sensible defaults.
type Config struct {
	Addr           string        // listen address, default ":6380"
	Shards         int           // keyspace shards, default 4×GOMAXPROCS
	AOFPath        string        // empty disables persistence
	Fsync          aof.Policy    // AOF fsync policy
	RewriteMinSize int64         // auto-rewrite once the AOF exceeds this and has doubled; 0 disables
	ExpireEvery    time.Duration // active expiry period, default 100ms
	MaxClients     int           // default 10000
	Clock          func() time.Time
	Logger         *slog.Logger
}

// Server is a Redis-compatible server.
type Server struct {
	cfg     Config
	store   *store.Store
	aof     *aof.Log
	logger  *slog.Logger
	started time.Time

	mu      sync.Mutex
	ln      net.Listener
	conns   map[net.Conn]struct{}
	closing bool

	handlers sync.WaitGroup // one per connection
	bg       sync.WaitGroup // background loops and rewrites
	stop     chan struct{}

	nextConnID      atomic.Int64
	totalConns      atomic.Int64
	totalCommands   atomic.Int64
	rejectedConns   atomic.Int64
	rewriting       atomic.Bool
	lastRewriteSize atomic.Int64
}

var errRewriteInProgress = errors.New("AOF rewrite already in progress")

// New creates a server and, if an AOF path is configured, replays the AOF into memory.
func New(cfg Config) (*Server, error) {
	if cfg.Addr == "" {
		cfg.Addr = ":6380"
	}
	if cfg.Shards <= 0 {
		cfg.Shards = 4 * runtime.GOMAXPROCS(0)
	}
	if cfg.ExpireEvery <= 0 {
		cfg.ExpireEvery = 100 * time.Millisecond
	}
	if cfg.MaxClients <= 0 {
		cfg.MaxClients = 10_000
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	s := &Server{
		cfg:     cfg,
		store:   store.New(cfg.Shards, cfg.Clock),
		logger:  cfg.Logger,
		started: time.Now(),
		conns:   make(map[net.Conn]struct{}),
		stop:    make(chan struct{}),
	}
	if cfg.AOFPath != "" {
		if err := s.loadAOF(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Server) loadAOF() error {
	start := time.Now()
	ctx := &cmdCtx{s: s, w: resp.NewWriter(io.Discard), replay: true}
	res, err := aof.Replay(s.cfg.AOFPath, func(args [][]byte) error {
		ctx.args = args
		s.execute(ctx)
		return nil
	})
	if err != nil {
		return fmt.Errorf("load AOF: %w", err)
	}
	if res.TruncatedBytes > 0 {
		s.logger.Warn("AOF ended with an incomplete command; truncated it",
			"bytes", res.TruncatedBytes)
	}
	keys, _ := s.store.Len()
	s.logger.Info("AOF loaded", "commands", res.Commands, "keys", keys,
		"took", time.Since(start).Round(time.Millisecond))
	log, err := aof.Open(s.cfg.AOFPath, s.cfg.Fsync)
	if err != nil {
		return err
	}
	s.aof = log
	s.lastRewriteSize.Store(log.Size())
	return nil
}

// Store exposes the keyspace (used by tests).
func (s *Server) Store() *store.Store { return s.store }

// ListenAndServe listens on cfg.Addr and serves until Shutdown.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Addr is the listening address once Serve has started.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Serve accepts connections on ln until Shutdown is called.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return net.ErrClosed
	}
	s.ln = ln
	s.mu.Unlock()
	s.logger.Info("listening", "addr", ln.Addr().String(), "shards", s.cfg.Shards,
		"aof", s.cfg.AOFPath != "")

	s.bg.Add(1)
	go s.expireLoop()
	if s.aof != nil && s.cfg.RewriteMinSize > 0 {
		s.bg.Add(1)
		go s.autoRewriteLoop()
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.isClosing() {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return err
		}
		if !s.track(conn) {
			s.rejectedConns.Add(1)
			_, _ = conn.Write([]byte("-ERR max number of clients reached\r\n"))
			conn.Close()
			continue
		}
		s.totalConns.Add(1)
		s.handlers.Add(1)
		go s.handle(conn, s.nextConnID.Add(1))
	}
}

func (s *Server) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

func (s *Server) track(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || len(s.conns) >= s.cfg.MaxClients {
		return false
	}
	s.conns[conn] = struct{}{}
	return true
}

func (s *Server) untrack(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
}

// handle serves one client. Replies are buffered and flushed only when the client has no more
// pipelined commands waiting, so a batch of N commands costs one write syscall, not N.
func (s *Server) handle(conn net.Conn, id int64) {
	defer s.handlers.Done()
	defer s.untrack(conn)
	defer conn.Close()

	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)
	ctx := &cmdCtx{s: s, w: w, connID: id}
	for {
		args, err := r.ReadCommand()
		if err != nil {
			if errors.Is(err, resp.ErrProtocol) {
				w.Error("ERR Protocol error: " + strings.TrimPrefix(err.Error(), "protocol error: "))
				_ = w.Flush()
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		ctx.args = args
		s.execute(ctx)
		if ctx.quit {
			_ = w.Flush()
			return
		}
		if r.Buffered() == 0 || w.Buffered() > 64<<10 {
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}

func (s *Server) execute(c *cmdCtx) {
	name := strings.ToLower(string(c.args[0]))
	cmd, ok := commands[name]
	if !ok {
		c.w.Error(fmt.Sprintf("ERR unknown command '%s'", truncate(c.args[0], 64)))
		return
	}
	if (cmd.arity > 0 && len(c.args) != cmd.arity) || (cmd.arity < 0 && len(c.args) < -cmd.arity) {
		c.w.Error(fmt.Sprintf("ERR wrong number of arguments for '%s' command", name))
		return
	}
	if cmd.write && !c.replay && s.aof != nil {
		if err := s.aof.Err(); err != nil {
			c.w.Error("MISCONF Errors writing to the AOF file: " + err.Error())
			return
		}
	}
	s.totalCommands.Add(1)
	cmd.fn(c)
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}

func (s *Server) expireLoop() {
	defer s.bg.Done()
	ticker := time.NewTicker(s.cfg.ExpireEvery)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.store.ActiveExpire(20, 16)
		}
	}
}

func (s *Server) autoRewriteLoop() {
	defer s.bg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			size := s.aof.Size()
			if size >= s.cfg.RewriteMinSize && size >= 2*s.lastRewriteSize.Load() {
				if err := s.rewriteAOF(); err != nil && !errors.Is(err, errRewriteInProgress) {
					s.logger.Error("automatic AOF rewrite failed", "err", err)
				}
			}
		}
	}
}

// rewriteAOF compacts the AOF into the minimal commands that recreate the current keyspace.
// The snapshot and BeginRewrite happen under all shard locks, so every write lands either in
// the snapshot or in the rewrite buffer, exactly once.
func (s *Server) rewriteAOF() error {
	if !s.rewriting.CompareAndSwap(false, true) {
		return errRewriteInProgress
	}
	defer s.rewriting.Store(false)
	start := time.Now()
	before := s.aof.Size()
	snapshot := s.store.Snapshot(s.aof.BeginRewrite)
	tmp := s.cfg.AOFPath + ".rewrite.tmp"
	if err := writeSnapshot(tmp, snapshot); err != nil {
		s.aof.AbortRewrite()
		os.Remove(tmp)
		return err
	}
	if err := s.aof.FinishRewrite(tmp); err != nil {
		s.aof.AbortRewrite()
		os.Remove(tmp)
		return err
	}
	after := s.aof.Size()
	s.lastRewriteSize.Store(after)
	s.logger.Info("AOF rewritten", "keys", len(snapshot), "before_bytes", before,
		"after_bytes", after, "took", time.Since(start).Round(time.Millisecond))
	return nil
}

// writeSnapshot serialises a snapshot as RESP commands. Large lists and hashes are split into
// several commands so no single command exceeds the protocol limits on replay.
func writeSnapshot(path string, entries []store.KeyEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 256<<10)
	var buf []byte
	emit := func(args ...[]byte) {
		buf = resp.AppendCommand(buf[:0], args...)
		bw.Write(buf)
	}
	const chunk = 128
	for _, ke := range entries {
		key, e := []byte(ke.Key), ke.Entry
		switch e.Kind {
		case store.KindString:
			if e.ExpireAt != 0 {
				emit([]byte("SET"), key, e.Str, []byte("PXAT"), itob(e.ExpireAt))
				continue
			}
			emit([]byte("SET"), key, e.Str)
			continue
		case store.KindList:
			for i := 0; i < e.List.Len(); i += chunk {
				items := e.List.Range(i, min(i+chunk, e.List.Len()))
				emit(append([][]byte{[]byte("RPUSH"), key}, items...)...)
			}
		case store.KindHash:
			args := [][]byte{[]byte("HSET"), key}
			for f, v := range e.Hash {
				args = append(args, []byte(f), v)
				if len(args) >= 2+2*chunk {
					emit(args...)
					args = args[:2]
				}
			}
			if len(args) > 2 {
				emit(args...)
			}
		}
		if e.ExpireAt != 0 {
			emit([]byte("PEXPIREAT"), key, itob(e.ExpireAt))
		}
	}
	err = errors.Join(bw.Flush(), f.Sync())
	return errors.Join(err, f.Close())
}

// Shutdown stops accepting connections, lets in-flight commands finish, then flushes and
// fsyncs the AOF. If ctx expires first, remaining connections are closed forcibly.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	var lnErr error
	if s.ln != nil {
		lnErr = s.ln.Close()
	}
	// Unblock reads: each handler finishes the command it is executing, replies, then exits.
	for conn := range s.conns {
		_ = conn.SetReadDeadline(time.Now())
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() { s.handlers.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		s.mu.Lock()
		for conn := range s.conns {
			conn.Close()
		}
		s.mu.Unlock()
		<-done
	}
	close(s.stop)
	s.bg.Wait()
	var aofErr error
	if s.aof != nil {
		aofErr = s.aof.Close()
	}
	if errors.Is(lnErr, net.ErrClosed) {
		lnErr = nil
	}
	return errors.Join(lnErr, aofErr)
}

func cmdInfo(c *cmdCtx) {
	s := c.s
	keys, volatile := s.store.Len()
	st := s.store.Stats()
	s.mu.Lock()
	clients := len(s.conns)
	s.mu.Unlock()
	aofEnabled, aofSize, aofStatus, fsyncs := 0, int64(0), "ok", int64(0)
	if s.aof != nil {
		aofEnabled, aofSize, fsyncs = 1, s.aof.Size(), s.aof.Fsyncs()
		if s.aof.Err() != nil {
			aofStatus = "err"
		}
	}
	sections := []struct {
		name   string
		fields [][2]string
	}{
		{"Server", [][2]string{
			{"redis_version", "7.2.0"}, // what clients check for feature support
			{"minikv_version", Version},
			{"go_version", runtime.Version()},
			{"os", runtime.GOOS + "/" + runtime.GOARCH},
			{"uptime_in_seconds", fmt.Sprint(int64(time.Since(s.started).Seconds()))},
		}},
		{"Clients", [][2]string{{"connected_clients", fmt.Sprint(clients)}}},
		{"Stats", [][2]string{
			{"total_connections_received", fmt.Sprint(s.totalConns.Load())},
			{"total_commands_processed", fmt.Sprint(s.totalCommands.Load())},
			{"rejected_connections", fmt.Sprint(s.rejectedConns.Load())},
			{"expired_keys", fmt.Sprint(st.ExpiredKeys)},
			{"keyspace_hits", fmt.Sprint(st.Hits)},
			{"keyspace_misses", fmt.Sprint(st.Misses)},
		}},
		{"Persistence", [][2]string{
			{"aof_enabled", fmt.Sprint(aofEnabled)},
			{"aof_current_size", fmt.Sprint(aofSize)},
			{"aof_rewrite_in_progress", map[bool]string{true: "1", false: "0"}[s.rewriting.Load()]},
			{"aof_last_write_status", aofStatus},
			{"aof_fsync", s.cfg.Fsync.String()},
			{"aof_fsyncs", fmt.Sprint(fsyncs)},
		}},
		{"Keyspace", [][2]string{{"db0", fmt.Sprintf("keys=%d,expires=%d", keys, volatile)}}},
	}
	want := ""
	if len(c.args) > 1 {
		want = strings.ToLower(string(c.args[1]))
	}
	var b strings.Builder
	for _, sec := range sections {
		if want != "" && want != "all" && want != strings.ToLower(sec.name) {
			continue
		}
		fmt.Fprintf(&b, "# %s\r\n", sec.name)
		for _, f := range sec.fields {
			fmt.Fprintf(&b, "%s:%s\r\n", f[0], f[1])
		}
		b.WriteString("\r\n")
	}
	c.w.BulkString(b.String())
}
