// Package aof implements minikv's append-only file: every write command is appended in RESP
// format, and replaying the file rebuilds the keyspace after a restart.
package aof

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/omergazi34/minikv/internal/resp"
)

// Policy controls when appended data is fsynced to disk: the classic durability/throughput
// trade-off.
type Policy int

const (
	// Always fsyncs before the write command replies. Nothing acknowledged is ever lost.
	Always Policy = iota
	// EverySec fsyncs once per second from a background goroutine. At most ~1s of
	// acknowledged writes can be lost on power failure; this is the default.
	EverySec
	// No leaves fsync to the operating system and only flushes the buffer once per second.
	No
)

// ParsePolicy parses "always", "everysec" or "no".
func ParsePolicy(s string) (Policy, error) {
	switch s {
	case "always":
		return Always, nil
	case "everysec":
		return EverySec, nil
	case "no":
		return No, nil
	}
	return 0, fmt.Errorf("unknown fsync policy %q (want always, everysec or no)", s)
}

func (p Policy) String() string {
	return [...]string{"always", "everysec", "no"}[p]
}

// Log is an open append-only file. It is safe for concurrent use.
//
// Durability is tracked with two monotonic counters: appended (bytes handed to Append) and
// synced (bytes known to be on disk). fsync always runs *without* holding mu, so appends keep
// flowing while the disk works:
//
//   - EverySec: a background goroutine flushes and fsyncs once per second.
//   - Always: group commit. An appender that needs durability becomes the "leader" if no
//     fsync is running, flushes everything buffered so far and fsyncs it; appenders that arrive
//     meanwhile wait on cond and are usually covered by that fsync or the next one. Many
//     concurrent writes share one fsync instead of queueing for one each.
type Log struct {
	mu      sync.Mutex
	cond    *sync.Cond // broadcast whenever synced advances or a sync finishes
	path    string
	policy  Policy
	f       *os.File
	w       *bufio.Writer
	size    int64 // bytes in the current file, including buffered ones
	err     error // first write/sync error; sticky until a successful rewrite
	scratch []byte

	appended int64 // logical bytes ever appended (survives rewrites, unlike size)
	synced   int64 // logical bytes known to be durable
	syncing  bool  // an fsync is in progress outside the lock
	fsyncs   int64 // completed fsyncs, reported by INFO

	rewriting  bool
	rewriteBuf bytes.Buffer // commands appended while a rewrite is in progress

	stop chan struct{}
	done chan struct{}
}

// Open opens (creating if needed) the AOF at path for appending.
func Open(path string, policy Policy) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	l := &Log{
		path:   path,
		policy: policy,
		f:      f,
		w:      bufio.NewWriterSize(f, 64<<10),
		size:   info.Size(),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	l.cond = sync.NewCond(&l.mu)
	go l.background()
	return l, nil
}

func (l *Log) background() {
	defer close(l.done)
	if l.policy == Always {
		<-l.stop
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			l.mu.Lock()
			if l.policy == EverySec {
				if !l.syncing {
					l.syncOutsideLock()
				}
			} else if l.err == nil {
				if err := l.w.Flush(); err != nil {
					l.err = err
				}
			}
			l.mu.Unlock()
		}
	}
}

// syncOutsideLock flushes the buffer and fsyncs everything appended so far. It must be called
// with mu held and no other sync running; it releases mu during the fsync and re-acquires it.
func (l *Log) syncOutsideLock() {
	if l.err != nil {
		return
	}
	if err := l.w.Flush(); err != nil {
		l.err = err
		l.cond.Broadcast()
		return
	}
	upTo, f := l.appended, l.f
	l.syncing = true
	l.mu.Unlock()
	err := f.Sync()
	l.mu.Lock()
	l.syncing = false
	if err != nil {
		l.err = err
	} else {
		l.fsyncs++
		l.synced = max(l.synced, upTo)
	}
	l.cond.Broadcast()
}

// waitIdle waits until no fsync is running outside the lock. Must be called with mu held.
func (l *Log) waitIdle() {
	for l.syncing {
		l.cond.Wait()
	}
}

// Append records one command. Callers append while still holding the keyspace locks the
// command used, so the order in the file matches the order in which commands were applied.
// With the Always policy it returns only once the command is on disk.
func (l *Log) Append(args ...[]byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return l.err
	}
	l.scratch = resp.AppendCommand(l.scratch[:0], args...)
	if _, err := l.w.Write(l.scratch); err != nil {
		l.err = err
		return err
	}
	n := int64(len(l.scratch))
	l.size += n
	l.appended += n
	if l.rewriting {
		l.rewriteBuf.Write(l.scratch)
	}
	if l.policy != Always {
		return nil
	}
	target := l.appended
	for l.synced < target && l.err == nil {
		if l.syncing {
			l.cond.Wait() // someone else's fsync is running; it or the next one covers us
		} else {
			l.syncOutsideLock() // become the leader for the next group
		}
	}
	return l.err
}

// Err returns the sticky write error, if any. The server refuses writes while it is set.
func (l *Log) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// Fsyncs is the number of completed fsync calls.
func (l *Log) Fsyncs() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fsyncs
}

// Size is the current file size in bytes.
func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.size
}

// Sync flushes buffered data and fsyncs the file.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.waitIdle()
	l.syncOutsideLock()
	return l.err
}

// Close stops the background flusher, then flushes, fsyncs and closes the file.
func (l *Log) Close() error {
	close(l.stop)
	<-l.done
	l.mu.Lock()
	defer l.mu.Unlock()
	l.waitIdle()
	l.syncOutsideLock()
	return errors.Join(l.err, l.f.Close())
}

// BeginRewrite starts buffering appended commands. It must be called at the exact point the
// keyspace snapshot is taken (inside Store.Snapshot's callback), so that every command is either
// in the snapshot or in the buffer, never in both and never in neither.
func (l *Log) BeginRewrite() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rewriting = true
	l.rewriteBuf.Reset()
}

// AbortRewrite stops buffering after a failed rewrite. The original file is untouched.
func (l *Log) AbortRewrite() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rewriting = false
	l.rewriteBuf.Reset()
}

// FinishRewrite appends the commands buffered during the rewrite to tmpPath (which already
// holds the snapshot), fsyncs it and atomically replaces the live AOF with it. Appends are
// blocked only for this final step, not while the snapshot was being written.
func (l *Log) FinishRewrite(tmpPath string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	defer func() { l.rewriting = false; l.rewriteBuf.Reset() }()
	l.waitIdle() // never swap the file under a running fsync

	tmp, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if _, err := tmp.Write(l.rewriteBuf.Bytes()); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	info, err := tmp.Stat()
	if err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	// Close the old file before renaming over it: Windows refuses to replace an open file.
	if err := l.w.Flush(); err != nil {
		l.err = err
	}
	if err := l.f.Close(); err != nil {
		return err
	}
	renameErr := os.Rename(tmpPath, l.path)
	// Reopen whichever file is now at l.path: the new one, or the untouched old one.
	f, err := os.OpenFile(l.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		l.err = fmt.Errorf("reopen AOF after rewrite: %w", err)
		l.cond.Broadcast()
		return l.err
	}
	l.f = f
	l.w.Reset(f)
	if renameErr != nil {
		return renameErr
	}
	syncDir(filepath.Dir(l.path))
	l.size = info.Size()
	l.err = nil
	// The new file holds the snapshot plus every command buffered since, all fsynced above.
	l.synced = l.appended
	l.cond.Broadcast()
	return nil
}

// syncDir makes a rename durable on POSIX systems. It is a best-effort no-op on Windows,
// where directories cannot be opened for syncing.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}

// ReplayResult describes what Replay loaded.
type ReplayResult struct {
	Commands       int
	TruncatedBytes int64 // bytes of an incomplete trailing command that were cut off
}

// Replay feeds every command in the file at path to apply. A missing file is an empty log.
//
// A crash can leave a half-written command at the end of the file. Like Redis'
// aof-load-truncated, Replay cuts the file back to the last complete command and continues,
// because everything before that point was fully written. Corruption anywhere else is an error.
func Replay(path string, apply func(args [][]byte) error) (ReplayResult, error) {
	var res ReplayResult
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return res, nil
	}
	if err != nil {
		return res, err
	}
	defer f.Close()

	r := resp.NewReader(f)
	for {
		args, err := r.ReadCommand()
		if errors.Is(err, io.EOF) {
			return res, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			info, statErr := f.Stat()
			if statErr != nil {
				return res, statErr
			}
			good := r.Offset()
			res.TruncatedBytes = info.Size() - good
			f.Close()
			return res, os.Truncate(path, good)
		}
		if err != nil {
			return res, fmt.Errorf("AOF corrupt at byte %d: %w", r.Offset(), err)
		}
		if len(args) == 0 {
			continue
		}
		if err := apply(args); err != nil {
			return res, fmt.Errorf("AOF command %d (%s): %w", res.Commands+1, args[0], err)
		}
		res.Commands++
	}
}
