# minikv: design notes

## 1. Concurrency model: sharded locks, not a single thread

Redis executes every command on one thread. Commands never race, and multi-key commands are
trivially atomic. Go makes a different design natural: one goroutine per connection, each blocking
on its own socket, with the runtime multiplexing them over all cores.

The keyspace is split into `4 × GOMAXPROCS` shards, each a `map` behind its own `sync.RWMutex`
(key → shard by FNV-1a). Commands on keys in different shards run in parallel, and reads share a
shard.

**Multi-key atomicity.** `MSET a 1 b 2` must never be half-visible. A command declares its keys up
front, and `Store.Update` locks *all* their shards before running. To avoid deadlock (client 1
locks shard 3 then waits for 7, client 2 locks 7 then waits for 3), shards are always locked in
ascending index order. With one global order there can be no cycle in the wait-for graph.

**Declared keys are enforced.** A `Tx` checks that every key it touches belongs to a locked shard,
and panics otherwise. A handler that forgets to declare a key fails a test immediately instead of
introducing a data race that shows up once a month in production.

**Trade-off.** Throughput scales with cores for independent keys, but a hot key is still one lock.
Redis' single thread avoids lock overhead entirely and gets its multi-core story from running many
instances. For a learning project in Go, sharding is the idiomatic answer, and it is what makes the
AOF ordering problem below interesting.

## 2. Expiry

Keys store an absolute expiry in Unix milliseconds.

- **Lazy:** every access checks the expiry. A *write* transaction deletes an expired key on the
  spot. A *read* transaction only holds a read lock, so it reports the key as missing and leaves
  the delete to a writer or the active cycle. (This is why `DBSIZE` can still count expired keys,
  exactly as in Redis.)
- **Active:** keys that are never read again would leak forever, so every 100 ms each shard samples
  up to 20 keys from its set of keys-with-TTL (Go's randomised map iteration makes this cheap) and
  deletes the expired ones. If more than 25% of the sample was expired, the shard probably holds many
  more, so it samples again, up to 16 rounds. This is Redis' adaptive algorithm. Work stays
  proportional to how much garbage there is, and a pathological shard can't stall the server.
- **A test clock** is injected through `Config.Clock`, so expiry tests are deterministic.

## 3. The append-only file

Every successful write is appended to the AOF in RESP format. Replaying the file rebuilds memory.

### Ordering with parallel writers
With sharded locks, two clients can write the *same key* almost simultaneously. If command A is
applied before B but logged after B, replay produces a different value than the live server had. So
**commands are appended while their shard locks are still held**. Two writes to the same key are
serialised by that shard lock, and their log order matches their apply order. Writes to different
shards can interleave in any order, because they commute.

### Time-relative commands are logged as absolute
`EXPIRE k 100` replayed a day later must not restart a 100-second countdown. All expiry variants
are logged as `PEXPIREAT k <absolute ms>`, and `SET … EX/PX` as `SET … PXAT <ms>`. A key that expired
while the server was down is gone after replay. A test advances a fake clock 60 s "offline" and
checks the TTL dropped from 100 to 40.

### fsync policies and group commit
- `everysec` (default): a background goroutine flushes and fsyncs once a second, so a crash loses at
  most ~1 s of acknowledged writes.
- `always`: `Append` returns only after an fsync covering it, so the client is acknowledged only when
  the write is on disk.
- `no`: flush once a second, leave fsync to the OS.

The first version of `always` fsynced inside `Append` under the AOF mutex, so every write paid for a
full fsync, serially. The benchmark showed **722 ops/s at a 63 ms median**.

**Group commit** fixes it. Durability is tracked with two monotonic counters, `appended` and `synced`.
A writer that needs durability checks whether an fsync is running. If not, it becomes the leader: it
flushes *everything buffered so far*, releases the mutex, fsyncs, and sets `synced` to the offset it
covered. Writers that arrive meanwhile append to the buffer and wait on a condition variable; the
next leader's fsync covers all of them at once. Result: **9.7k ops/s (13×)**, and a unit test
shows 1,280 concurrent writes completing with 80 fsyncs.

The same change moved `everysec`'s fsync outside the lock. Previously appends stalled for the
duration of the disk flush, which showed up as 100 ms latency spikes.

The counters are logical bytes, not file offsets, because a rewrite replaces the file and resets its
size. Waiters must never be waiting for an offset in a file that no longer exists.

### Crash recovery
A crash mid-append leaves a truncated final command. On startup, replay stops at the incomplete
command, **truncates the file back to the last complete one** (the reader tracks exact byte offsets)
and continues. That data was never acknowledged under `always`, and under `everysec` it falls inside
the documented loss window. Corruption *before* the end is different: it means the file is damaged,
so the server refuses to start rather than silently dropping data. This mirrors Redis'
`aof-load-truncated` behaviour.

### Rewrite (compaction) while serving writes
After a million `INCR counter`s the AOF holds a million commands for one key. A rewrite replaces it
with the minimal commands for the current state, *without stopping writes for the duration*:

1. **Snapshot + start buffering, atomically.** `Store.Snapshot` read-locks every shard (blocking
   writers for a moment), copies the maps (values are shared; they are never mutated in place),
   and, *while still holding the locks*, tells the AOF to start copying new appends into a rewrite
   buffer. Every write is therefore either in the snapshot or in the buffer, never both and never
   neither. Without this, a non-idempotent command like `INCR` could be counted twice.
2. **Serialise the snapshot** to a temp file with no locks held; writers continue.
3. **Swap.** Under the AOF mutex: append the buffer to the temp file, fsync it, close the old file
   (Windows can't rename over an open file), atomically rename, reopen, fsync the directory (POSIX).
   If the rename fails, the old file is reopened untouched.

Redis gets step 1 for free with `fork()` and copy-on-write pages. Go can't fork a multi-threaded
runtime safely, so minikv pays a short stop-the-world copy of map headers instead. The integration
test runs a rewrite while four clients hammer the server, then restarts and compares every key.

## 4. Network layer

- **Pipelining for free.** Replies go into a 16 KiB buffer that is flushed only when the
  connection's read buffer is empty, so a batch of N pipelined commands costs one `write` syscall,
  not N. Order is preserved because one goroutine handles one connection sequentially.
- **Hostile input.** Bulk strings are capped at 64 MiB, arrays at 1M elements, header lines at 16
  KiB. A malformed request gets `-ERR Protocol error` and the connection is closed, as in Redis.
  The parser is fuzzed for crashes and checked for exact encode/decode round trips.
- **Graceful shutdown.** On SIGTERM: close the listener, set an immediate read deadline on every
  connection (each handler finishes the command it is executing, replies, then exits), wait with a
  timeout, stop background loops, then flush and fsync the AOF. A test checks that with `fsync=no`
  an acknowledged write still survives a clean restart.

## 5. Things I'd do differently or next

- **AOF mutex as the write bottleneck.** With 16-deep pipelining, AOF `everysec` reaches 485k ops/s
  against 882k in memory. Every append encodes and copies under one mutex. Per-connection buffers
  merged at flush time, or a lock-free MPSC ring, would remove most of that.
- **Memory accounting and eviction** (`maxmemory` with approximate LRU via sampling, like Redis).
- **Replication:** stream the AOF to followers. The absolute-time logging already makes the log
  deterministic to replay elsewhere.
- **Snapshots without stop-the-world:** persistent data structures (or copy-on-write per shard) would
  make the snapshot pause independent of keyspace size.
- More types: sets and sorted sets (a skiplist is the natural next data-structure exercise).
