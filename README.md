# minikv

[![CI](https://github.com/omergazi34/minikv/actions/workflows/ci.yml/badge.svg)](https://github.com/omergazi34/minikv/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.24+-00ADD8)
![Dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A **Redis-compatible key-value server in Go**, written from scratch on the standard library: the
RESP protocol, a sharded keyspace with TTLs, append-only-file persistence with crash recovery and
online compaction, group-committed fsync, and graceful shutdown.

Real Redis clients work unchanged. CI runs `redis-cli` and `redis-benchmark` against every commit,
and the official Python client (redis-py) was checked by hand: pipelining, TTLs, lists, hashes,
`INFO` parsing and error replies.

```console
$ minikv -aof appendonly.aof &
$ redis-cli -p 6380 SET greeting hello EX 60
OK
$ redis-cli -p 6380 RPUSH queue a b c && redis-cli -p 6380 LRANGE queue 0 -1
(integer) 3
1) "a"
2) "b"
3) "c"
```

## What's inside

| Area | Implementation |
|---|---|
| **Protocol** | RESP2 multibulk and inline commands, pipelining, size limits against memory-exhaustion attacks. The parser is **fuzzed** (~1M inputs per 30 s run) |
| **Keyspace** | Hash map split into N independently locked shards; multi-key commands lock shards in a global order (atomic, deadlock-free) |
| **Expiry** | Lazy expiry on access plus Redis' **adaptive active-expiry** sampling for keys nobody reads |
| **Persistence** | Append-only file with `always` / `everysec` / `no` fsync; **group commit** for `always`; fsync never blocks other writers |
| **Recovery** | A torn final write (crash mid-append) is detected and truncated; corruption elsewhere refuses to start |
| **Compaction** | `BGREWRITEAOF` and automatic rewrites from a consistent snapshot while writes continue, with an atomic file swap |
| **Server** | Goroutine per connection, reply batching, `maxclients`, graceful shutdown that finishes in-flight commands and fsyncs |
| **Ops** | `INFO` (Redis format), structured logs (`log/slog`), 3.5 MB static binary, distroless non-root Docker image |

**Commands (51):** `GET SET (EX PX EXAT PXAT KEEPTTL NX XX GET) SETNX SETEX MGET MSET INCR DECR INCRBY
DECRBY APPEND STRLEN` · `LPUSH RPUSH LPOP RPOP LLEN LRANGE LINDEX` · `HSET HGET HDEL HGETALL HKEYS HVALS
HLEN HEXISTS HINCRBY` · `DEL EXISTS TYPE KEYS EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL PERSIST` ·
`PING ECHO QUIT SELECT DBSIZE FLUSHDB FLUSHALL INFO CONFIG GET CLIENT COMMAND BGREWRITEAOF`

## Performance

`minikv-bench` (included), 50 clients, 16-byte values, 100k keys, loopback. Intel i7-12650H, Windows 11.

| Mode | Pipeline | SET | GET | INCR | LPUSH | p99 (SET) |
|---|---:|---:|---:|---:|---:|---:|
| In-memory | 1 | 88k ops/s | 90k | 88k | 89k | 1.6 ms |
| In-memory | 16 | **882k ops/s** | 841k | – | 804k | 4.7 ms |
| AOF `everysec` | 1 | 91k ops/s | – | 87k | 91k | 1.5 ms |
| AOF `everysec` | 16 | 485k ops/s | – | – | – | 4.6 ms |
| AOF `always` | 1 | 9.7k ops/s | – | 9.9k | – | 15.6 ms |

Unpipelined numbers are bound by one network round trip per command. Pipelining amortises that and
is ~10× faster.

### Three bottlenecks the benchmark found, and what fixed them

| Symptom | Cause | Fix | Result |
|---|---|---|---|
| `LPUSH` at 50k ops/s, p99 5.7 ms; every other command 85k+ | Lists were slices; prepending copied the whole list (O(n)) | A deque made of two back-to-back stacks: O(1) at both ends | **89k ops/s, p99 1.5 ms** |
| `fsync=always`: **722 ops/s**, p50 63 ms | Every write waited for its own fsync, one after another | **Group commit**: one leader fsyncs everything buffered; concurrent writers share it | **9.7k ops/s (13×)**; a test shows 1,280 writes sharing 80 fsyncs |
| `everysec` with pipelining: 100 ms stalls | The per-second fsync ran while holding the AOF lock | fsync outside the lock, durability tracked with monotonic offsets | max stall 100 → 72 ms, throughput +4% |

## Run it

```bash
go install github.com/omergazi34/minikv/cmd/minikv@latest
minikv -aof appendonly.aof -fsync everysec          # default port 6380

# or with Docker: data survives container restarts
docker build -t minikv . && docker run -p 6380:6380 -v minikv-data:/data minikv
```

| Flag | Default | |
|---|---|---|
| `-addr` | `:6380` | listen address |
| `-aof` | *(off)* | append-only file path |
| `-fsync` | `everysec` | `always` · `everysec` · `no` |
| `-aof-rewrite-min-size` | 64 MiB | auto-compact once the AOF exceeds this and has doubled |
| `-shards` | 4 × CPUs | keyspace lock shards |
| `-maxclients` | 10000 | connection limit |

## Design

[DESIGN.md](DESIGN.md) covers the concurrency model (why sharded locks instead of Redis' single
thread), how AOF ordering stays correct with parallel writers, the snapshot/rewrite protocol, group
commit, and the trade-offs I'd revisit.

```text
cmd/minikv          flags, signals, graceful shutdown
cmd/minikv-bench    load generator (clients × pipeline depth, latency percentiles)
internal/resp       RESP parser/encoder + a small client (fuzzed)
internal/store      sharded keyspace, transactions, expiry, deque, glob matcher, snapshots
internal/aof        append-only file: fsync policies, group commit, replay, rewrite
internal/server     TCP server, 51 commands, INFO, AOF integration
```

## Testing

```bash
go test ./...          # unit + integration over real TCP sockets
go test -race ./...    # CI runs every test under the race detector
make fuzz              # fuzz the RESP parser and the glob matcher
```

- **Integration tests over TCP**: pipelining order, protocol errors, 32 concurrent clients with no
  lost updates, `maxclients`, `QUIT`.
- **Persistence tests** restart the server and compare the full keyspace: every command type,
  absolute TTLs across downtime (an injected clock simulates 60 s offline), a torn trailing write,
  mid-file corruption, and a rewrite running while 4 clients keep writing.
- **Model-based test**: the deque against a plain slice for 20,000 random operations.
- **Lock discipline**: a transaction that touches a key it didn't declare panics in tests instead of
  becoming a silent data race.
- **Real clients in CI**: `redis-cli`, `redis-benchmark` (plain and pipelined), and a Docker
  restart test proving data survives the container.

## License

MIT
