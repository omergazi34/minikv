// Package store is minikv's in-memory keyspace: a hash map split into independently locked
// shards, with per-key expiry.
package store

import (
	"errors"
	"hash/fnv"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Kind is the type of value stored under a key.
type Kind uint8

// Value kinds, matching Redis' TYPE names.
const (
	KindString Kind = iota + 1
	KindList
	KindHash
)

func (k Kind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindList:
		return "list"
	case KindHash:
		return "hash"
	}
	return "none"
}

// Errors returned to clients verbatim, so they carry Redis' error codes.
var (
	ErrWrongType  = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrNotInteger = errors.New("ERR value is not an integer or out of range")
	ErrOverflow   = errors.New("ERR increment or decrement would overflow")
)

// Entry is one key's value. Exactly one of Str, List, Hash is used, according to Kind.
//
// Byte slices are never modified in place once stored; operations replace them or append past
// their length. Snapshots therefore only need to copy the List structure and the Hash map.
type Entry struct {
	Kind     Kind
	Str      []byte
	List     *List
	Hash     map[string][]byte
	ExpireAt int64 // Unix milliseconds; 0 means no expiry
}

func (e *Entry) expired(now int64) bool { return e.ExpireAt != 0 && e.ExpireAt <= now }

type shard struct {
	mu   sync.RWMutex
	data map[string]*Entry
	// volatile holds the keys that have an expiry, so the active expiry cycle can sample
	// them without scanning the whole shard.
	volatile map[string]struct{}
}

// Store is a sharded keyspace. Operations on keys in different shards run in parallel.
type Store struct {
	shards  []shard
	clock   func() time.Time
	expired atomic.Int64
	hits    atomic.Int64
	misses  atomic.Int64
}

// New creates a store with n shards (rounded up to at least 1). clock may be nil.
func New(n int, clock func() time.Time) *Store {
	n = max(n, 1)
	if clock == nil {
		clock = time.Now
	}
	s := &Store{shards: make([]shard, n), clock: clock}
	for i := range s.shards {
		s.shards[i].data = make(map[string]*Entry)
		s.shards[i].volatile = make(map[string]struct{})
	}
	return s
}

// Now is the store's clock in Unix milliseconds.
func (s *Store) Now() int64 { return s.clock().UnixMilli() }

func (s *Store) shardOf(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(s.shards)))
}

// lockOrder returns the distinct shard indices for keys in ascending order. Every multi-shard
// lock is taken in this order, which rules out deadlocks between concurrent multi-key commands.
func (s *Store) lockOrder(keys []string) []int {
	idx := make([]int, 0, len(keys))
	for _, k := range keys {
		idx = append(idx, s.shardOf(k))
	}
	slices.Sort(idx)
	return slices.Compact(idx)
}

// Tx gives access to the keys a command declared up front, under their shard locks.
type Tx struct {
	s      *Store
	now    int64
	write  bool
	locked []int // sorted shard indices held by this transaction
}

// shard returns key's shard, panicking if the transaction did not lock it. Commands must
// declare every key they touch; this turns a forgotten key into a test failure instead of a
// data race.
func (tx *Tx) shard(key string) *shard {
	i := tx.s.shardOf(key)
	if _, ok := slices.BinarySearch(tx.locked, i); !ok {
		panic("store: key " + key + " was not declared by the transaction")
	}
	return &tx.s.shards[i]
}

// View runs fn with read locks on the shards of keys.
func (s *Store) View(keys []string, fn func(*Tx) error) error {
	order := s.lockOrder(keys)
	for _, i := range order {
		s.shards[i].mu.RLock()
	}
	defer func() {
		for _, i := range order {
			s.shards[i].mu.RUnlock()
		}
	}()
	return fn(&Tx{s: s, now: s.Now(), locked: order})
}

// Update runs fn with write locks on the shards of keys. Multi-key commands (MSET, DEL k1 k2)
// are atomic: no other command observes a partial result.
func (s *Store) Update(keys []string, fn func(*Tx) error) error {
	order := s.lockOrder(keys)
	for _, i := range order {
		s.shards[i].mu.Lock()
	}
	defer func() {
		for _, i := range order {
			s.shards[i].mu.Unlock()
		}
	}()
	return fn(&Tx{s: s, now: s.Now(), write: true, locked: order})
}

// Now is the time the transaction started, in Unix milliseconds. Every expiry check in one
// command uses the same instant.
func (tx *Tx) Now() int64 { return tx.now }

// Get returns the live entry for key, or nil. In a write transaction an expired entry is
// deleted on the spot (lazy expiry); in a read transaction it is only reported as missing.
func (tx *Tx) Get(key string) *Entry {
	sh := tx.shard(key)
	e, ok := sh.data[key]
	if !ok {
		return nil
	}
	if e.expired(tx.now) {
		if tx.write {
			delete(sh.data, key)
			delete(sh.volatile, key)
			tx.s.expired.Add(1)
		}
		return nil
	}
	return e
}

// Lookup is Get plus keyspace hit/miss accounting, for read commands (reported by INFO).
func (tx *Tx) Lookup(key string) *Entry {
	e := tx.Get(key)
	if e == nil {
		tx.s.misses.Add(1)
	} else {
		tx.s.hits.Add(1)
	}
	return e
}

// Put stores e under key, replacing any previous value.
func (tx *Tx) Put(key string, e *Entry) {
	tx.mustWrite()
	sh := tx.shard(key)
	sh.data[key] = e
	tx.trackExpiry(sh, key, e)
}

// SetExpire changes the expiry of an existing entry (0 removes it).
func (tx *Tx) SetExpire(key string, e *Entry, at int64) {
	tx.mustWrite()
	e.ExpireAt = at
	tx.trackExpiry(tx.shard(key), key, e)
}

func (tx *Tx) trackExpiry(sh *shard, key string, e *Entry) {
	if e.ExpireAt != 0 {
		sh.volatile[key] = struct{}{}
	} else {
		delete(sh.volatile, key)
	}
}

// Delete removes key and reports whether a live entry existed.
func (tx *Tx) Delete(key string) bool {
	tx.mustWrite()
	existed := tx.Get(key) != nil
	sh := tx.shard(key)
	delete(sh.data, key)
	delete(sh.volatile, key)
	return existed
}

func (tx *Tx) mustWrite() {
	if !tx.write {
		panic("store: mutation inside a read-only transaction")
	}
}

// Len counts keys, including expired keys not yet reclaimed (like Redis' DBSIZE).
func (s *Store) Len() (keys, volatile int) {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		keys += len(sh.data)
		volatile += len(sh.volatile)
		sh.mu.RUnlock()
	}
	return keys, volatile
}

// Keys returns live keys matching a glob pattern. It locks one shard at a time, so it is not
// a point-in-time snapshot under concurrent writes (neither is Redis' SCAN).
func (s *Store) Keys(pattern string) []string {
	now := s.Now()
	var out []string
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for k, e := range sh.data {
			if !e.expired(now) && Match(pattern, k) {
				out = append(out, k)
			}
		}
		sh.mu.RUnlock()
	}
	return out
}

// Flush deletes every key atomically. onFlushed (may be nil) runs while every shard is still
// locked, so the AOF records FLUSHDB in the right order relative to other writes.
func (s *Store) Flush(onFlushed func()) {
	s.lockAll()
	defer s.unlockAll()
	for i := range s.shards {
		s.shards[i].data = make(map[string]*Entry)
		s.shards[i].volatile = make(map[string]struct{})
	}
	if onFlushed != nil {
		onFlushed()
	}
}

func (s *Store) lockAll() {
	for i := range s.shards {
		s.shards[i].mu.Lock()
	}
}

func (s *Store) unlockAll() {
	for i := range s.shards {
		s.shards[i].mu.Unlock()
	}
}

// KeyEntry is one key in a snapshot.
type KeyEntry struct {
	Key   string
	Entry Entry
}

// Snapshot returns a consistent point-in-time copy of all live keys. While every shard is
// read-locked, it calls atSnapshot, which the AOF rewrite uses to mark exactly where new writes
// start being buffered. Writes are blocked while containers are copied; values are shared.
func (s *Store) Snapshot(atSnapshot func()) []KeyEntry {
	for i := range s.shards {
		s.shards[i].mu.RLock()
	}
	now := s.Now()
	var out []KeyEntry
	for i := range s.shards {
		for k, e := range s.shards[i].data {
			if e.expired(now) {
				continue
			}
			c := *e
			if e.List != nil {
				c.List = e.List.Clone()
			}
			if e.Hash != nil {
				c.Hash = make(map[string][]byte, len(e.Hash))
				for f, v := range e.Hash {
					c.Hash[f] = v
				}
			}
			out = append(out, KeyEntry{Key: k, Entry: c})
		}
	}
	if atSnapshot != nil {
		atSnapshot()
	}
	for i := range s.shards {
		s.shards[i].mu.RUnlock()
	}
	return out
}

// ActiveExpire reclaims expired keys that nobody reads, using Redis' adaptive sampling: per
// shard, test up to sampleSize keys with a TTL; if more than a quarter were expired, the shard
// probably has many more, so sample again (bounded by maxRounds). Returns keys deleted.
func (s *Store) ActiveExpire(sampleSize, maxRounds int) int {
	now := s.Now()
	total := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for range maxRounds {
			sampled, deleted := 0, 0
			for k := range sh.volatile { // Go randomises map iteration order: a cheap sample
				if sampled == sampleSize {
					break
				}
				sampled++
				if e := sh.data[k]; e == nil || e.expired(now) {
					delete(sh.data, k)
					delete(sh.volatile, k)
					deleted++
				}
			}
			total += deleted
			if sampled == 0 || deleted*4 <= sampled {
				break
			}
		}
		sh.mu.Unlock()
	}
	s.expired.Add(int64(total))
	return total
}

// Stats are counters reported by INFO.
type Stats struct {
	ExpiredKeys, Hits, Misses int64
}

// Stats returns a copy of the store's counters.
func (s *Store) Stats() Stats {
	return Stats{ExpiredKeys: s.expired.Load(), Hits: s.hits.Load(), Misses: s.misses.Load()}
}
