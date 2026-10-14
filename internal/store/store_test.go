package store

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct{ ms atomic.Int64 }

func (c *fakeClock) now() time.Time          { return time.UnixMilli(c.ms.Load()) }
func (c *fakeClock) advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

func newStore(t *testing.T) (*Store, *fakeClock) {
	t.Helper()
	clock := &fakeClock{}
	clock.ms.Store(1_700_000_000_000)
	return New(8, clock.now), clock
}

func put(s *Store, key, val string, ttl time.Duration) {
	_ = s.Update([]string{key}, func(tx *Tx) error {
		e := &Entry{Kind: KindString, Str: []byte(val)}
		if ttl > 0 {
			e.ExpireAt = tx.Now() + ttl.Milliseconds()
		}
		tx.Put(key, e)
		return nil
	})
}

func get(s *Store, key string) *Entry {
	var e *Entry
	_ = s.View([]string{key}, func(tx *Tx) error { e = tx.Get(key); return nil })
	return e
}

func TestLazyExpiry(t *testing.T) {
	s, clock := newStore(t)
	put(s, "k", "v", 10*time.Second)
	if get(s, "k") == nil {
		t.Fatal("key should be live")
	}
	clock.advance(10 * time.Second)
	if get(s, "k") != nil {
		t.Fatal("expired key visible to a reader")
	}
	if keys, _ := s.Len(); keys != 1 {
		t.Fatal("a read-only transaction must not delete")
	}
	_ = s.Update([]string{"k"}, func(tx *Tx) error {
		if tx.Get("k") != nil {
			t.Fatal("expired key visible to a writer")
		}
		return nil
	})
	if keys, volatile := s.Len(); keys != 0 || volatile != 0 {
		t.Fatalf("writer should reclaim the key, got keys=%d volatile=%d", keys, volatile)
	}
	if s.Stats().ExpiredKeys != 1 {
		t.Fatal("expired_keys not counted")
	}
}

func TestActiveExpireReclaimsUnreadKeys(t *testing.T) {
	s, clock := newStore(t)
	for i := range 1000 {
		put(s, fmt.Sprint("tmp", i), "v", time.Second)
	}
	for i := range 100 {
		put(s, fmt.Sprint("keep", i), "v", 0)
	}
	clock.advance(2 * time.Second)
	total := 0
	for range 50 {
		total += s.ActiveExpire(20, 16)
	}
	keys, volatile := s.Len()
	if total != 1000 || keys != 100 || volatile != 0 {
		t.Fatalf("deleted=%d keys=%d volatile=%d", total, keys, volatile)
	}
}

func TestTransactionsMustDeclareKeys(t *testing.T) {
	s := New(64, nil)
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for an undeclared key")
		}
	}()
	_ = s.View([]string{"a"}, func(tx *Tx) error {
		for i := range 1000 { // some key must land in another shard
			tx.Get(fmt.Sprint("other", i))
		}
		return nil
	})
}

func TestReadOnlyTransactionRejectsWrites(t *testing.T) {
	s := New(1, nil)
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = s.View([]string{"a"}, func(tx *Tx) error { tx.Put("a", &Entry{}); return nil })
}

func TestMultiKeyUpdatesAreAtomic(t *testing.T) {
	s := New(16, nil)
	keys := []string{"a", "b", "c", "d"}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				val := []byte(fmt.Sprint(w, ":", i))
				_ = s.Update(keys, func(tx *Tx) error {
					for _, k := range keys {
						tx.Put(k, &Entry{Kind: KindString, Str: val})
					}
					return nil
				})
			}
		}()
	}
	for range 2000 {
		_ = s.View(keys, func(tx *Tx) error {
			first := tx.Get("a")
			for _, k := range keys[1:] {
				if e := tx.Get(k); (e == nil) != (first == nil) || (e != nil && string(e.Str) != string(first.Str)) {
					t.Errorf("torn read: a=%v %s=%v", first, k, e)
				}
			}
			return nil
		})
	}
	close(stop)
	wg.Wait()
}

func TestSnapshotIsIsolatedFromLaterWrites(t *testing.T) {
	s, clock := newStore(t)
	_ = s.Update([]string{"l", "h"}, func(tx *Tx) error {
		tx.Put("l", &Entry{Kind: KindList, List: NewList([]byte("x"))})
		tx.Put("h", &Entry{Kind: KindHash, Hash: map[string][]byte{"f": []byte("1")}})
		return nil
	})
	put(s, "gone", "v", time.Millisecond)
	clock.advance(time.Second)
	called := false
	snap := s.Snapshot(func() { called = true })
	_ = s.Update([]string{"l", "h"}, func(tx *Tx) error {
		l := tx.Get("l")
		l.List.PopFront()
		l.List.PushFront([]byte("mutated"))
		tx.Get("h").Hash["f"] = []byte("2")
		return nil
	})
	if !called || len(snap) != 2 {
		t.Fatalf("called=%v entries=%d", called, len(snap))
	}
	for _, ke := range snap {
		switch ke.Key {
		case "l":
			if string(ke.Entry.List.At(0)) != "x" {
				t.Fatal("snapshot list changed")
			}
		case "h":
			if string(ke.Entry.Hash["f"]) != "1" {
				t.Fatal("snapshot hash changed")
			}
		}
	}
}

func TestKeysAndFlush(t *testing.T) {
	s, _ := newStore(t)
	for _, k := range []string{"user:1", "user:2", "session:9"} {
		put(s, k, "v", 0)
	}
	if got := len(s.Keys("user:*")); got != 2 {
		t.Fatalf("KEYS user:* returned %d", got)
	}
	flushed := false
	s.Flush(func() { flushed = true })
	if keys, _ := s.Len(); keys != 0 || !flushed {
		t.Fatal("flush failed")
	}
}

func TestMatch(t *testing.T) {
	tests := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "", true},
		{"*", "anything", true},
		{"user:*", "user:42", true},
		{"user:*", "users:42", false},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"h[a-c]llo", "hbllo", true},
		{"h[c-a]llo", "hbllo", true},
		{`h\*llo`, "h*llo", true},
		{`h\*llo`, "hallo", false},
		{"*a*a*a*b", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false},
		{"[abc", "a", false},
		{"a*", "a", true},
		{"*b", "ab", true},
	}
	for _, tt := range tests {
		if got := Match(tt.pattern, tt.s); got != tt.want {
			t.Errorf("Match(%q, %q) = %v, want %v", tt.pattern, tt.s, got, tt.want)
		}
	}
}

func FuzzMatch(f *testing.F) {
	f.Add("*a?[b-d]\\*", "xaab*")
	f.Fuzz(func(t *testing.T, pattern, s string) {
		// Appending '*' can only widen a pattern, unless the pattern ends in an escaping
		// backslash (then the '*' becomes a literal).
		if Match(pattern, s) && !strings.HasSuffix(pattern, "\\") && !Match(pattern+"*", s) {
			t.Fatalf("%q matches %q but %q does not", pattern, s, pattern+"*")
		}
		if !Match(pattern, pattern) && !hasMeta(pattern) {
			t.Fatalf("literal pattern %q does not match itself", pattern)
		}
	})
}

func hasMeta(p string) bool {
	for i := range len(p) {
		switch p[i] {
		case '*', '?', '[', '\\', ']':
			return true
		}
	}
	return false
}

func BenchmarkParallelSetGet(b *testing.B) {
	s := New(64, nil)
	val := []byte("value")
	var n atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k := fmt.Sprint("key", n.Add(1)%10_000)
			_ = s.Update([]string{k}, func(tx *Tx) error {
				tx.Put(k, &Entry{Kind: KindString, Str: val})
				return nil
			})
			_ = s.View([]string{k}, func(tx *Tx) error { tx.Get(k); return nil })
		}
	})
}
