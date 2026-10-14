package aof

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func cmd(parts ...string) [][]byte {
	out := make([][]byte, len(parts))
	for i, p := range parts {
		out[i] = []byte(p)
	}
	return out
}

func replayAll(t *testing.T, path string) ([]string, ReplayResult) {
	t.Helper()
	var got []string
	res, err := Replay(path, func(args [][]byte) error {
		parts := make([]string, len(args))
		for i, a := range args {
			parts[i] = string(a)
		}
		got = append(got, strings.Join(parts, " "))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return got, res
}

func TestParsePolicy(t *testing.T) {
	for _, p := range []Policy{Always, EverySec, No} {
		got, err := ParsePolicy(p.String())
		if err != nil || got != p {
			t.Fatalf("%v: got %v, %v", p, got, err)
		}
	}
	if _, err := ParsePolicy("sometimes"); err == nil {
		t.Fatal("expected error")
	}
}

func TestAppendAndReplay(t *testing.T) {
	for _, policy := range []Policy{Always, EverySec, No} {
		t.Run(policy.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "test.aof")
			log, err := Open(path, policy)
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range [][][]byte{cmd("SET", "a", "1"), cmd("INCR", "a"), cmd("DEL", "b")} {
				if err := log.Append(c...); err != nil {
					t.Fatal(err)
				}
			}
			if err := log.Close(); err != nil {
				t.Fatal(err)
			}
			got, res := replayAll(t, path)
			want := []string{"SET a 1", "INCR a", "DEL b"}
			if !reflect.DeepEqual(got, want) || res.Commands != 3 {
				t.Fatalf("got %q (%d)", got, res.Commands)
			}
		})
	}
}

func TestReplayMissingFileIsEmpty(t *testing.T) {
	got, res := replayAll(t, filepath.Join(t.TempDir(), "none.aof"))
	if len(got) != 0 || res.Commands != 0 {
		t.Fatal("expected nothing")
	}
}

func TestReplayTruncatesIncompleteTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "torn.aof")
	good := "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
	torn := "*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$5\r\nhel" // crash mid-write
	if err := os.WriteFile(path, []byte(good+torn), 0o644); err != nil {
		t.Fatal(err)
	}
	got, res := replayAll(t, path)
	if !reflect.DeepEqual(got, []string{"SET a 1"}) || res.TruncatedBytes != int64(len(torn)) {
		t.Fatalf("got %q, truncated %d", got, res.TruncatedBytes)
	}
	data, _ := os.ReadFile(path)
	if string(data) != good {
		t.Fatalf("file not truncated to the last complete command: %q", data)
	}
}

func TestReplayRejectsCorruptionInTheMiddle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.aof")
	content := "*1\r\n$4\r\nPING\r\n*1\r\n#oops\r\n*1\r\n$4\r\nPING\r\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Replay(path, func([][]byte) error { return nil }); err == nil ||
		!strings.Contains(err.Error(), "corrupt at byte 14") {
		t.Fatalf("got %v", err)
	}
}

func TestRewriteKeepsCommandsAppendedDuringIt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.aof")
	log, err := Open(path, EverySec)
	if err != nil {
		t.Fatal(err)
	}
	for range 100 {
		_ = log.Append(cmd("INCR", "counter")...)
	}
	log.BeginRewrite()
	tmp := filepath.Join(dir, "rewrite.tmp")
	// The snapshot of the state at BeginRewrite: counter = 100.
	if err := os.WriteFile(tmp, []byte("*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$3\r\n100\r\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = log.Append(cmd("INCR", "counter")...) // arrives while the rewrite is running
	if err := log.FinishRewrite(tmp); err != nil {
		t.Fatal(err)
	}
	_ = log.Append(cmd("INCR", "counter")...) // after the swap, into the new file
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	got, _ := replayAll(t, path)
	want := []string{"SET counter 100", "INCR counter", "INCR counter"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q", got)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("temp file should have been renamed away")
	}
}

func TestAbortRewriteLeavesLogUsable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.aof")
	log, err := Open(path, Always)
	if err != nil {
		t.Fatal(err)
	}
	log.BeginRewrite()
	log.AbortRewrite()
	if err := log.Append(cmd("PING")...); err != nil {
		t.Fatal(err)
	}
	if log.Size() == 0 || log.Err() != nil {
		t.Fatal("log should still accept writes")
	}
	if err := log.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
}

// With policy Always every Append returns only after an fsync covering it. Concurrent appends
// should share fsyncs (group commit) rather than each paying for one.
func TestGroupCommitSharesFsyncs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "group.aof")
	log, err := Open(path, Always)
	if err != nil {
		t.Fatal(err)
	}
	const writers, each = 32, 40
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				if err := log.Append(cmd("SET", fmt.Sprint(w), fmt.Sprint(i))...); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	fsyncs := log.Fsyncs()
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	got, _ := replayAll(t, path)
	if len(got) != writers*each {
		t.Fatalf("replayed %d commands, want %d", len(got), writers*each)
	}
	if fsyncs >= writers*each {
		t.Fatalf("no group commit: %d fsyncs for %d appends", fsyncs, writers*each)
	}
	t.Logf("%d appends shared %d fsyncs", writers*each, fsyncs)
}
