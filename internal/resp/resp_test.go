package resp

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func readAll(t *testing.T, input string) ([][]string, error) {
	t.Helper()
	r := NewReader(strings.NewReader(input))
	var out [][]string
	for {
		args, err := r.ReadCommand()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		strs := make([]string, len(args))
		for i, a := range args {
			strs[i] = string(a)
		}
		out = append(out, strs)
	}
}

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  [][]string
	}{
		{"multibulk", "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n", [][]string{{"GET", "key"}}},
		{"binary safe", "*2\r\n$3\r\nSET\r\n$4\r\na\r\nb\r\n", [][]string{{"SET", "a\r\nb"}}},
		{"empty bulk", "*2\r\n$4\r\nECHO\r\n$0\r\n\r\n", [][]string{{"ECHO", ""}}},
		{"pipelined", "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n", [][]string{{"PING"}, {"PING"}}},
		{"inline", "SET  a   b\r\nGET a\n", [][]string{{"SET", "a", "b"}, {"GET", "a"}}},
		{"empty inline line", "\r\nPING\r\n", [][]string{{}, {"PING"}}},
		{"empty array", "*0\r\n*-1\r\n", [][]string{{}, {}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readAll(t, tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReadCommandErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  error
	}{
		{"bad array length", "*x\r\n", ErrProtocol},
		{"missing $", "*1\r\n:3\r\n", ErrProtocol},
		{"bad bulk length", "*1\r\n$-5\r\n", ErrProtocol},
		{"bulk too large", "*1\r\n$999999999999\r\n", ErrProtocol},
		{"array too large", "*99999999\r\n", ErrProtocol},
		{"bulk without CRLF", "*1\r\n$3\r\nGETxx", ErrProtocol},
		{"line too long", strings.Repeat("a", readBufSize+1), ErrProtocol},
		{"truncated bulk", "*1\r\n$10\r\nabc", io.ErrUnexpectedEOF},
		{"truncated array", "*2\r\n$3\r\nGET\r\n", io.ErrUnexpectedEOF},
		{"truncated header", "*2\r\n$3", io.ErrUnexpectedEOF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := readAll(t, tt.input)
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
}

func TestOffsetCountsOnlyCompleteCommands(t *testing.T) {
	full := "*1\r\n$4\r\nPING\r\n"
	r := NewReader(strings.NewReader(full + "*2\r\n$3\r\nGET"))
	if _, err := r.ReadCommand(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadCommand(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v", err)
	}
	if r.Offset() != int64(len(full)) {
		t.Fatalf("offset %d, want %d", r.Offset(), len(full))
	}
}

func TestWriterEncodesReplies(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.Simple("OK")
	w.Error("ERR boom")
	w.Int(-42)
	w.Bulk([]byte("hi"))
	w.BulkString("")
	w.Null()
	w.BulkArray([][]byte{[]byte("a"), nil})
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	want := "+OK\r\n-ERR boom\r\n:-42\r\n$2\r\nhi\r\n$0\r\n\r\n$-1\r\n*2\r\n$1\r\na\r\n$-1\r\n"
	if buf.String() != want {
		t.Fatalf("got %q", buf.String())
	}

	r := NewReader(&buf)
	var kinds []byte
	for {
		v, err := r.ReadReply()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		kinds = append(kinds, v.Kind)
	}
	if string(kinds) != "+-:$$$*" {
		t.Fatalf("decoded kinds %q", kinds)
	}
}

func TestAppendCommandRoundTrip(t *testing.T) {
	args := [][]byte{[]byte("SET"), []byte("k"), {0, '\r', '\n', 0xff}}
	r := NewReader(bytes.NewReader(AppendCommand(nil, args...)))
	got, err := r.ReadCommand()
	if err != nil || !reflect.DeepEqual(got, args) {
		t.Fatalf("got %q, %v", got, err)
	}
}

// FuzzReadCommand checks the parser never panics on arbitrary input, and that whatever it
// accepts as a multibulk command survives an encode/decode round trip unchanged.
func FuzzReadCommand(f *testing.F) {
	f.Add([]byte("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"))
	f.Add([]byte("PING\r\n"))
	f.Add([]byte("*1\r\n$-1\r\n"))
	f.Add([]byte("*3\r\n$0\r\n\r\n$1\r\n\x00\r\n$2\r\n\r\n\r\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		for range 16 {
			args, err := r.ReadCommand()
			if err != nil {
				return
			}
			if len(args) == 0 {
				continue
			}
			again, err := NewReader(bytes.NewReader(AppendCommand(nil, args...))).ReadCommand()
			if err != nil || !reflect.DeepEqual(again, args) {
				t.Fatalf("round trip of %q gave %q, %v", args, again, err)
			}
		}
	})
}
