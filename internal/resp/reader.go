// Package resp implements the Redis Serialization Protocol (RESP2): parsing client commands,
// encoding server replies, and a small client used by tests and the benchmark tool.
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Limits protect the server from a single client allocating unbounded memory.
const (
	MaxBulkLen  = 64 << 20 // largest accepted bulk string (64 MiB)
	MaxArrayLen = 1 << 20  // most arguments in one command
	readBufSize = 16 << 10 // also the longest accepted inline command / header line
)

// ErrProtocol wraps every malformed-input error. The server answers it with
// "-ERR Protocol error: ..." and closes the connection, like Redis does.
var ErrProtocol = errors.New("protocol error")

func protocolErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrProtocol, fmt.Sprintf(format, args...))
}

// Reader reads RESP messages and tracks how many bytes it has consumed, which lets
// AOF loading truncate a file exactly after the last complete command.
type Reader struct {
	br     *bufio.Reader
	offset int64
}

// NewReader wraps r in a buffered RESP reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, readBufSize)}
}

// Offset is the number of bytes consumed by fully read messages so far.
func (r *Reader) Offset() int64 { return r.offset }

// Buffered reports bytes already read from the connection but not yet parsed. The server uses
// it to batch replies for pipelined commands: flush only when no more input is waiting.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// readLine returns one CRLF-terminated line without the terminator. The returned slice is only
// valid until the next read. A clean EOF before any byte is io.EOF; EOF mid-line is
// io.ErrUnexpectedEOF.
func (r *Reader) readLine() ([]byte, int, error) {
	line, err := r.br.ReadSlice('\n')
	switch {
	case errors.Is(err, bufio.ErrBufferFull):
		return nil, 0, protocolErr("line longer than %d bytes", readBufSize)
	case errors.Is(err, io.EOF):
		if len(line) == 0 {
			return nil, 0, io.EOF
		}
		return nil, 0, io.ErrUnexpectedEOF
	case err != nil:
		return nil, 0, err
	}
	n := len(line)
	line = line[:n-1]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line, n, nil
}

func parseLen(b []byte, limit int, what string) (int, error) {
	n, err := strconv.Atoi(string(b))
	if err != nil {
		return 0, protocolErr("invalid %s length %q", what, b)
	}
	if n > limit {
		return 0, protocolErr("%s length %d exceeds limit %d", what, n, limit)
	}
	return n, nil
}

// ReadCommand reads one client command: either a RESP array of bulk strings
// ("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n") or an inline command ("GET k\r\n", as typed in telnet).
// An empty line yields an empty, non-nil slice, which callers skip.
func (r *Reader) ReadCommand() ([][]byte, error) {
	line, n, err := r.readLine()
	if err != nil {
		return nil, err
	}
	consumed := int64(n)
	if len(line) == 0 || line[0] != '*' {
		r.offset += consumed
		fields := bytes.Fields(line)
		args := make([][]byte, len(fields))
		for i, f := range fields {
			args[i] = bytes.Clone(f) // line aliases the read buffer
		}
		return args, nil
	}
	count, err := parseLen(line[1:], MaxArrayLen, "multibulk")
	if err != nil {
		return nil, err
	}
	if count <= 0 {
		r.offset += consumed
		return [][]byte{}, nil
	}
	args := make([][]byte, 0, count)
	for range count {
		header, n, err := r.readLine()
		if err != nil {
			return nil, unexpected(err)
		}
		consumed += int64(n)
		if len(header) == 0 || header[0] != '$' {
			return nil, protocolErr("expected '$', got %q", firstByte(header))
		}
		size, err := parseLen(header[1:], MaxBulkLen, "bulk")
		if err != nil {
			return nil, err
		}
		if size < 0 {
			return nil, protocolErr("negative bulk length in command")
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return nil, unexpected(err)
		}
		if buf[size] != '\r' || buf[size+1] != '\n' {
			return nil, protocolErr("bulk string not terminated by CRLF")
		}
		consumed += int64(size + 2)
		args = append(args, buf[:size:size])
	}
	r.offset += consumed
	return args, nil
}

func unexpected(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

func firstByte(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return string(b[:1])
}
