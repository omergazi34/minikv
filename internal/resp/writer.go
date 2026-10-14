package resp

import (
	"bufio"
	"io"
	"strconv"
)

// Writer encodes RESP2 replies into a buffer. Write errors are sticky inside bufio.Writer and
// surface on Flush, so individual reply methods don't return errors.
type Writer struct {
	bw      *bufio.Writer
	scratch []byte
}

// NewWriter returns a Writer with a 16 KiB buffer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, 16<<10), scratch: make([]byte, 0, 24)}
}

func (w *Writer) prefixed(prefix byte, n int64) {
	w.bw.WriteByte(prefix)
	w.scratch = strconv.AppendInt(w.scratch[:0], n, 10)
	w.bw.Write(w.scratch)
	w.bw.WriteString("\r\n")
}

// Simple writes a status reply such as +OK.
func (w *Writer) Simple(s string) {
	w.bw.WriteByte('+')
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

// Error writes an error reply. msg should start with an error code, e.g. "ERR ..." or "WRONGTYPE ...".
func (w *Writer) Error(msg string) {
	w.bw.WriteByte('-')
	w.bw.WriteString(msg)
	w.bw.WriteString("\r\n")
}

// Int writes an integer reply.
func (w *Writer) Int(n int64) { w.prefixed(':', n) }

// Bulk writes a binary-safe bulk string.
func (w *Writer) Bulk(b []byte) {
	w.prefixed('$', int64(len(b)))
	w.bw.Write(b)
	w.bw.WriteString("\r\n")
}

// BulkString writes s as a bulk string.
func (w *Writer) BulkString(s string) {
	w.prefixed('$', int64(len(s)))
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

// Null writes the RESP2 null bulk string ($-1), used for missing keys.
func (w *Writer) Null() { w.bw.WriteString("$-1\r\n") }

// ArrayHeader starts an array of n elements; the caller writes the elements.
func (w *Writer) ArrayHeader(n int) { w.prefixed('*', int64(n)) }

// BulkArray writes an array of bulk strings, with nil elements encoded as null.
func (w *Writer) BulkArray(items [][]byte) {
	w.ArrayHeader(len(items))
	for _, item := range items {
		if item == nil {
			w.Null()
		} else {
			w.Bulk(item)
		}
	}
}

// Buffered is the number of bytes waiting to be flushed.
func (w *Writer) Buffered() int { return w.bw.Buffered() }

// Flush sends buffered replies and reports any write error since the last flush.
func (w *Writer) Flush() error { return w.bw.Flush() }

// AppendCommand encodes args as a RESP array of bulk strings, the format clients send and the
// AOF stores.
func AppendCommand(dst []byte, args ...[]byte) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(args)), 10)
	dst = append(dst, '\r', '\n')
	for _, a := range args {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(a)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, a...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}
