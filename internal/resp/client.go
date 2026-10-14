package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// Value is a decoded server reply.
type Value struct {
	Kind  byte // '+', '-', ':', '$' or '*'
	Str   string
	Int   int64
	Array []Value
	Null  bool
}

// Err returns the reply as a Go error if it is an error reply.
func (v Value) Err() error {
	if v.Kind == '-' {
		return errors.New(v.Str)
	}
	return nil
}

// Strings returns the elements of an array reply as strings ("" for nulls).
func (v Value) Strings() []string {
	out := make([]string, len(v.Array))
	for i, e := range v.Array {
		out[i] = e.Str
	}
	return out
}

// ReadReply decodes one reply. Replies are trusted (they come from our own server), so only
// basic structural checks are made.
func (r *Reader) ReadReply() (Value, error) {
	line, _, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	if len(line) == 0 {
		return Value{}, protocolErr("empty reply line")
	}
	kind, body := line[0], string(line[1:])
	switch kind {
	case '+', '-':
		return Value{Kind: kind, Str: body}, nil
	case ':':
		n, err := strconv.ParseInt(body, 10, 64)
		if err != nil {
			return Value{}, protocolErr("bad integer reply %q", body)
		}
		return Value{Kind: kind, Int: n}, nil
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			return Value{}, protocolErr("bad bulk length %q", body)
		}
		if n < 0 {
			return Value{Kind: kind, Null: true}, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return Value{}, unexpected(err)
		}
		return Value{Kind: kind, Str: string(buf[:n])}, nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil {
			return Value{}, protocolErr("bad array length %q", body)
		}
		if n < 0 {
			return Value{Kind: kind, Null: true}, nil
		}
		items := make([]Value, n)
		for i := range items {
			if items[i], err = r.ReadReply(); err != nil {
				return Value{}, unexpected(err)
			}
		}
		return Value{Kind: kind, Array: items}, nil
	}
	return Value{}, protocolErr("unknown reply type %q", kind)
}

// Client is a minimal synchronous RESP client with pipelining support.
type Client struct {
	conn net.Conn
	r    *Reader
	bw   *bufio.Writer
	buf  []byte
}

// Dial connects to a RESP server.
func Dial(addr string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, r: NewReader(conn), bw: bufio.NewWriterSize(conn, 16<<10)}, nil
}

// Send buffers a command without waiting for its reply (pipelining).
func (c *Client) Send(args ...string) error {
	c.buf = c.buf[:0]
	c.buf = append(c.buf, '*')
	c.buf = strconv.AppendInt(c.buf, int64(len(args)), 10)
	c.buf = append(c.buf, '\r', '\n')
	for _, a := range args {
		c.buf = append(c.buf, '$')
		c.buf = strconv.AppendInt(c.buf, int64(len(a)), 10)
		c.buf = append(c.buf, '\r', '\n')
		c.buf = append(c.buf, a...)
		c.buf = append(c.buf, '\r', '\n')
	}
	_, err := c.bw.Write(c.buf)
	return err
}

// Flush writes all buffered commands to the connection.
func (c *Client) Flush() error { return c.bw.Flush() }

// Receive reads the next reply.
func (c *Client) Receive() (Value, error) { return c.r.ReadReply() }

// Do sends one command and waits for its reply.
func (c *Client) Do(args ...string) (Value, error) {
	if err := c.Send(args...); err != nil {
		return Value{}, err
	}
	if err := c.Flush(); err != nil {
		return Value{}, err
	}
	v, err := c.Receive()
	if err != nil {
		return Value{}, fmt.Errorf("%v: %w", args, err)
	}
	return v, nil
}

// Close closes the connection.
func (c *Client) Close() error { return c.conn.Close() }
