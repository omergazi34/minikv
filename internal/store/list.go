package store

// List is a double-ended sequence with amortised O(1) push and pop at both ends.
//
// It is two stacks back to back: head holds the front of the list in reverse order (so pushing
// to the front is an append) and tail holds the rest in order. The first version stored a
// plain slice and prepended by copying, which made LPUSH O(n). The benchmark showed it: LPUSH
// at ~50k ops/s with a 5.7 ms p99, against ~85k for every other command.
type List struct {
	head [][]byte // reversed: head[len(head)-1] is the first element
	tail [][]byte
}

// NewList returns a list holding items in order.
func NewList(items ...[]byte) *List {
	return &List{tail: append([][]byte(nil), items...)}
}

// Len is the number of elements.
func (l *List) Len() int { return len(l.head) + len(l.tail) }

// At returns the element at index i (0 <= i < Len).
func (l *List) At(i int) []byte {
	if i < len(l.head) {
		return l.head[len(l.head)-1-i]
	}
	return l.tail[i-len(l.head)]
}

// PushFront inserts v before the first element.
func (l *List) PushFront(v []byte) { l.head = append(l.head, v) }

// PushBack appends v after the last element.
func (l *List) PushBack(v []byte) { l.tail = append(l.tail, v) }

// PopFront removes and returns the first element. The list must not be empty.
func (l *List) PopFront() []byte {
	if n := len(l.head); n > 0 {
		v := l.head[n-1]
		l.head[n-1] = nil // let the value be garbage-collected
		l.head = l.head[:n-1]
		return v
	}
	v := l.tail[0]
	l.tail[0] = nil
	l.tail = l.tail[1:]
	return v
}

// PopBack removes and returns the last element. The list must not be empty.
func (l *List) PopBack() []byte {
	if n := len(l.tail); n > 0 {
		v := l.tail[n-1]
		l.tail[n-1] = nil
		l.tail = l.tail[:n-1]
		return v
	}
	v := l.head[0]
	l.head[0] = nil
	l.head = l.head[1:]
	return v
}

// Range returns a new slice with the elements in [lo, hi).
func (l *List) Range(lo, hi int) [][]byte {
	out := make([][]byte, 0, max(hi-lo, 0))
	for i := lo; i < hi; i++ {
		out = append(out, l.At(i))
	}
	return out
}

// Clone copies the list structure; element byte slices are shared (they are never mutated).
func (l *List) Clone() *List {
	return &List{
		head: append([][]byte(nil), l.head...),
		tail: append([][]byte(nil), l.tail...),
	}
}
