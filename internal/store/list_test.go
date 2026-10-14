package store

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// TestListMatchesSliceModel drives the deque and a plain slice with the same random
// operations and checks they always agree.
func TestListMatchesSliceModel(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	l := NewList()
	var model [][]byte
	for i := range 20_000 {
		v := []byte{byte(i), byte(i >> 8)}
		switch op := r.IntN(4); {
		case op == 0:
			l.PushFront(v)
			model = append([][]byte{v}, model...)
		case op == 1:
			l.PushBack(v)
			model = append(model, v)
		case op == 2 && len(model) > 0:
			if got := l.PopFront(); !slices.Equal(got, model[0]) {
				t.Fatalf("step %d: PopFront = %v, want %v", i, got, model[0])
			}
			model = model[1:]
		case op == 3 && len(model) > 0:
			if got := l.PopBack(); !slices.Equal(got, model[len(model)-1]) {
				t.Fatalf("step %d: PopBack mismatch", i)
			}
			model = model[:len(model)-1]
		}
		if l.Len() != len(model) {
			t.Fatalf("step %d: Len = %d, want %d", i, l.Len(), len(model))
		}
	}
	got := l.Range(0, l.Len())
	if len(got) != len(model) {
		t.Fatal("length mismatch")
	}
	for i := range got {
		if !slices.Equal(got[i], model[i]) || !slices.Equal(l.At(i), model[i]) {
			t.Fatalf("element %d differs", i)
		}
	}
	clone := l.Clone()
	if l.Len() > 0 {
		l.PopFront()
		if clone.Len() != len(model) {
			t.Fatal("clone shares structure with the original")
		}
	}
}

func BenchmarkPushFront(b *testing.B) {
	l := NewList()
	v := []byte("x")
	for b.Loop() {
		l.PushFront(v)
	}
}
