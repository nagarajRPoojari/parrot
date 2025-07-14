package compactor

import (
	"fmt"

	"github.com/nagarajRPoojari/lsm/storage/types"
)

type Pair[K types.Key, V types.Value] struct {
	pl *types.Payload[K, V]
	I  int
	J  int
}

type MergerHeap[K types.Key, V types.Value] struct {
	h []Pair[K, V]
}

func (h *MergerHeap[K, V]) Len() int {
	return len(h.h)
}

func (h *MergerHeap[K, V]) Less(i, j int) bool {
	fmt.Printf("Less() = %v\n", h.h[j].pl)
	return h.h[i].pl.Key.Less(h.h[j].pl.Key)
}

func (h *MergerHeap[K, V]) Swap(i, j int) {
	h.h[i], h.h[j] = h.h[j], h.h[i]
}

func (h *MergerHeap[K, V]) Push(x any) {
	pl := x.(Pair[K, V])
	h.h = append(h.h, pl)
}

func (h *MergerHeap[K, V]) Pop() any {
	n := len(h.h)
	x := h.h[n-1]
	h.h = h.h[:n-1]
	return x
}
