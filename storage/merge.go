package storage

import "container/heap"

// mergeIter yields the union of its children in internal key order. Equal
// keys come out in child order, so listing newer sources first keeps the
// newest version ahead.
type mergeIter struct {
	children []internalIter
	h        mergeHeap
	err      error
}

type mergeItem struct {
	it  internalIter
	idx int
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	c := compareInternalKey(h[i].it.key(), h[j].it.key())
	if c != 0 {
		return c < 0
	}
	return h[i].idx < h[j].idx
}
func (h mergeHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x interface{}) { *h = append(*h, x.(mergeItem)) }
func (h *mergeHeap) Pop() interface{} {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

func newMergeIter(children []internalIter) *mergeIter {
	return &mergeIter{children: children}
}

func (m *mergeIter) rebuild() {
	m.h = m.h[:0]
	for i, c := range m.children {
		if c.valid() {
			m.h = append(m.h, mergeItem{c, i})
		}
	}
	heap.Init(&m.h)
}

func (m *mergeIter) seekToFirst() {
	for _, c := range m.children {
		c.seekToFirst()
	}
	m.rebuild()
}

func (m *mergeIter) seek(target []byte) {
	for _, c := range m.children {
		c.seek(target)
	}
	m.rebuild()
}

func (m *mergeIter) valid() bool {
	return len(m.h) > 0 && m.err == nil
}

func (m *mergeIter) next() {
	top := m.h[0]
	top.it.next()
	if top.it.valid() {
		heap.Fix(&m.h, 0)
	} else {
		heap.Pop(&m.h)
	}
}

func (m *mergeIter) key() []byte   { return m.h[0].it.key() }
func (m *mergeIter) value() []byte { return m.h[0].it.value() }

func (m *mergeIter) close() error {
	for _, c := range m.children {
		if err := c.close(); err != nil && m.err == nil {
			m.err = err
		}
	}
	return m.err
}
