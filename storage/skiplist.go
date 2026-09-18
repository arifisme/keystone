package storage

import "sync/atomic"

const (
	maxHeight = 12
	// branching of 4 gives the classic p = 1/4 level distribution.
	branching = 4
)

type node struct {
	key   []byte
	value []byte
	next  []atomic.Pointer[node]
}

// skiplist holds internal keys in compareInternalKey order. One goroutine
// inserts at a time; any number read concurrently without locks, because a
// node is fully built before its predecessors publish it with atomic stores
// and nodes are never removed or modified afterwards.
type skiplist struct {
	head   *node
	height atomic.Int32
	seed   uint64
}

func newSkiplist(seed uint64) *skiplist {
	if seed == 0 {
		seed = 1
	}
	s := &skiplist{head: &node{next: make([]atomic.Pointer[node], maxHeight)}, seed: seed}
	s.height.Store(1)
	return s
}

func (s *skiplist) randomHeight() int {
	h := 1
	for h < maxHeight {
		s.seed ^= s.seed << 13
		s.seed ^= s.seed >> 7
		s.seed ^= s.seed << 17
		if s.seed%branching != 0 {
			break
		}
		h++
	}
	return h
}

func (s *skiplist) insert(key, value []byte) {
	var prev [maxHeight]*node
	x := s.head
	for level := int(s.height.Load()) - 1; level >= 0; level-- {
		for {
			next := x.next[level].Load()
			if next == nil || compareInternalKey(next.key, key) >= 0 {
				break
			}
			x = next
		}
		prev[level] = x
	}
	h := s.randomHeight()
	if cur := int(s.height.Load()); h > cur {
		for level := cur; level < h; level++ {
			prev[level] = s.head
		}
		s.height.Store(int32(h))
	}
	n := &node{key: key, value: value, next: make([]atomic.Pointer[node], h)}
	for level := 0; level < h; level++ {
		n.next[level].Store(prev[level].next[level].Load())
	}
	for level := 0; level < h; level++ {
		prev[level].next[level].Store(n)
	}
}

// seek returns the first node with key >= target, or nil.
func (s *skiplist) seek(target []byte) *node {
	x := s.head
	for level := int(s.height.Load()) - 1; level >= 0; level-- {
		for {
			next := x.next[level].Load()
			if next == nil || compareInternalKey(next.key, target) >= 0 {
				break
			}
			x = next
		}
	}
	return x.next[0].Load()
}

func (s *skiplist) first() *node {
	return s.head.next[0].Load()
}

type skiplistIter struct {
	list *skiplist
	cur  *node
}

func (it *skiplistIter) seekToFirst()    { it.cur = it.list.first() }
func (it *skiplistIter) seek(key []byte) { it.cur = it.list.seek(key) }
func (it *skiplistIter) valid() bool     { return it.cur != nil }
func (it *skiplistIter) next()           { it.cur = it.cur.next[0].Load() }
func (it *skiplistIter) key() []byte     { return it.cur.key }
func (it *skiplistIter) value() []byte   { return it.cur.value }
func (it *skiplistIter) close() error    { return nil }
