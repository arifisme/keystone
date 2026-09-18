package sim

import (
	"container/heap"
	"time"

	"github.com/arifisme/keystone/raft"
)

type eventKind uint8

const (
	evDeliver eventKind = iota
	evTick
	evCrash
	evRestart
	evPartition
	evHeal
	evClient
	evClientTimeout
	evChaos
)

func (k eventKind) String() string {
	return [...]string{"deliver", "tick", "crash", "restart", "partition", "heal", "client", "timeout", "chaos"}[k]
}

type event struct {
	at   time.Duration
	seq  uint64
	kind eventKind

	node    raft.NodeID
	gen     uint64
	msg     raft.Message
	client  int
	attempt uint64
	pairs   [][2]raft.NodeID
}

// eventHeap orders by time, then by insertion, so equal times resolve the
// same way on every run.
type eventHeap []*event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x interface{}) { *h = append(*h, x.(*event)) }
func (h *eventHeap) Pop() interface{} {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}

func (s *Sim) schedule(after time.Duration, e *event) {
	e.at = s.now + after
	s.seq++
	e.seq = s.seq
	heap.Push(&s.queue, e)
}
