package sim

import (
	"time"

	"github.com/arifisme/keystone/raft"
)

// network decides the fate of every message: dropped, duplicated,
// delayed, or cut by a partition. Partitions are directed pairs; a
// symmetric partition installs both directions.
type network struct {
	s        *Sim
	drop     float64
	dup      float64
	minDelay time.Duration
	maxDelay time.Duration
	cuts     map[[2]raft.NodeID]int
}

type outMsg struct {
	to  raft.NodeID
	msg raft.Message
}

// transport is what a node sends through. Messages sent during a step
// are held until the step completes, so a crash mid-step loses them the
// way a real crash would.
type transport struct {
	s *Sim
}

func (t *transport) Send(to raft.NodeID, m raft.Message) {
	t.s.outbox = append(t.s.outbox, outMsg{to: to, msg: m})
}

func (t *transport) Recv() <-chan raft.Message {
	return nil
}

func (n *network) send(o outMsg) {
	s := n.s
	s.stats.Messages++
	if n.cuts[[2]raft.NodeID{o.msg.From, o.to}] > 0 || s.rng.Float64() < n.drop {
		s.stats.Dropped++
		return
	}
	s.schedule(n.delay(), &event{kind: evDeliver, node: o.to, msg: o.msg})
	if s.rng.Float64() < n.dup {
		s.stats.Duplicated++
		s.schedule(n.delay(), &event{kind: evDeliver, node: o.to, msg: o.msg})
	}
}

func (n *network) delay() time.Duration {
	return n.minDelay + time.Duration(n.s.rng.Int63n(int64(n.maxDelay-n.minDelay)+1))
}

func (n *network) deliver(e *event) {
	s := n.s
	if n.cuts[[2]raft.NodeID{e.msg.From, e.node}] > 0 {
		s.stats.Dropped++
		return
	}
	nd := s.nodes[e.node-1]
	if !nd.up {
		s.stats.Dropped++
		return
	}
	s.run(nd, func() { nd.rn.Step(e.msg) })
}

func (n *network) cut(pairs [][2]raft.NodeID) {
	for _, p := range pairs {
		n.cuts[p]++
	}
}

func (n *network) heal(pairs [][2]raft.NodeID) {
	for _, p := range pairs {
		n.cuts[p]--
	}
}
