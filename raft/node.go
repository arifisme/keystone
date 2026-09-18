package raft

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"
)

type NodeConfig struct {
	Config
	StateMachine StateMachine
	Clock        Clock
	TickInterval time.Duration
	// SnapshotThreshold is how many applied entries the log may hold
	// before it is compacted into a snapshot. Zero disables snapshots.
	SnapshotThreshold uint64
}

// Proposal is one client entry on its way through the log. Done closes
// once the entry has been applied on this node or the attempt failed.
type Proposal struct {
	data   []byte
	term   uint64
	Index  uint64
	Result []byte
	Err    error
	done   chan struct{}
}

func (p *Proposal) Done() <-chan struct{} { return p.done }

func (p *Proposal) finish(result []byte, err error) {
	p.Result, p.Err = result, err
	close(p.done)
}

// applyBatch bounds how many entries are read from the store per pass.
const applyBatch = 256

// Node drives a Raft over its state machine. Step, Tick and Flush are
// synchronous and safe for concurrent use; Run calls them from one
// goroutine in production, the simulator calls them directly.
type Node struct {
	mu      sync.Mutex
	r       *Raft
	sm      StateMachine
	store   LogStore
	tr      Transport
	clock   Clock
	tick    time.Duration
	snapGap uint64

	applied uint64
	queue   []*Proposal
	pending map[uint64]*Proposal

	wake chan struct{}
	stop chan struct{}
	done chan struct{}
}

func NewNode(cfg NodeConfig) (*Node, error) {
	r, err := New(cfg.Config)
	if err != nil {
		return nil, err
	}
	n := &Node{
		r:       r,
		sm:      cfg.StateMachine,
		store:   cfg.Store,
		tr:      cfg.Transport,
		clock:   cfg.Clock,
		tick:    cfg.TickInterval,
		snapGap: cfg.SnapshotThreshold,
		pending: make(map[uint64]*Proposal),
		wake:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	n.applied = n.sm.LastApplied()
	snapIdx, _, data, err := n.store.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("raft: load snapshot: %w", err)
	}
	// A state machine behind the compaction point can only be rebuilt
	// from the snapshot; the entries it is missing are gone.
	if n.applied < snapIdx {
		if err := n.sm.Restore(bytes.NewReader(data)); err != nil {
			return nil, fmt.Errorf("raft: restore snapshot: %w", err)
		}
		n.applied = snapIdx
	}
	if n.applied > r.commit {
		r.commit = n.applied
	}
	return n, nil
}

func (n *Node) Run() {
	defer close(n.done)
	tick := n.clock.After(n.tick)
	for {
		select {
		case m := <-n.tr.Recv():
			n.Step(m)
		case <-tick:
			n.Tick()
			tick = n.clock.After(n.tick)
		case <-n.wake:
			n.Flush()
		case <-n.stop:
			return
		}
	}
}

// Stop ends Run and fails every outstanding proposal.
func (n *Node) Stop() {
	close(n.stop)
	<-n.done
	n.mu.Lock()
	defer n.mu.Unlock()
	n.failAll(ErrNotLeader)
}

func (n *Node) Step(m Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.r.Step(m)
	n.advance()
}

func (n *Node) Tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.r.Tick()
	n.advance()
}

// Propose queues data for the next Flush and returns a handle to wait on.
func (n *Node) Propose(data []byte) *Proposal {
	p := &Proposal{data: data, done: make(chan struct{})}
	n.mu.Lock()
	n.queue = append(n.queue, p)
	n.mu.Unlock()
	select {
	case n.wake <- struct{}{}:
	default:
	}
	return p
}

// Flush appends every queued proposal as one batch.
func (n *Node) Flush() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.queue) == 0 {
		return
	}
	queue := n.queue
	n.queue = nil
	if n.r.state != Leader {
		for _, p := range queue {
			p.finish(nil, ErrNotLeader)
		}
		return
	}
	payloads := make([][]byte, len(queue))
	for i, p := range queue {
		payloads[i] = p.data
	}
	first, err := n.r.Propose(payloads)
	if err != nil {
		for _, p := range queue {
			p.finish(nil, err)
		}
		return
	}
	for i, p := range queue {
		p.Index = first + uint64(i)
		p.term = n.r.term
		n.pending[p.Index] = p
	}
	n.advance()
}

// advance applies newly committed entries and settles proposals. Caller
// holds mu.
func (n *Node) advance() {
	if n.r.state != Leader {
		n.failAll(ErrNotLeader)
	}
	for n.applied < n.r.commit {
		hi := min(n.r.commit, n.applied+applyBatch)
		entries, err := n.store.Entries(n.applied+1, hi+1)
		if err != nil {
			panic(fmt.Sprintf("raft: read committed entries [%d,%d]: %v", n.applied+1, hi, err))
		}
		for _, e := range entries {
			var result []byte
			if e.Type == EntryNormal {
				result = n.sm.Apply(e)
			}
			n.applied = e.Index
			if p, ok := n.pending[e.Index]; ok {
				delete(n.pending, e.Index)
				if p.term == e.Term {
					p.finish(result, nil)
				} else {
					p.finish(nil, ErrNotLeader)
				}
			}
		}
	}
	n.maybeSnapshot()
}

func (n *Node) failAll(err error) {
	for _, p := range n.queue {
		p.finish(nil, err)
	}
	n.queue = nil
	for idx, p := range n.pending {
		delete(n.pending, idx)
		p.finish(nil, err)
	}
}

func (n *Node) maybeSnapshot() {
	if n.snapGap == 0 || n.applied+1-n.store.FirstIndex() < n.snapGap {
		return
	}
	rc, err := n.sm.Snapshot()
	if err != nil {
		panic(fmt.Sprintf("raft: snapshot: %v", err))
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		panic(fmt.Sprintf("raft: read snapshot: %v", err))
	}
	if err := n.store.Compact(n.applied, n.r.termAt(n.applied), data); err != nil {
		panic(fmt.Sprintf("raft: compact: %v", err))
	}
}

type Status struct {
	ID         NodeID
	Term       uint64
	State      State
	Leader     NodeID
	Commit     uint64
	Applied    uint64
	FirstIndex uint64
	LastIndex  uint64
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:         n.r.id,
		Term:       n.r.term,
		State:      n.r.state,
		Leader:     n.r.lead,
		Commit:     n.r.commit,
		Applied:    n.applied,
		FirstIndex: n.store.FirstIndex(),
		LastIndex:  n.store.LastIndex(),
	}
}
