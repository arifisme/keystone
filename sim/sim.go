// Package sim is a deterministic, single-process simulation of a Raft
// cluster with injected network, disk, and clock faults. Test-only.
package sim

import (
	"bytes"
	"container/heap"
	"fmt"
	"math/rand"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/arifisme/keystone/raft"
)

const (
	tickInterval      = 10 * time.Millisecond
	electionTick      = 10
	snapshotThreshold = 40
	snapshotChunk     = 128
)

type Config struct {
	Seed    int64
	Nodes   int
	Clients int
	Keys    int
	// Chaos selects the fault set: 0 delays only, 1 adds drops,
	// duplicates and partitions, 2 adds crashes, 3 adds leader kills, torn
	// tails and heavy clock jitter.
	Chaos int
	// Faults is how long faults are injected; Quiet is the fault-free
	// period afterwards in which the cluster must converge.
	Faults time.Duration
	Quiet  time.Duration
}

type Stats struct {
	Events      int
	Messages    int
	Dropped     int
	Duplicated  int
	Partitions  int
	Crashes     int
	LeaderKills int
	TornTails   int
	Snapshots   int
	Ops         int
	Retries     int
	Timeouts    int
	Terms       uint64
}

type Sim struct {
	cfg   Config
	rng   *rand.Rand
	now   time.Duration
	seq   uint64
	queue eventHeap

	ids     []raft.NodeID
	nodes   []*node
	net     *network
	clients []*client
	faults  faultParams
	outbox  []outMsg

	inv     *invariants
	trace   *trace
	history []porcupine.Operation
	stats   Stats
}

func New(cfg Config) *Sim {
	if cfg.Nodes == 0 {
		cfg.Nodes = 3
	}
	if cfg.Clients == 0 {
		cfg.Clients = 3
	}
	if cfg.Keys == 0 {
		cfg.Keys = 4
	}
	if cfg.Faults == 0 {
		cfg.Faults = 4 * time.Second
	}
	if cfg.Quiet == 0 {
		cfg.Quiet = 2 * time.Second
	}
	s := &Sim{cfg: cfg, rng: rand.New(rand.NewSource(cfg.Seed)), inv: newInvariants(), trace: newTrace()}
	s.net = &network{s: s, cuts: map[[2]raft.NodeID]int{}}
	for i := 1; i <= cfg.Nodes; i++ {
		id := raft.NodeID(i)
		s.ids = append(s.ids, id)
		n := &node{id: id, store: &logStore{}, tr: &transport{s: s}}
		n.clock = &clock{s: s, node: n}
		s.nodes = append(s.nodes, n)
	}
	for i := 0; i < cfg.Clients; i++ {
		s.clients = append(s.clients, &client{id: i})
	}
	s.drawFaults()
	return s
}

// Run drives the cluster to the end of the quiet period and returns an
// error if the history is not linearizable or the nodes did not converge.
// Invariant violations panic.
func (s *Sim) Run() (Stats, error) {
	for _, n := range s.nodes {
		s.start(n)
	}
	for _, c := range s.clients {
		s.schedule(time.Duration(s.rng.Int63n(int64(100*time.Millisecond))), &event{kind: evClient, client: c.id})
	}
	if s.cfg.Chaos > 0 {
		s.schedule(200*time.Millisecond, &event{kind: evChaos})
	}
	end := s.cfg.Faults + s.cfg.Quiet
	for len(s.queue) > 0 {
		e := heap.Pop(&s.queue).(*event)
		if e.at > end {
			break
		}
		s.now = e.at
		s.stats.Events++
		s.trace.add(describe(e))
		s.handle(e)
		s.settle()
		s.inv.check(s)
	}
	for _, t := range s.inv.lastTerm {
		if t > s.stats.Terms {
			s.stats.Terms = t
		}
	}
	return s.stats, s.verify()
}

func (s *Sim) handle(e *event) {
	switch e.kind {
	case evDeliver:
		s.net.deliver(e)
	case evTick:
		s.tick(e)
	case evCrash:
		s.crash(s.nodes[e.node-1])
	case evRestart:
		if n := s.nodes[e.node-1]; !n.up {
			s.start(n)
		}
	case evPartition:
		s.net.cut(e.pairs)
	case evHeal:
		s.net.heal(e.pairs)
	case evClient:
		s.clientEvent(e)
	case evClientTimeout:
		s.clientTimeout(e)
	case evChaos:
		s.chaos()
	}
}

func (s *Sim) verify() error {
	for _, n := range s.nodes {
		if !n.up {
			return fmt.Errorf("seed %d: node %d still down at the end", s.cfg.Seed, n.id)
		}
	}
	lead := s.leader()
	if lead == nil {
		return fmt.Errorf("seed %d: no leader after the quiet period", s.cfg.Seed)
	}
	commit := lead.rn.Status().Commit
	var want []byte
	for _, n := range s.nodes {
		st := n.rn.Status()
		if st.Applied != commit {
			return fmt.Errorf("seed %d: node %d applied %d, leader committed %d", s.cfg.Seed, n.id, st.Applied, commit)
		}
		var buf bytes.Buffer
		if err := n.db.Export(&buf); err != nil {
			return err
		}
		if want == nil {
			want = buf.Bytes()
		} else if !bytes.Equal(want, buf.Bytes()) {
			return fmt.Errorf("seed %d: node %d state differs from node %d", s.cfg.Seed, n.id, s.nodes[0].id)
		}
	}
	for _, c := range s.clients {
		if c.op != nil {
			return fmt.Errorf("seed %d: client %d never finished %s (attempt %d)", s.cfg.Seed, c.id, c.op.kind, c.op.attempt)
		}
	}
	if !porcupine.CheckOperations(kvModel, s.history) {
		return fmt.Errorf("seed %d: history of %d operations is not linearizable", s.cfg.Seed, len(s.history))
	}
	return nil
}
