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
	"github.com/arifisme/keystone/shard"
)

const (
	tickInterval      = 10 * time.Millisecond
	electionTick      = 10
	snapshotThreshold = 40
	snapshotChunk     = 128
)

type Config struct {
	Seed int64
	// Groups is the number of Raft groups; Nodes is the size of each. With
	// more than one group, group 1 issues sessions and the rest serve
	// hash ranges of the key space, as in a sharded deployment.
	Groups  int
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
	groups  []*group
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
	if cfg.Groups == 0 {
		cfg.Groups = 1
	}
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
	for gi := 1; gi <= cfg.Groups; gi++ {
		g := &group{id: uint64(gi)}
		for i := 0; i < cfg.Nodes; i++ {
			id := raft.NodeID(len(s.nodes) + 1)
			s.ids = append(s.ids, id)
			g.ids = append(g.ids, id)
			n := &node{id: id, group: g, store: &logStore{}}
			n.tr = &transport{s: s, node: n}
			n.clock = &clock{s: s, node: n}
			s.nodes = append(s.nodes, n)
			g.nodes = append(g.nodes, n)
		}
		s.groups = append(s.groups, g)
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
	s.schedule(s.cfg.Faults, &event{kind: evQuiet})
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
	case evQuiet:
		s.quiet()
	}
}

func (s *Sim) statusLine() string {
	var b bytes.Buffer
	for _, n := range s.nodes {
		if !n.up {
			fmt.Fprintf(&b, "[%d down] ", n.id)
			continue
		}
		st := n.rn.Status()
		fmt.Fprintf(&b, "[%d %s t%d lead=%d c%d a%d first=%d last=%d] ", n.id, st.State, st.Term, st.Leader, st.Commit, st.Applied, st.FirstIndex, st.LastIndex)
	}
	return b.String()
}

func (s *Sim) verify() error {
	for _, n := range s.nodes {
		if !n.up {
			return fmt.Errorf("seed %d: node %d still down at the end", s.cfg.Seed, n.id)
		}
	}
	for _, g := range s.groups {
		if err := s.verifyGroup(g); err != nil {
			return err
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

func (s *Sim) verifyGroup(g *group) error {
	lead := s.leaderOf(g)
	if lead == nil {
		return fmt.Errorf("seed %d: group %d has no leader after the quiet period: %s", s.cfg.Seed, g.id, s.statusLine())
	}
	commit := lead.rn.Status().Commit
	var want []byte
	for _, n := range g.nodes {
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
			return fmt.Errorf("seed %d: node %d state differs from node %d", s.cfg.Seed, n.id, g.nodes[0].id)
		}
	}
	return nil
}

// groupFor routes a key the way the real router does: the meta group
// issues sessions, the others each own a set of hash ranges.
func (s *Sim) groupFor(key string) *group {
	if len(s.groups) == 1 {
		return s.groups[0]
	}
	shard := shard.ShardOf([]byte(key))
	return s.groups[1+shard%(len(s.groups)-1)]
}
