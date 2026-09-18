package raft

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var errNoSnapshot = errors.New("snapshots not supported")

// hub is an in-memory network between nodes. It records which node sent
// AppendEntries in which term, which is exactly the set of nodes that
// believed themselves leader of that term.
type hub struct {
	mu      sync.Mutex
	inbox   map[NodeID]chan Message
	down    map[NodeID]bool
	leaders map[uint64]map[NodeID]bool
}

func newHub() *hub {
	return &hub{inbox: map[NodeID]chan Message{}, down: map[NodeID]bool{}, leaders: map[uint64]map[NodeID]bool{}}
}

type hubTransport struct {
	h  *hub
	id NodeID
}

func (t hubTransport) Send(to NodeID, m Message) {
	t.h.mu.Lock()
	defer t.h.mu.Unlock()
	if m.Type == MsgApp {
		if t.h.leaders[m.Term] == nil {
			t.h.leaders[m.Term] = map[NodeID]bool{}
		}
		t.h.leaders[m.Term][m.From] = true
	}
	if t.h.down[t.id] || t.h.down[to] {
		return
	}
	select {
	case t.h.inbox[to] <- m:
	default:
	}
}

func (t hubTransport) Recv() <-chan Message {
	t.h.mu.Lock()
	defer t.h.mu.Unlock()
	return t.h.inbox[t.id]
}

// recordingSM appends every applied entry so tests can compare nodes.
type recordingSM struct {
	mu      sync.Mutex
	applied []Entry
	last    uint64
}

func (s *recordingSM) Apply(e Entry) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = append(s.applied, e)
	s.last = e.Index
	return append([]byte("ok:"), e.Data...)
}

func (s *recordingSM) LastApplied() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

func (s *recordingSM) Snapshot() (io.ReadCloser, error) { return nil, errNoSnapshot }
func (s *recordingSM) Restore(io.Reader) error          { return errNoSnapshot }

func (s *recordingSM) entries() []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Entry(nil), s.applied...)
}

const (
	testTick         = 5 * time.Millisecond
	testElectionTick = 10
)

func electionTimeout() time.Duration {
	return time.Duration(testElectionTick) * testTick
}

type cluster struct {
	t      *testing.T
	h      *hub
	ids    []NodeID
	stores map[NodeID]LogStore
	dirs   map[NodeID]string
	sms    map[NodeID]*recordingSM
	nodes  map[NodeID]*Node
	seed   int64
	snap   uint64
}

func newCluster(t *testing.T, n int) *cluster {
	return newClusterWith(t, n, false)
}

// newClusterWith builds n nodes; with disk set, each node persists to a
// DiskStore in its own directory and a restart reopens it, as a real
// process would. Killing a node always wipes its state machine.
func newClusterWith(t *testing.T, n int, disk bool) *cluster {
	t.Helper()
	c := &cluster{t: t, h: newHub(), stores: map[NodeID]LogStore{}, dirs: map[NodeID]string{}, sms: map[NodeID]*recordingSM{}, nodes: map[NodeID]*Node{}, seed: time.Now().UnixNano()}
	for i := 1; i <= n; i++ {
		id := NodeID(i)
		c.ids = append(c.ids, id)
		if disk {
			c.dirs[id] = t.TempDir()
		} else {
			c.stores[id] = NewMemStore()
		}
	}
	for _, id := range c.ids {
		c.start(id)
	}
	return c
}

func (c *cluster) start(id NodeID) {
	c.h.mu.Lock()
	c.h.inbox[id] = make(chan Message, 1024)
	c.h.down[id] = false
	c.h.mu.Unlock()
	if dir, ok := c.dirs[id]; ok {
		s, err := OpenDiskStore(dir)
		if err != nil {
			c.t.Fatal(err)
		}
		c.stores[id] = s
	}
	c.sms[id] = &recordingSM{}
	node, err := NewNode(NodeConfig{
		Config: Config{
			ID:            id,
			Peers:         c.ids,
			ElectionTick:  testElectionTick,
			HeartbeatTick: 1,
			Store:         c.stores[id],
			Transport:     hubTransport{c.h, id},
			Rand:          rand.New(rand.NewSource(c.seed + int64(id))),
		},
		StateMachine:      c.sms[id],
		Clock:             SystemClock{},
		TickInterval:      testTick,
		SnapshotThreshold: c.snap,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = node
	go node.Run()
}

func (c *cluster) kill(id NodeID) {
	c.h.mu.Lock()
	c.h.down[id] = true
	c.h.mu.Unlock()
	c.nodes[id].Stop()
	delete(c.nodes, id)
	if s, ok := c.stores[id].(*DiskStore); ok {
		s.Close()
	}
}

func (c *cluster) stop() {
	for id := range c.nodes {
		c.kill(id)
	}
}

func (c *cluster) leader() *Node {
	for _, n := range c.nodes {
		if n.Status().State == Leader {
			return n
		}
	}
	return nil
}

func (c *cluster) waitForLeader(within time.Duration) *Node {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if l := c.leader(); l != nil {
			return l
		}
		time.Sleep(time.Millisecond)
	}
	c.t.Fatalf("no leader within %v (seed %d): %s", within, c.seed, statusLine(c))
	return nil
}

func (c *cluster) checkOneLeaderPerTerm() {
	c.t.Helper()
	c.h.mu.Lock()
	defer c.h.mu.Unlock()
	for term, who := range c.h.leaders {
		if len(who) > 1 {
			c.t.Fatalf("term %d had %d leaders: %v (seed %d)", term, len(who), who, c.seed)
		}
	}
}

func TestElectionElectsExactlyOneLeaderPerTermAcrossFiftyLeaderKills(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()
	for i := 0; i < 50; i++ {
		l := c.waitForLeader(5 * electionTimeout())
		id := l.Status().ID
		c.kill(id)
		c.waitForLeader(5 * electionTimeout())
		c.start(id)
	}
	c.checkOneLeaderPerTerm()
	terms := 0
	c.h.mu.Lock()
	terms = len(c.h.leaders)
	c.h.mu.Unlock()
	if terms < 50 {
		t.Fatalf("only %d terms had a leader after 50 kills", terms)
	}
}

func TestSingleNodeBecomesLeaderAlone(t *testing.T) {
	c := newCluster(t, 1)
	defer c.stop()
	c.waitForLeader(3 * electionTimeout())
}

func TestMinorityCannotElectLeader(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()
	c.waitForLeader(5 * electionTimeout())
	for _, id := range c.ids[:3] {
		c.kill(id)
	}
	time.Sleep(6 * electionTimeout())
	for _, n := range c.nodes {
		if s := n.Status(); s.State == Leader {
			t.Fatalf("node %d became leader with two of five nodes alive", s.ID)
		}
	}
}

func statusLine(c *cluster) string {
	var out string
	for _, id := range c.ids {
		if n, ok := c.nodes[id]; ok {
			s := n.Status()
			out += fmt.Sprintf("[%d %s t%d c%d a%d] ", s.ID, s.State, s.Term, s.Commit, s.Applied)
		}
	}
	return out
}
