package sim

import (
	"hash/fnv"
	"io"
	"math/rand"
	"time"

	"github.com/arifisme/keystone/kv"
	"github.com/arifisme/keystone/raft"
	"github.com/arifisme/keystone/storage"
)

type node struct {
	id    raft.NodeID
	store *logStore
	tr    *transport
	clock *clock

	up    bool
	gen   uint64
	rn    *raft.Node
	db    storage.DB
	sm    *stateMachine
	timer chan time.Time
}

// stateMachine wraps the real key-value state machine to report what is
// applied, so the simulator can compare nodes.
type stateMachine struct {
	inner *kv.StateMachine
	s     *Sim
	node  *node
}

type fingerprint struct {
	term uint64
	hash uint64
}

func fingerprintOf(e raft.Entry) fingerprint {
	h := fnv.New64a()
	h.Write(e.Data)
	return fingerprint{term: e.Term, hash: h.Sum64()}
}

func (m *stateMachine) Apply(e raft.Entry) []byte {
	m.s.inv.applied(m.s, m.node, e)
	return m.inner.Apply(e)
}

func (m *stateMachine) LastApplied() uint64 { return m.inner.LastApplied() }

func (m *stateMachine) Snapshot() (io.ReadCloser, error) {
	m.s.stats.Snapshots++
	return m.inner.Snapshot()
}

func (m *stateMachine) Restore(r io.Reader) error {
	if err := m.inner.Restore(r); err != nil {
		return err
	}
	m.s.inv.restored(m.node, m.inner.LastApplied())
	return nil
}

func (s *Sim) start(n *node) {
	n.gen++
	n.store.replay()
	s.inv.checkRestart(s, n)
	n.db = storage.NewMemory(s.rng.Uint64())
	inner, err := kv.NewStateMachine(n.db)
	if err != nil {
		panic(err)
	}
	n.sm = &stateMachine{inner: inner, s: s, node: n}
	rn, err := raft.NewNode(raft.NodeConfig{
		Config: raft.Config{
			ID:            n.id,
			Peers:         s.ids,
			ElectionTick:  electionTick,
			HeartbeatTick: 1,
			SnapshotChunk: snapshotChunk,
			Store:         n.store,
			Transport:     n.tr,
			Rand:          rand.New(rand.NewSource(s.rng.Int63())),
		},
		StateMachine:      n.sm,
		Clock:             n.clock,
		TickInterval:      tickInterval,
		SnapshotThreshold: snapshotThreshold,
	})
	if err != nil {
		panic(err)
	}
	n.rn = rn
	n.up = true
	n.clock.After(s.jitteredTick())
}

func (s *Sim) crash(n *node) {
	if !n.up {
		return
	}
	s.stats.Crashes++
	n.up = false
	n.rn = nil
	n.sm = nil
	n.db.Close()
	n.db = nil
	for _, c := range s.clients {
		if c.op != nil && c.op.node == n {
			s.abandon(c)
			s.schedule(retryDelay, &event{kind: evClient, client: c.id})
		}
	}
}

func (s *Sim) tick(e *event) {
	n := s.nodes[e.node-1]
	if !n.up || n.gen != e.gen {
		return
	}
	select {
	case n.timer <- n.clock.Now():
	default:
	}
	s.run(n, func() {
		<-n.timer
		n.rn.Tick()
	})
	if n.up {
		n.clock.After(s.jitteredTick())
	}
}

func (s *Sim) jitteredTick() time.Duration {
	j := s.faults.clockJitter
	f := 1 + (s.rng.Float64()*2-1)*j
	return time.Duration(float64(tickInterval) * f)
}

// run executes one step of a node: a message, a tick, or a flush. The
// store is checkpointed first and outgoing messages are held. At the
// highest chaos level the step may be cut short by a crash, which keeps
// a random prefix of its writes and none of its messages.
func (s *Sim) run(n *node, step func()) {
	n.store.checkpoint()
	step()
	if s.now < s.cfg.Faults && n.store.dirty() && s.rng.Float64() < s.faults.tornTail {
		s.stats.TornTails++
		n.store.rollback(s.rng)
		s.outbox = s.outbox[:0]
		s.crash(n)
		s.schedule(s.restartDelay(), &event{kind: evRestart, node: n.id})
		return
	}
	n.store.commit()
	for _, o := range s.outbox {
		s.net.send(o)
	}
	s.outbox = s.outbox[:0]
}
