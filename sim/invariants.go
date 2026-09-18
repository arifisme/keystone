package sim

import (
	"fmt"
	"strings"

	"github.com/arifisme/keystone/raft"
)

// invariants are checked after every event. A violation panics with the
// seed and the last events, because the run is not worth continuing and
// the seed is all that is needed to see it again.
type invariants struct {
	leaders    map[groupTerm]raft.NodeID
	lastTerm   map[raft.NodeID]uint64
	committed  map[uint64][]fingerprint
	checked    map[raft.NodeID]uint64
	appliedLog map[uint64][]fingerprint
	lastApply  map[raft.NodeID]uint64
}

type groupTerm struct {
	group uint64
	term  uint64
}

func newInvariants() *invariants {
	return &invariants{
		leaders:    map[groupTerm]raft.NodeID{},
		lastTerm:   map[raft.NodeID]uint64{},
		committed:  map[uint64][]fingerprint{},
		checked:    map[raft.NodeID]uint64{},
		appliedLog: map[uint64][]fingerprint{},
		lastApply:  map[raft.NodeID]uint64{},
	}
}

func (s *Sim) violation(format string, args ...interface{}) {
	panic(fmt.Sprintf("seed %d: invariant violated: %s\n%s", s.cfg.Seed, fmt.Sprintf(format, args...), s.trace.String()))
}

func (inv *invariants) check(s *Sim) {
	for _, n := range s.nodes {
		if !n.up {
			continue
		}
		st := n.rn.Status()
		if st.Term < inv.lastTerm[n.id] {
			s.violation("node %d term went from %d to %d", n.id, inv.lastTerm[n.id], st.Term)
		}
		inv.lastTerm[n.id] = st.Term
		if st.State == raft.Leader {
			key := groupTerm{n.group.id, st.Term}
			if other, ok := inv.leaders[key]; ok && other != n.id {
				s.violation("group %d term %d has leaders %d and %d", n.group.id, st.Term, other, n.id)
			}
			inv.leaders[key] = n.id
		}
		from := inv.checked[n.id] + 1
		if from <= n.store.snapIdx {
			from = n.store.snapIdx + 1
		}
		log := inv.committed[n.group.id]
		for i := from; i <= st.Commit; i++ {
			fp := fingerprintOf(n.store.entries[i-n.store.snapIdx-1])
			inv.record(s, &log, i, fp, "committed", n.id)
		}
		inv.committed[n.group.id] = log
		if st.Commit > inv.checked[n.id] {
			inv.checked[n.id] = st.Commit
		}
	}
}

func (inv *invariants) record(s *Sim, log *[]fingerprint, index uint64, fp fingerprint, what string, id raft.NodeID) {
	for uint64(len(*log)) < index {
		*log = append(*log, fingerprint{})
	}
	cur := &(*log)[index-1]
	if *cur == (fingerprint{}) {
		*cur = fp
		return
	}
	if *cur != fp {
		s.violation("node %d %s entry %d (term %d) differs from what another node %s there (term %d)", id, what, index, fp.term, what, cur.term)
	}
}

// applied checks that a node applies entries in order, skipping only the
// no-ops that never reach the state machine.
func (inv *invariants) applied(s *Sim, n *node, e raft.Entry) {
	last := inv.lastApply[n.id]
	if e.Index <= last {
		s.violation("node %d applied %d after %d", n.id, e.Index, last)
	}
	for i := last + 1; i < e.Index; i++ {
		if i <= n.store.snapIdx {
			continue
		}
		if skipped := n.store.entries[i-n.store.snapIdx-1]; skipped.Type != raft.EntryNoop {
			s.violation("node %d applied %d without applying %d", n.id, e.Index, i)
		}
	}
	inv.lastApply[n.id] = e.Index
	log := inv.appliedLog[n.group.id]
	inv.record(s, &log, e.Index, fingerprintOf(e), "applied", n.id)
	inv.appliedLog[n.group.id] = log
}

func (inv *invariants) restored(n *node, index uint64) {
	inv.lastApply[n.id] = index
}

// checkRestart runs after the journal is replayed: everything this node
// once reported committed must still be in its log or under its
// snapshot.
func (inv *invariants) checkRestart(s *Sim, n *node) {
	inv.lastApply[n.id] = 0
	if c := inv.checked[n.id]; c > n.store.last() && c > n.store.snapIdx {
		s.violation("node %d restarted with last index %d below its committed %d", n.id, n.store.last(), c)
	}
}

// trace keeps the most recent events for failure reports.
type trace struct {
	entries []string
	next    int
	full    bool
}

const traceSize = 200

func newTrace() *trace {
	return &trace{entries: make([]string, traceSize)}
}

func (t *trace) add(s string) {
	t.entries[t.next] = s
	t.next = (t.next + 1) % len(t.entries)
	if t.next == 0 {
		t.full = true
	}
}

func (t *trace) String() string {
	var b strings.Builder
	b.WriteString("last events:\n")
	start := 0
	if t.full {
		start = t.next
	}
	for i := 0; i < len(t.entries); i++ {
		idx := (start + i) % len(t.entries)
		if !t.full && idx >= t.next {
			break
		}
		b.WriteString("  ")
		b.WriteString(t.entries[idx])
		b.WriteByte('\n')
	}
	return b.String()
}

func describe(e *event) string {
	switch e.kind {
	case evDeliver:
		m := e.msg
		return fmt.Sprintf("%v %s %d->%d term=%d idx=%d n=%d commit=%d reject=%v", e.at, m.Type, m.From, m.To, m.Term, m.Index, len(m.Entries), m.Commit, m.Reject)
	case evTick, evCrash, evRestart:
		return fmt.Sprintf("%v %s node=%d", e.at, e.kind, e.node)
	case evPartition, evHeal:
		return fmt.Sprintf("%v %s pairs=%v", e.at, e.kind, e.pairs)
	case evClient, evClientTimeout:
		return fmt.Sprintf("%v %s client=%d attempt=%d", e.at, e.kind, e.client, e.attempt)
	}
	return fmt.Sprintf("%v %s", e.at, e.kind)
}
