package sim

import (
	"fmt"
	"math/rand"

	"github.com/arifisme/keystone/raft"
)

// logStore is a raft.LogStore that models durability. Every write is a
// record in a journal, and the in-memory state is what replaying the
// journal yields, exactly like the disk store. Before each step the
// journal position is marked; a crash injected mid-step truncates the
// journal at a random point after the mark, which is a torn tail, and
// restart replays what is left.
type logStore struct {
	journal []record
	mark    int
	rewrite bool

	term     uint64
	vote     raft.NodeID
	entries  []raft.Entry
	snapIdx  uint64
	snapTerm uint64
	snap     []byte
}

type recordKind uint8

const (
	recEntry recordKind = iota
	recState
	recSnapshot
)

type record struct {
	kind  recordKind
	entry raft.Entry
	term  uint64
	vote  raft.NodeID
	index uint64
	snap  []byte
}

func (l *logStore) last() uint64 {
	return l.snapIdx + uint64(len(l.entries))
}

func (l *logStore) Append(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	first := entries[0].Index
	if first > l.last()+1 || first <= l.snapIdx {
		return fmt.Errorf("sim: append at %d outside (%d, %d]", first, l.snapIdx, l.last()+1)
	}
	for _, e := range entries {
		l.journal = append(l.journal, record{kind: recEntry, entry: e})
		l.apply(l.journal[len(l.journal)-1])
	}
	return nil
}

func (l *logStore) Entries(lo, hi uint64) ([]raft.Entry, error) {
	if lo <= l.snapIdx {
		return nil, raft.ErrCompacted
	}
	if hi > l.last()+1 {
		return nil, fmt.Errorf("sim: entries [%d,%d) beyond last %d", lo, hi, l.last())
	}
	out := make([]raft.Entry, hi-lo)
	copy(out, l.entries[lo-l.snapIdx-1:hi-l.snapIdx-1])
	return out, nil
}

func (l *logStore) Term(index uint64) (uint64, error) {
	if index == l.snapIdx {
		return l.snapTerm, nil
	}
	if index < l.snapIdx {
		return 0, raft.ErrCompacted
	}
	if index > l.last() {
		return 0, fmt.Errorf("sim: term of %d beyond last %d", index, l.last())
	}
	return l.entries[index-l.snapIdx-1].Term, nil
}

func (l *logStore) FirstIndex() uint64 { return l.snapIdx + 1 }
func (l *logStore) LastIndex() uint64  { return l.last() }

func (l *logStore) SetState(term uint64, votedFor raft.NodeID) error {
	l.journal = append(l.journal, record{kind: recState, term: term, vote: votedFor})
	l.apply(l.journal[len(l.journal)-1])
	return nil
}

func (l *logStore) State() (uint64, raft.NodeID, error) {
	return l.term, l.vote, nil
}

func (l *logStore) Compact(index, term uint64, snapshot []byte) error {
	if index <= l.snapIdx {
		return nil
	}
	l.journal = append(l.journal, record{kind: recSnapshot, index: index, term: term, snap: snapshot})
	l.apply(l.journal[len(l.journal)-1])
	l.rewrite = true
	return nil
}

func (l *logStore) Snapshot() (uint64, uint64, []byte, error) {
	return l.snapIdx, l.snapTerm, l.snap, nil
}

func (l *logStore) apply(r record) {
	switch r.kind {
	case recEntry:
		e := r.entry
		if e.Index <= l.snapIdx {
			return
		}
		if e.Index <= l.last() {
			l.entries = l.entries[:e.Index-l.snapIdx-1]
		}
		l.entries = append(l.entries, e)
	case recState:
		l.term, l.vote = r.term, r.vote
	case recSnapshot:
		if r.index >= l.last() {
			l.entries = nil
		} else {
			l.entries = append([]raft.Entry(nil), l.entries[r.index-l.snapIdx:]...)
		}
		l.snapIdx, l.snapTerm, l.snap = r.index, r.term, r.snap
	}
}

func (l *logStore) replay() {
	l.term, l.vote = 0, raft.None
	l.entries = nil
	l.snapIdx, l.snapTerm, l.snap = 0, 0, nil
	for _, r := range l.journal {
		l.apply(r)
	}
}

func (l *logStore) checkpoint() {
	l.mark = len(l.journal)
}

func (l *logStore) dirty() bool {
	return len(l.journal) > l.mark
}

// commit makes the step's writes permanent. After a compaction the
// journal is rewritten from state so it does not grow without bound.
func (l *logStore) commit() {
	if l.rewrite {
		l.rewrite = false
		j := make([]record, 0, len(l.entries)+2)
		j = append(j, record{kind: recSnapshot, index: l.snapIdx, term: l.snapTerm, snap: l.snap})
		j = append(j, record{kind: recState, term: l.term, vote: l.vote})
		for _, e := range l.entries {
			j = append(j, record{kind: recEntry, entry: e})
		}
		l.journal = j
	}
	l.mark = len(l.journal)
}

// rollback keeps a random prefix of the step's writes and rebuilds state.
func (l *logStore) rollback(rng *rand.Rand) {
	cut := l.mark + rng.Intn(len(l.journal)-l.mark)
	l.journal = l.journal[:cut]
	l.rewrite = false
	l.replay()
	l.mark = len(l.journal)
}
