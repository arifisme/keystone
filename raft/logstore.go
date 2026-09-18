package raft

// LogStore is durable Raft state. Every method that writes must not return
// until the write is stable, because the node answers RPCs on that basis.
//
// Indexes are 1-based and contiguous. After Compact(upTo, snap),
// FirstIndex() == upTo+1 and Term(upTo) still answers with the snapshot's
// term, so a leader can build prevLogTerm for the first live entry.
type LogStore interface {
	// Append writes entries, which must be contiguous and start at or
	// before LastIndex()+1. Any existing entries at or after the first
	// new index are discarded first.
	Append(entries []Entry) error
	// Entries returns entries in [lo, hi).
	Entries(lo, hi uint64) ([]Entry, error)
	Term(index uint64) (uint64, error)
	FirstIndex() uint64
	LastIndex() uint64
	SetState(term uint64, votedFor NodeID) error
	State() (term uint64, votedFor NodeID, err error)
	// Compact discards entries up to and including upTo, retaining
	// snapshot as the state at that index.
	Compact(upTo uint64, snapshot []byte) error
	// Snapshot returns the most recent compaction point. Index is zero when
	// the log has never been compacted.
	Snapshot() (index, term uint64, data []byte, err error)
}
