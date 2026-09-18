package raft

import "io"

// StateMachine consumes committed entries in index order. Apply's result is
// returned to whoever proposed the entry on this node. LastApplied tells a
// restarting node where to resume; a state machine that keeps nothing
// across restarts returns zero and is restored from the latest snapshot.
type StateMachine interface {
	Apply(entry Entry) []byte
	LastApplied() uint64
	Snapshot() (io.ReadCloser, error)
	Restore(io.Reader) error
}
