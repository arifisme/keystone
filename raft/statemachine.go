package raft

import "io"

// StateMachine consumes committed entries in index order. Apply's result is
// returned to whoever proposed the entry on this node.
type StateMachine interface {
	Apply(entry Entry) []byte
	Snapshot() (io.ReadCloser, error)
	Restore(io.Reader) error
}
