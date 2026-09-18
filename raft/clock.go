package raft

import "time"

// Clock is the only source of time. The simulator substitutes a clock that
// advances under its control.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}
