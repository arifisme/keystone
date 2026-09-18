package raft

import (
	"errors"
	"fmt"
	"sync"
)

var ErrCompacted = errors.New("raft: index compacted")

// MemStore is a LogStore that forgets everything when the process ends.
// It backs unit tests and the simulator's durability model.
type MemStore struct {
	mu       sync.Mutex
	term     uint64
	vote     NodeID
	entries  []Entry
	snapIdx  uint64
	snapTerm uint64
	snap     []byte
}

func NewMemStore() *MemStore {
	return &MemStore{}
}

func (s *MemStore) Append(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLocked(entries)
}

func (s *MemStore) appendLocked(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	first := entries[0].Index
	last := s.lastLocked()
	if first > last+1 || first <= s.snapIdx {
		return fmt.Errorf("raft: append at %d outside (%d, %d]", first, s.snapIdx, last+1)
	}
	s.entries = append(s.entries[:first-s.snapIdx-1], entries...)
	return nil
}

func (s *MemStore) Entries(lo, hi uint64) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lo <= s.snapIdx {
		return nil, ErrCompacted
	}
	if hi > s.lastLocked()+1 {
		return nil, fmt.Errorf("raft: entries [%d,%d) beyond last %d", lo, hi, s.lastLocked())
	}
	out := make([]Entry, hi-lo)
	copy(out, s.entries[lo-s.snapIdx-1:hi-s.snapIdx-1])
	return out, nil
}

func (s *MemStore) Term(index uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index == s.snapIdx {
		return s.snapTerm, nil
	}
	if index < s.snapIdx {
		return 0, ErrCompacted
	}
	if index > s.lastLocked() {
		return 0, fmt.Errorf("raft: term of %d beyond last %d", index, s.lastLocked())
	}
	return s.entries[index-s.snapIdx-1].Term, nil
}

func (s *MemStore) FirstIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapIdx + 1
}

func (s *MemStore) LastIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastLocked()
}

func (s *MemStore) lastLocked() uint64 {
	return s.snapIdx + uint64(len(s.entries))
}

func (s *MemStore) SetState(term uint64, votedFor NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term, s.vote = term, votedFor
	return nil
}

func (s *MemStore) State() (uint64, NodeID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term, s.vote, nil
}

func (s *MemStore) Compact(index, term uint64, snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index <= s.snapIdx {
		return nil
	}
	if index <= s.lastLocked() {
		s.entries = append([]Entry(nil), s.entries[index-s.snapIdx:]...)
	} else {
		s.entries = nil
	}
	s.snapIdx, s.snapTerm, s.snap = index, term, snapshot
	return nil
}

func (s *MemStore) Snapshot() (uint64, uint64, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapIdx, s.snapTerm, s.snap, nil
}
