package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/arifisme/keystone/storage"
)

// DiskStore is a LogStore whose records live in storage.WAL segments and
// whose snapshot lives in a file named by its index. The live tail of the
// log is also kept in memory: the WAL is written, never read, until the
// next open.
//
// WAL record payloads:
//
//	entry     0x01 | index uint64 | term uint64 | type uint8 | data
//	state     0x02 | term uint64 | votedFor uint64
//	snapshot  0x03 | index uint64 | term uint64
//
// An entry whose index is at or below the last one truncates the log
// before it is appended; that is the only truncation mechanism. A snapshot
// record marks that entries at or below its index are obsolete; the
// snapshot itself is in snap-<index>, written before the record.
const (
	recEntry    = 0x01
	recState    = 0x02
	recSnapshot = 0x03
)

type DiskStore struct {
	mu   sync.Mutex
	dir  string
	wal  *storage.WAL
	term uint64
	vote NodeID

	// entries[i] has index base+1+i. base equals snapIdx once recovery has
	// seen a snapshot record; before that it is one below the first
	// replayed entry, which may sit above zero if older segments were
	// already deleted.
	base     uint64
	entries  []Entry
	snapIdx  uint64
	snapTerm uint64
	segMax   map[uint64]uint64
}

func OpenDiskStore(dir string) (*DiskStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &DiskStore{dir: dir, segMax: map[uint64]uint64{}}
	wal, err := storage.OpenWAL(dir, storage.WALOptions{SegmentSize: 16 << 20}, s.replay)
	if err != nil {
		return nil, fmt.Errorf("raft: open log: %w", err)
	}
	if s.base != s.snapIdx {
		wal.Close()
		return nil, fmt.Errorf("raft: log starts at %d but snapshot is at %d", s.base+1, s.snapIdx)
	}
	s.wal = wal
	if err := s.removeStaleSnapshots(); err != nil {
		wal.Close()
		return nil, err
	}
	return s, nil
}

func (s *DiskStore) replay(seg uint64, p []byte) error {
	if len(p) == 0 {
		return errors.New("empty record")
	}
	switch p[0] {
	case recEntry:
		if len(p) < 18 {
			return errors.New("short entry record")
		}
		e := Entry{
			Index: binary.LittleEndian.Uint64(p[1:]),
			Term:  binary.LittleEndian.Uint64(p[9:]),
			Type:  EntryType(p[17]),
			Data:  append([]byte(nil), p[18:]...),
		}
		if e.Index <= s.snapIdx {
			return nil
		}
		last := s.lastLocked()
		switch {
		case len(s.entries) == 0 && s.base == s.snapIdx && s.snapIdx == 0 && e.Index > 1:
			s.base = e.Index - 1
		case e.Index <= last:
			s.entries = s.entries[:e.Index-s.base-1]
		case e.Index > last+1:
			return fmt.Errorf("entry %d follows %d", e.Index, last)
		}
		s.entries = append(s.entries, e)
		s.segMax[seg] = e.Index
	case recState:
		if len(p) < 17 {
			return errors.New("short state record")
		}
		s.term = binary.LittleEndian.Uint64(p[1:])
		s.vote = NodeID(binary.LittleEndian.Uint64(p[9:]))
	case recSnapshot:
		if len(p) < 17 {
			return errors.New("short snapshot record")
		}
		idx := binary.LittleEndian.Uint64(p[1:])
		term := binary.LittleEndian.Uint64(p[9:])
		if idx < s.base {
			return fmt.Errorf("snapshot %d below log start %d", idx, s.base+1)
		}
		s.dropThrough(idx)
		s.snapIdx, s.snapTerm = idx, term
	default:
		return fmt.Errorf("unknown record type %d", p[0])
	}
	return nil
}

// dropThrough discards entries at or below idx and moves base to idx.
func (s *DiskStore) dropThrough(idx uint64) {
	if idx >= s.lastLocked() {
		s.entries = nil
	} else {
		s.entries = append([]Entry(nil), s.entries[idx-s.base:]...)
	}
	s.base = idx
}

func (s *DiskStore) lastLocked() uint64 {
	return s.base + uint64(len(s.entries))
}

func (s *DiskStore) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	first := entries[0].Index
	last := s.lastLocked()
	if first > last+1 || first <= s.snapIdx {
		return fmt.Errorf("raft: append at %d outside (%d, %d]", first, s.snapIdx, last+1)
	}
	payloads := make([][]byte, len(entries))
	for i, e := range entries {
		p := make([]byte, 18, 18+len(e.Data))
		p[0] = recEntry
		binary.LittleEndian.PutUint64(p[1:], e.Index)
		binary.LittleEndian.PutUint64(p[9:], e.Term)
		p[17] = byte(e.Type)
		payloads[i] = append(p, e.Data...)
	}
	seg, err := s.wal.Append(payloads...)
	if err != nil {
		return err
	}
	s.entries = append(s.entries[:first-s.base-1], entries...)
	s.segMax[seg] = entries[len(entries)-1].Index
	return nil
}

func (s *DiskStore) Entries(lo, hi uint64) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lo <= s.snapIdx {
		return nil, ErrCompacted
	}
	if hi > s.lastLocked()+1 {
		return nil, fmt.Errorf("raft: entries [%d,%d) beyond last %d", lo, hi, s.lastLocked())
	}
	out := make([]Entry, hi-lo)
	copy(out, s.entries[lo-s.base-1:hi-s.base-1])
	return out, nil
}

func (s *DiskStore) Term(index uint64) (uint64, error) {
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
	return s.entries[index-s.base-1].Term, nil
}

func (s *DiskStore) FirstIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapIdx + 1
}

func (s *DiskStore) LastIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastLocked()
}

func (s *DiskStore) SetState(term uint64, votedFor NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writeState(term, votedFor); err != nil {
		return err
	}
	s.term, s.vote = term, votedFor
	return nil
}

func (s *DiskStore) writeState(term uint64, vote NodeID) error {
	p := make([]byte, 17)
	p[0] = recState
	binary.LittleEndian.PutUint64(p[1:], term)
	binary.LittleEndian.PutUint64(p[9:], uint64(vote))
	_, err := s.wal.Append(p)
	return err
}

func (s *DiskStore) State() (uint64, NodeID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term, s.vote, nil
}

func (s *DiskStore) snapshotPath(index uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("snap-%d", index))
}

func (s *DiskStore) Compact(index, term uint64, snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index <= s.snapIdx {
		return nil
	}
	if err := writeFileAtomic(s.snapshotPath(index), snapshot); err != nil {
		return fmt.Errorf("raft: write snapshot: %w", err)
	}
	// The record goes into a fresh segment together with the current
	// state, so every older segment holding only obsolete entries can go.
	if _, err := s.wal.Rotate(); err != nil {
		return err
	}
	p := make([]byte, 17)
	p[0] = recSnapshot
	binary.LittleEndian.PutUint64(p[1:], index)
	binary.LittleEndian.PutUint64(p[9:], term)
	st := make([]byte, 17)
	st[0] = recState
	binary.LittleEndian.PutUint64(st[1:], s.term)
	binary.LittleEndian.PutUint64(st[9:], uint64(s.vote))
	if _, err := s.wal.Append(p, st); err != nil {
		return err
	}
	old := s.snapIdx
	s.dropThrough(index)
	s.snapIdx, s.snapTerm = index, term
	if old > 0 {
		os.Remove(s.snapshotPath(old))
	}
	return s.deleteObsoleteSegments()
}

func (s *DiskStore) deleteObsoleteSegments() error {
	cur := s.wal.Segment()
	keep := uint64(0)
	for seg := range s.segMax {
		if seg >= cur {
			continue
		}
		if s.segMax[seg] > s.snapIdx {
			if keep == 0 || seg < keep {
				keep = seg
			}
		}
	}
	if keep == 0 {
		keep = cur
	}
	for seg := range s.segMax {
		if seg < keep {
			delete(s.segMax, seg)
		}
	}
	return s.wal.DeleteBefore(keep)
}

func (s *DiskStore) Snapshot() (uint64, uint64, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapIdx == 0 {
		return 0, 0, nil, nil
	}
	data, err := os.ReadFile(s.snapshotPath(s.snapIdx))
	if err != nil {
		return 0, 0, nil, fmt.Errorf("raft: read snapshot: %w", err)
	}
	return s.snapIdx, s.snapTerm, data, nil
}

func (s *DiskStore) removeStaleSnapshots() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "snap-") {
			continue
		}
		idx, err := strconv.ParseUint(strings.TrimPrefix(strings.TrimSuffix(name, ".tmp"), "snap-"), 10, 64)
		if err != nil || idx != s.snapIdx || strings.HasSuffix(name, ".tmp") {
			if err := os.Remove(filepath.Join(s.dir, name)); err != nil {
				return err
			}
		}
	}
	if s.snapIdx > 0 {
		if _, err := os.Stat(s.snapshotPath(s.snapIdx)); err != nil {
			return fmt.Errorf("raft: snapshot %d recorded but missing: %w", s.snapIdx, err)
		}
	}
	return nil
}

func (s *DiskStore) Close() error {
	return s.wal.Close()
}

func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
