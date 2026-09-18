package raft

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

func openStore(t *testing.T, dir string) *DiskStore {
	t.Helper()
	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func entriesFor(lo, hi, term uint64) []Entry {
	var out []Entry
	for i := lo; i < hi; i++ {
		out = append(out, Entry{Index: i, Term: term, Data: []byte(fmt.Sprintf("e%d", i))})
	}
	return out
}

func TestDiskStoreRecoversEntriesStateAndTruncation(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	if err := s.Append(entriesFor(1, 11, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.SetState(3, 2); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(entriesFor(6, 9, 3)); err != nil {
		t.Fatal(err)
	}
	if s.LastIndex() != 8 {
		t.Fatalf("last = %d", s.LastIndex())
	}
	s.Close()

	s = openStore(t, dir)
	defer s.Close()
	term, vote, _ := s.State()
	if term != 3 || vote != 2 {
		t.Fatalf("state = %d/%d", term, vote)
	}
	if s.FirstIndex() != 1 || s.LastIndex() != 8 {
		t.Fatalf("range = [%d,%d]", s.FirstIndex(), s.LastIndex())
	}
	got, err := s.Entries(1, 9)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range got {
		wantTerm := uint64(1)
		if e.Index >= 6 {
			wantTerm = 3
		}
		if e.Term != wantTerm || !bytes.Equal(e.Data, []byte(fmt.Sprintf("e%d", e.Index))) {
			t.Fatalf("entry %+v", e)
		}
	}
}

func TestDiskStoreCompactKeepsTailAndSnapshotAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	if err := s.Append(entriesFor(1, 101, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(60, 1, []byte("snap60")); err != nil {
		t.Fatal(err)
	}
	if s.FirstIndex() != 61 || s.LastIndex() != 100 {
		t.Fatalf("range = [%d,%d]", s.FirstIndex(), s.LastIndex())
	}
	if _, err := s.Entries(60, 62); err != ErrCompacted {
		t.Fatalf("err = %v, want ErrCompacted", err)
	}
	if term, err := s.Term(60); err != nil || term != 1 {
		t.Fatalf("term(60) = %d, %v", term, err)
	}
	if err := s.Append(entriesFor(101, 111, 2)); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s = openStore(t, dir)
	defer s.Close()
	idx, term, data, err := s.Snapshot()
	if err != nil || idx != 60 || term != 1 || string(data) != "snap60" {
		t.Fatalf("snapshot = %d/%d/%q, %v", idx, term, data, err)
	}
	if s.FirstIndex() != 61 || s.LastIndex() != 110 {
		t.Fatalf("range after reopen = [%d,%d]", s.FirstIndex(), s.LastIndex())
	}
	got, _ := s.Entries(61, 111)
	if len(got) != 50 || got[0].Index != 61 || got[49].Term != 2 {
		t.Fatalf("entries = %d, first %+v", len(got), got[0])
	}
}

func TestDiskStoreCompactBeyondLastReplacesLog(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	s.Append(entriesFor(1, 11, 1))
	if err := s.Compact(500, 7, []byte("far")); err != nil {
		t.Fatal(err)
	}
	if s.FirstIndex() != 501 || s.LastIndex() != 500 {
		t.Fatalf("range = [%d,%d]", s.FirstIndex(), s.LastIndex())
	}
	if err := s.Append([]Entry{{Index: 501, Term: 7}}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s = openStore(t, dir)
	defer s.Close()
	if s.FirstIndex() != 501 || s.LastIndex() != 501 {
		t.Fatalf("range after reopen = [%d,%d]", s.FirstIndex(), s.LastIndex())
	}
	if term, _ := s.Term(500); term != 7 {
		t.Fatalf("term(500) = %d", term)
	}
}

func TestDiskStoreDeletesSegmentsBelowSnapshot(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	defer s.Close()
	big := make([]byte, 1<<20)
	for i := uint64(1); i <= 40; i++ {
		if err := s.Append([]Entry{{Index: i, Term: 1, Data: big}}); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	if len(before) < 3 {
		t.Fatalf("expected several segments, have %d", len(before))
	}
	if err := s.Compact(35, 1, []byte("s")); err != nil {
		t.Fatal(err)
	}
	after, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	if len(after) >= len(before) {
		t.Fatalf("segments %d -> %d, nothing deleted", len(before), len(after))
	}
	got, err := s.Entries(36, 41)
	if err != nil || len(got) != 5 {
		t.Fatalf("tail = %d entries, %v", len(got), err)
	}
}

func TestDiskStoreRejectsAppendWithGap(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	if err := s.Append([]Entry{{Index: 5, Term: 1}}); err == nil {
		t.Fatal("expected error")
	}
}
