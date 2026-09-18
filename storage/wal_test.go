package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func replayAll(t *testing.T, dir string, opts WALOptions) (*WAL, [][]byte, []uint64) {
	t.Helper()
	var payloads [][]byte
	var segs []uint64
	w, err := OpenWAL(dir, opts, func(seg uint64, p []byte) error {
		payloads = append(payloads, append([]byte(nil), p...))
		segs = append(segs, seg)
		return nil
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return w, payloads, segs
}

func TestWALReplaysRecordsAcrossSegmentsInOrder(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := replayAll(t, dir, WALOptions{SegmentSize: 64})
	var want []string
	for i := 0; i < 50; i++ {
		p := fmt.Sprintf("record-%03d", i)
		if _, err := w.Append([]byte(p)); err != nil {
			t.Fatal(err)
		}
		want = append(want, p)
	}
	if w.Segment() < 5 {
		t.Fatalf("expected rotation at 64 bytes, on segment %d", w.Segment())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, got, segs := replayAll(t, dir, WALOptions{SegmentSize: 64})
	defer w2.Close()
	if len(got) != len(want) {
		t.Fatalf("replayed %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Fatalf("record %d = %q, want %q", i, got[i], want[i])
		}
		if i > 0 && segs[i] < segs[i-1] {
			t.Fatalf("segments not monotonic at %d", i)
		}
	}
	if w2.Segment() <= segs[len(segs)-1] {
		t.Fatalf("new segment %d should follow last replayed %d", w2.Segment(), segs[len(segs)-1])
	}
}

func TestWALAppendsSeveralPayloadsIntoOneSegment(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := replayAll(t, dir, WALOptions{SegmentSize: 16})
	if _, err := w.Append([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")); err != nil {
		t.Fatal(err)
	}
	seg, err := w.Append([]byte("b"), []byte("c"), []byte("d"))
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	_, got, segs := replayAll(t, dir, WALOptions{})
	if len(got) != 4 {
		t.Fatalf("got %d records", len(got))
	}
	for _, s := range segs[1:] {
		if s != seg {
			t.Fatalf("grouped payloads split across segments: %v", segs)
		}
	}
}

func TestWALTruncatesTornTailOfNewestSegment(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := replayAll(t, dir, WALOptions{})
	for i := 0; i < 3; i++ {
		if _, err := w.Append([]byte(fmt.Sprintf("rec%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	seg := w.Segment()
	w.Close()

	path := segmentPath(dir, seg)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-2); err != nil {
		t.Fatal(err)
	}

	w2, got, _ := replayAll(t, dir, WALOptions{})
	w2.Close()
	if len(got) != 2 || string(got[1]) != "rec1" {
		t.Fatalf("got %q", got)
	}
	info, _ = os.Stat(path)
	if info.Size() != 2*(recordHeader+4) {
		t.Fatalf("torn tail not truncated, size %d", info.Size())
	}
}

func TestWALTreatsCorruptionInOlderSegmentAsError(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := replayAll(t, dir, WALOptions{})
	if _, err := w.Append([]byte("first")); err != nil {
		t.Fatal(err)
	}
	first := w.Segment()
	if _, err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append([]byte("second")); err != nil {
		t.Fatal(err)
	}
	w.Close()

	path := segmentPath(dir, first)
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, recordHeader); err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, err = OpenWAL(dir, WALOptions{}, func(uint64, []byte) error { return nil })
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

func TestWALDeleteBeforeRemovesOldSegments(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := replayAll(t, dir, WALOptions{})
	for i := 0; i < 3; i++ {
		if _, err := w.Append([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Rotate(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.DeleteBefore(3); err != nil {
		t.Fatal(err)
	}
	w.Close()
	matches, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	if len(matches) != 2 {
		t.Fatalf("segments left: %v", matches)
	}
}

func TestWALSyncIntervalFlushesInBackground(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := replayAll(t, dir, WALOptions{Sync: SyncInterval(5 * time.Millisecond)})
	if _, err := w.Append([]byte("x")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		w.mu.Lock()
		dirty := w.dirty
		w.mu.Unlock()
		if !dirty {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background sync never ran")
		}
		time.Sleep(time.Millisecond)
	}
	w.Close()
}

func TestWALRejectsEmptyRecord(t *testing.T) {
	w, _, _ := replayAll(t, t.TempDir(), WALOptions{})
	defer w.Close()
	if _, err := w.Append(nil); err == nil {
		t.Fatal("expected error")
	}
}
