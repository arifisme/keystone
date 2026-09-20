package storage

import (
	"bytes"
	"os/signal"
	"syscall"
	"testing"
)

// A file size limit makes the kernel take part of a write and refuse the
// rest, which is what a full disk does. The limit is process-wide, so
// nothing else may write a file while it is lowered.
func TestWALRefusesToAppendBehindAPartlyWrittenRecord(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := replayAll(t, dir, WALOptions{})
	if _, err := w.Append([]byte("first")); err != nil {
		t.Fatal(err)
	}

	signal.Ignore(syscall.SIGXFSZ)
	defer signal.Reset(syscall.SIGXFSZ)
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: 100, Max: limit.Max}); err != nil {
		t.Fatal(err)
	}
	_, failed := w.Append(bytes.Repeat([]byte("x"), 200))
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Fatal(err)
	}
	if failed == nil {
		t.Fatal("the append fit under the file size limit")
	}
	if w.Size() != int64(recordHeader+len("first")) {
		t.Fatalf("size = %d after a failed append", w.Size())
	}

	if _, err := w.Append([]byte("third")); err == nil {
		t.Fatal("appended behind a partly written record")
	}
	if _, err := w.Rotate(); err == nil {
		t.Fatal("rotated a partly written record into an older segment")
	}
	w.Close()

	w, got, _ := replayAll(t, dir, WALOptions{})
	defer w.Close()
	if len(got) != 1 || string(got[0]) != "first" {
		t.Fatalf("replayed %q, want only the first record", got)
	}
}
