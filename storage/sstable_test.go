package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func writeTestTable(t *testing.T, dir string, n int, blockSize int) (*table, []string) {
	t.Helper()
	path := tablePath(dir, 1)
	w, err := newTableWriter(path, 1, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%06d", i*3)
		keys = append(keys, k)
		kind := kindPut
		if i%10 == 9 {
			kind = kindDelete
		}
		if err := w.add(makeInternalKey(nil, []byte(k), uint64(i+1), kind), []byte("value-"+k)); err != nil {
			t.Fatal(err)
		}
	}
	meta, err := w.finish()
	if err != nil {
		t.Fatal(err)
	}
	if meta.count != uint64(n) || string(userKey(meta.smallest)) != keys[0] || string(userKey(meta.largest)) != keys[n-1] {
		t.Fatalf("meta = %+v", meta)
	}
	tb, err := openTable(path, meta)
	if err != nil {
		t.Fatal(err)
	}
	return tb, keys
}

func TestTableGetFindsEveryKeyAndRespectsTombstones(t *testing.T) {
	tb, keys := writeTestTable(t, t.TempDir(), 5000, 4096)
	defer tb.unref()
	for i, k := range keys {
		v, found, deleted, err := tb.get([]byte(k), ^uint64(0)>>8)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("%q not found", k)
		}
		if i%10 == 9 {
			if !deleted {
				t.Fatalf("%q should be a tombstone", k)
			}
			continue
		}
		if deleted || string(v) != "value-"+k {
			t.Fatalf("%q = %q deleted=%v", k, v, deleted)
		}
	}
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("key%06d", i*3+1)
		if _, found, _, _ := tb.get([]byte(k), ^uint64(0)>>8); found {
			t.Fatalf("%q should be absent", k)
		}
	}
}

func TestTableGetHonoursSequenceBound(t *testing.T) {
	tb, _ := writeTestTable(t, t.TempDir(), 100, 4096)
	defer tb.unref()
	if _, found, _, _ := tb.get([]byte("key000030"), 5); found {
		t.Fatal("version with sequence 11 must be invisible at sequence 5")
	}
	if _, found, _, _ := tb.get([]byte("key000030"), 11); !found {
		t.Fatal("version with sequence 11 must be visible at sequence 11")
	}
}

func TestTableIteratorWalksAllEntriesAcrossBlocks(t *testing.T) {
	tb, keys := writeTestTable(t, t.TempDir(), 5000, 512)
	defer tb.unref()
	it := tb.iter()
	defer it.close()
	i := 0
	for it.seekToFirst(); it.valid(); it.next() {
		if string(userKey(it.key())) != keys[i] {
			t.Fatalf("entry %d = %q want %q", i, userKey(it.key()), keys[i])
		}
		i++
	}
	if err := it.close(); err != nil {
		t.Fatal(err)
	}
	if i != len(keys) {
		t.Fatalf("walked %d entries", i)
	}
}

func TestTableIteratorSeekAcrossBlockBoundaries(t *testing.T) {
	tb, keys := writeTestTable(t, t.TempDir(), 5000, 256)
	defer tb.unref()
	it := tb.iter()
	defer it.close()
	for _, i := range []int{0, 1, 17, 1000, 2500, 4999} {
		target := makeInternalKey(nil, []byte(keys[i]), ^uint64(0)>>8, kindPut)
		it.seek(target)
		if !it.valid() || string(userKey(it.key())) != keys[i] {
			t.Fatalf("seek %q landed on %q", keys[i], userKey(it.key()))
		}
		gap := makeInternalKey(nil, []byte(keys[i]+"x"), ^uint64(0)>>8, kindPut)
		it.seek(gap)
		if i == 4999 {
			if it.valid() {
				t.Fatal("seek past end should be invalid")
			}
			continue
		}
		if !it.valid() || string(userKey(it.key())) != keys[i+1] {
			t.Fatalf("seek past %q landed on %q", keys[i], userKey(it.key()))
		}
	}
}

func TestTableDetectsCorruptBlock(t *testing.T) {
	dir := t.TempDir()
	tb, keys := writeTestTable(t, dir, 1000, 512)
	tb.unref()
	path := tablePath(dir, 1)
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff, 0xff}, 30); err != nil {
		t.Fatal(err)
	}
	f.Close()
	tb, err = openTable(path, tableMeta{num: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer tb.unref()
	if _, _, _, err := tb.get([]byte(keys[0]), ^uint64(0)>>8); err == nil {
		t.Fatal("expected checksum failure")
	}
}

func TestTableUnlinkedOnlyAfterLastReaderCloses(t *testing.T) {
	dir := t.TempDir()
	tb, _ := writeTestTable(t, dir, 10, 512)
	it := tb.iter()
	tb.gone.Store(true)
	tb.unref()
	if _, err := os.Stat(tablePath(dir, 1)); err != nil {
		t.Fatal("file removed while an iterator is open")
	}
	it.close()
	if _, err := os.Stat(filepath.Join(dir, "00000001.sst")); !os.IsNotExist(err) {
		t.Fatal("file should be removed after the last reader closes")
	}
}
