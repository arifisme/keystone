package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func testOptions(seed int64) Options {
	return Options{
		MemtableSize: 32 << 10,
		BlockSize:    512,
		Sync:         SyncInterval(10 * time.Millisecond),
		Rand:         rand.New(rand.NewSource(seed)),
	}
}

// waitIdle blocks until no flush or compaction is pending.
func waitIdle(t *testing.T, d DB) {
	t.Helper()
	db := d.(*db)
	deadline := time.Now().Add(30 * time.Second)
	for {
		db.mu.Lock()
		idle := len(db.current.imm) == 0 && pickCompaction(db.current.tables) == nil
		err := db.bgErr
		db.mu.Unlock()
		if err != nil {
			t.Fatalf("background error: %v", err)
		}
		if idle {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("background work never settled")
		}
		time.Sleep(time.Millisecond)
	}
}

func checkOracle(t *testing.T, d DB, oracle map[string][]byte) {
	t.Helper()
	keys := make([]string, 0, len(oracle))
	for k := range oracle {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	it := d.Scan(nil, nil)
	i := 0
	for it.Next() {
		if i >= len(keys) {
			t.Fatalf("scan produced extra key %q", it.Key())
		}
		if string(it.Key()) != keys[i] || !bytes.Equal(it.Value(), oracle[keys[i]]) {
			t.Fatalf("scan position %d: %q=%q, want %q=%q", i, it.Key(), it.Value(), keys[i], oracle[keys[i]])
		}
		i++
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if i != len(keys) {
		t.Fatalf("scan produced %d keys, want %d", i, len(keys))
	}
}

func TestDBPropertyMatchesOracle(t *testing.T) {
	for _, seed := range []int64{1, 2, 3} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			dir := t.TempDir()
			rng := rand.New(rand.NewSource(seed))
			d, err := Open(dir, testOptions(seed))
			if err != nil {
				t.Fatal(err)
			}
			oracle := map[string][]byte{}
			const ops = 100000
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("k%05d", rng.Intn(3000))
				switch r := rng.Intn(100); {
				case r < 60:
					val := make([]byte, rng.Intn(200))
					rng.Read(val)
					if err := d.Put([]byte(key), val); err != nil {
						t.Fatal(err)
					}
					oracle[key] = val
				case r < 80:
					if err := d.Delete([]byte(key)); err != nil {
						t.Fatal(err)
					}
					delete(oracle, key)
				default:
					got, err := d.Get([]byte(key))
					want, ok := oracle[key]
					if !ok {
						if err != ErrNotFound {
							t.Fatalf("op %d: get %q = %q, %v; want not found", i, key, got, err)
						}
					} else if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("op %d: get %q = %q, %v; want %q", i, key, got, err, want)
					}
				}
				if i == ops/3 || i == 2*ops/3 {
					checkOracle(t, d, oracle)
					if err := d.Close(); err != nil {
						t.Fatal(err)
					}
					if d, err = Open(dir, testOptions(seed)); err != nil {
						t.Fatal(err)
					}
				}
			}
			checkOracle(t, d, oracle)
			waitIdle(t, d)
			checkOracle(t, d, oracle)
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			d, err = Open(dir, testOptions(seed))
			if err != nil {
				t.Fatal(err)
			}
			checkOracle(t, d, oracle)
			d.Close()
		})
	}
}

func TestDBRecoversFromWALAndDeletesFlushedSegments(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{MemtableSize: 4 << 10, Sync: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if err := d.Put([]byte(fmt.Sprintf("key%04d", i)), bytes.Repeat([]byte{'v'}, 50)); err != nil {
			t.Fatal(err)
		}
	}
	waitIdle(t, d)
	segs, _ := listSegments(dir)
	if len(segs) != 1 {
		t.Fatalf("flushed segments not deleted: %v", segs)
	}
	if err := d.Put([]byte("tail"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, Options{MemtableSize: 4 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < 500; i++ {
		if _, err := d.Get([]byte(fmt.Sprintf("key%04d", i))); err != nil {
			t.Fatalf("key%04d after reopen: %v", i, err)
		}
	}
	if v, err := d.Get([]byte("tail")); err != nil || string(v) != "x" {
		t.Fatalf("tail = %q, %v", v, err)
	}
}

func TestDBScanRespectsBoundsAndHidesDeletedKeys(t *testing.T) {
	d, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		d.Put([]byte(k), []byte("v"+k))
	}
	d.Delete([]byte("c"))
	d.Put([]byte("b"), []byte("v2"))
	var got []string
	it := d.Scan([]byte("b"), []byte("e"))
	for it.Next() {
		got = append(got, string(it.Key())+"="+string(it.Value()))
	}
	it.Close()
	want := "b=v2,d=vd"
	if joined := joinStrings(got); joined != want {
		t.Fatalf("scan = %q, want %q", joined, want)
	}
}

func joinStrings(s []string) string {
	var b bytes.Buffer
	for i, x := range s {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(x)
	}
	return b.String()
}

func TestDBScanIsASnapshot(t *testing.T) {
	d, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	d.Put([]byte("a"), []byte("1"))
	it := d.Scan(nil, nil)
	d.Put([]byte("a"), []byte("2"))
	d.Put([]byte("b"), []byte("1"))
	n := 0
	for it.Next() {
		n++
		if string(it.Key()) != "a" || string(it.Value()) != "1" {
			t.Fatalf("saw %q=%q", it.Key(), it.Value())
		}
	}
	it.Close()
	if n != 1 {
		t.Fatalf("saw %d keys", n)
	}
}

func TestDBCompactionDropsTombstonesOnlyAtBottom(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{MemtableSize: 2 << 10, BlockSize: 256})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < 100; i++ {
		d.Put([]byte(fmt.Sprintf("key%04d", i)), bytes.Repeat([]byte{'x'}, 40))
	}
	for i := 0; i < 100; i++ {
		d.Delete([]byte(fmt.Sprintf("key%04d", i)))
	}
	// Two oversized writes push everything above into flushed tables; the
	// second one stays behind in the active memtable.
	d.Put([]byte("zfill1"), make([]byte, 4<<10))
	d.Put([]byte("zfill2"), make([]byte, 4<<10))
	waitIdle(t, d)
	db := d.(*db)
	liveTables := func() []*table {
		db.mu.RLock()
		defer db.mu.RUnlock()
		return db.current.tables
	}
	tombstones := func(tables []*table) int {
		n := 0
		for _, tb := range tables {
			it := tb.iter()
			for it.seekToFirst(); it.valid(); it.next() {
				if _, _, kind := splitInternalKey(it.key()); kind == kindDelete {
					n++
				}
			}
			it.close()
		}
		return n
	}
	db.installMu.Lock()
	defer db.installMu.Unlock()
	tables := liveTables()
	if len(tables) < 3 {
		t.Fatalf("test needs several tables, have %d", len(tables))
	}
	inputTombstones := tombstones(tables[:2])
	if inputTombstones == 0 {
		t.Fatal("newest tables should hold tombstones")
	}
	if err := db.compactLocked(tables[:2]); err != nil {
		t.Fatal(err)
	}
	if got := tombstones(liveTables()[:1]); got != inputTombstones {
		t.Fatalf("compaction above the bottom kept %d of %d tombstones", got, inputTombstones)
	}
	if err := db.compactLocked(liveTables()); err != nil {
		t.Fatal(err)
	}
	tables = liveTables()
	if len(tables) != 1 || tables[0].meta.count != 1 || tombstones(tables) != 0 {
		t.Fatalf("bottom compaction left %d tables, %d entries, %d tombstones", len(tables), tables[0].meta.count, tombstones(tables))
	}
	it := d.Scan(nil, []byte("z"))
	if it.Next() {
		t.Fatalf("deleted key %q visible", it.Key())
	}
	it.Close()
}

func TestDBKeepsTableCountBounded(t *testing.T) {
	d, err := Open(t.TempDir(), Options{MemtableSize: 1 << 10, BlockSize: 256, Sync: SyncInterval(time.Millisecond)})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 20000; i++ {
		v := make([]byte, rng.Intn(100))
		d.Put([]byte(fmt.Sprintf("k%d", rng.Intn(500))), v)
	}
	waitIdle(t, d)
	db := d.(*db)
	db.mu.RLock()
	n := len(db.current.tables)
	db.mu.RUnlock()
	if n > maxTables {
		t.Fatalf("%d tables after settling", n)
	}
}

func TestDBCompactionDeletesItsInputFiles(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{MemtableSize: 1 << 10, BlockSize: 256, Sync: SyncInterval(time.Millisecond)})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 3000; i++ {
		d.Put([]byte(fmt.Sprintf("k%d", rng.Intn(500))), make([]byte, rng.Intn(100)))
	}
	waitIdle(t, d)
	st := d.Stats()
	if st.Compactions == 0 {
		t.Fatal("no compaction ran")
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != st.Tables {
		t.Fatalf("%d table files on disk for %d live tables after %d compactions", len(files), st.Tables, st.Compactions)
	}
}

func TestDBConcurrentReadersAndWriters(t *testing.T) {
	d, err := Open(t.TempDir(), Options{MemtableSize: 16 << 10, BlockSize: 512, Sync: SyncInterval(time.Millisecond)})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := []byte(fmt.Sprintf("w%d-%d", w, i%100))
				if err := d.Put(k, []byte(fmt.Sprint(i))); err != nil {
					t.Error(err)
					return
				}
				if i%7 == 0 {
					d.Delete(k)
				}
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				it := d.Scan(nil, nil)
				var prev []byte
				for it.Next() {
					if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
						t.Error("scan out of order")
					}
					prev = append(prev[:0], it.Key()...)
				}
				if err := it.Close(); err != nil {
					t.Error(err)
				}
				d.Get([]byte("w1-5"))
			}
		}()
	}
	wg.Wait()
}

func TestDBOpenRemovesStrayTablesAndTempManifest(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	d.Put([]byte("a"), []byte("1"))
	d.Close()
	os.WriteFile(filepath.Join(dir, "00000099.sst"), []byte("junk"), 0o644)
	os.WriteFile(filepath.Join(dir, "MANIFEST.tmp"), []byte("junk"), 0o644)
	d, err = Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := os.Stat(filepath.Join(dir, "00000099.sst")); !os.IsNotExist(err) {
		t.Fatal("stray table not removed")
	}
	if v, err := d.Get([]byte("a")); err != nil || string(v) != "1" {
		t.Fatalf("a = %q, %v", v, err)
	}
}

func TestDBGetAfterCloseFails(t *testing.T) {
	d, _ := Open(t.TempDir(), Options{})
	d.Close()
	if _, err := d.Get([]byte("x")); err != ErrClosed {
		t.Fatalf("err = %v", err)
	}
	if err := d.Put([]byte("x"), nil); err != ErrClosed {
		t.Fatalf("put err = %v", err)
	}
}

func TestDBExportRestoreRoundTripsLiveState(t *testing.T) {
	src, err := Open(t.TempDir(), Options{MemtableSize: 4 << 10, BlockSize: 256})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	oracle := map[string][]byte{}
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("k%03d", i%200)
		v := []byte(fmt.Sprintf("v%d", i))
		src.Put([]byte(k), v)
		oracle[k] = v
	}
	for i := 0; i < 200; i += 3 {
		k := fmt.Sprintf("k%03d", i)
		src.Delete([]byte(k))
		delete(oracle, k)
	}
	var buf bytes.Buffer
	if err := src.Export(&buf); err != nil {
		t.Fatal(err)
	}

	dstDir := t.TempDir()
	dst, err := Open(dstDir, Options{MemtableSize: 4 << 10, BlockSize: 256})
	if err != nil {
		t.Fatal(err)
	}
	dst.Put([]byte("stale"), []byte("gone after restore"))
	dst.Put([]byte("k001"), []byte("stale value"))
	if err := dst.Restore(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	checkOracle(t, dst, oracle)

	// Writes after a restore must win over restored versions.
	dst.Put([]byte("k001"), []byte("after"))
	oracle["k001"] = []byte("after")
	checkOracle(t, dst, oracle)
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
	dst, err = Open(dstDir, Options{MemtableSize: 4 << 10, BlockSize: 256})
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	checkOracle(t, dst, oracle)
	if _, err := dst.Get([]byte("stale")); err != ErrNotFound {
		t.Fatalf("stale key survived restore: %v", err)
	}
}

func TestDBExportOfEmptyDatabaseRestoresEmpty(t *testing.T) {
	src, _ := Open(t.TempDir(), Options{})
	defer src.Close()
	var buf bytes.Buffer
	if err := src.Export(&buf); err != nil {
		t.Fatal(err)
	}
	dst, _ := Open(t.TempDir(), Options{})
	defer dst.Close()
	dst.Put([]byte("x"), []byte("y"))
	if err := dst.Restore(&buf); err != nil {
		t.Fatal(err)
	}
	checkOracle(t, dst, map[string][]byte{})
}

func TestMemoryDBMatchesOracleAndSnapshotsInterchangeWithDisk(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	m := NewMemory(9)
	defer m.Close()
	oracle := map[string][]byte{}
	for i := 0; i < 20000; i++ {
		key := fmt.Sprintf("k%03d", rng.Intn(500))
		if rng.Intn(4) == 0 {
			m.Delete([]byte(key))
			delete(oracle, key)
			continue
		}
		v := []byte(fmt.Sprint(i))
		m.Put([]byte(key), v)
		oracle[key] = v
	}
	checkOracle(t, m, oracle)

	var buf bytes.Buffer
	if err := m.Export(&buf); err != nil {
		t.Fatal(err)
	}
	disk, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	if err := disk.Restore(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	checkOracle(t, disk, oracle)

	disk.Put([]byte("k000"), []byte("from disk"))
	oracle["k000"] = []byte("from disk")
	buf.Reset()
	disk.Export(&buf)
	back := NewMemory(1)
	defer back.Close()
	back.Put([]byte("zzz"), []byte("gone"))
	if err := back.Restore(&buf); err != nil {
		t.Fatal(err)
	}
	checkOracle(t, back, oracle)
	back.Put([]byte("k000"), []byte("newer"))
	oracle["k000"] = []byte("newer")
	checkOracle(t, back, oracle)
}
