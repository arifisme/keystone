package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNotFound = errors.New("storage: not found")
	ErrClosed   = errors.New("storage: closed")
)

type Options struct {
	// MemtableSize is the approximate byte size at which the memtable is
	// frozen and its WAL segment rotated.
	MemtableSize int64
	BlockSize    int
	Sync         SyncPolicy
	Rand         *rand.Rand
	// OnSync, if set, is told how long each WAL fsync took.
	OnSync func(time.Duration)
}

// Stats counts background work since the engine was opened.
type Stats struct {
	Flushes     uint64
	Compactions uint64
	Tables      int
}

func (o Options) withDefaults() Options {
	if o.MemtableSize <= 0 {
		o.MemtableSize = 4 << 20
	}
	if o.BlockSize <= 0 {
		o.BlockSize = 4096
	}
	if o.Rand == nil {
		o.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return o
}

type DB interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Write(b *Batch) error
	// Scan returns keys in [start, end). A nil end means no upper bound.
	Scan(start, end []byte) Iterator
	// Sync forces everything written so far to stable storage.
	Sync() error
	// Export writes the live contents as one SSTable.
	Export(w io.Writer) error
	// Restore replaces the contents with an SSTable produced by Export.
	Restore(r io.Reader) error
	Stats() Stats
	Close() error
}

type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Err() error
	Close() error
}

// maxImmutables bounds how far writers may run ahead of the flusher before
// they stall.
const maxImmutables = 2

type db struct {
	dir  string
	opts Options
	wal  *WAL

	// mu guards current and the fields below it. Readers take it shared
	// just long enough to reference the current version.
	mu      sync.RWMutex
	current *version
	nextNum uint64
	closed  bool
	bgErr   error
	bgCond  *sync.Cond

	seq         atomic.Uint64
	flushes     atomic.Uint64
	compactions atomic.Uint64

	wmu    sync.Mutex
	wcond  *sync.Cond
	wqueue []*writer

	// installMu serializes flush and compaction: each rewrites MANIFEST
	// from the table set as it stands, so two must not interleave.
	installMu  sync.Mutex
	flushedSeq uint64

	bg sync.WaitGroup
}

type writer struct {
	batch *Batch
	err   error
	done  bool
}

func Open(dir string, opts Options) (DB, error) {
	opts = opts.withDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	man, err := readManifest(dir)
	if err != nil {
		return nil, err
	}
	if err := removeStrayTables(dir, man); err != nil {
		return nil, err
	}
	var tables []*table
	for _, meta := range man.tables {
		t, err := openTable(tablePath(dir, meta.num), meta)
		if err != nil {
			for _, t := range tables {
				t.unref()
			}
			return nil, err
		}
		tables = append(tables, t)
	}

	d := &db{dir: dir, opts: opts, nextNum: man.nextNum, flushedSeq: man.lastSeq}
	d.seq.Store(man.lastSeq)
	d.bgCond = sync.NewCond(&d.mu)
	d.wcond = sync.NewCond(&d.wmu)

	var recovered []*memtable
	var cur *memtable
	wal, err := OpenWAL(dir, WALOptions{SegmentSize: opts.MemtableSize * 4, Sync: opts.Sync, OnSync: opts.OnSync}, func(seg uint64, payload []byte) error {
		if seg < man.logNum {
			return nil
		}
		seq, b, err := decodeBatch(payload)
		if err != nil {
			return fmt.Errorf("segment %d: %w", seg, err)
		}
		if cur == nil || cur.seg != seg {
			cur = newMemtable(opts.Rand.Uint64(), seg)
			recovered = append(recovered, cur)
		}
		for i, op := range b.ops {
			cur.put(seq+uint64(i), op.kind, op.key, op.value)
		}
		if last := seq + uint64(len(b.ops)) - 1; last > d.seq.Load() {
			d.seq.Store(last)
		}
		return nil
	})
	if err != nil {
		for _, t := range tables {
			t.unref()
		}
		return nil, err
	}
	d.wal = wal
	if err := wal.DeleteBefore(man.logNum); err != nil {
		return nil, err
	}
	active := newMemtable(opts.Rand.Uint64(), wal.Segment())
	d.current = newVersion(active, recovered, tables)
	for _, t := range tables {
		t.unref()
	}

	d.bg.Add(2)
	go d.flushLoop()
	go d.compactLoop()
	return d, nil
}

func removeStrayTables(dir string, man *manifest) error {
	live := make(map[string]bool, len(man.tables))
	for _, t := range man.tables {
		live[filepath.Base(tablePath(dir, t.num))] = true
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		stray := strings.HasSuffix(name, ".sst") && !live[name] || name == "MANIFEST.tmp"
		if stray {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *db) Put(key, value []byte) error {
	var b Batch
	b.Put(key, value)
	return d.Write(&b)
}

func (d *db) Delete(key []byte) error {
	var b Batch
	b.Delete(key)
	return d.Write(&b)
}

// Write queues the batch and lets the writer at the head of the queue
// commit every queued batch with one WAL append and one sync.
func (d *db) Write(b *Batch) error {
	if b.Len() == 0 {
		return nil
	}
	for _, op := range b.ops {
		if len(op.key) == 0 {
			return errors.New("storage: empty key")
		}
	}
	w := &writer{batch: b}
	d.wmu.Lock()
	d.wqueue = append(d.wqueue, w)
	for !w.done && d.wqueue[0] != w {
		d.wcond.Wait()
	}
	if w.done {
		d.wmu.Unlock()
		return w.err
	}
	group := append([]*writer(nil), d.wqueue...)
	d.wmu.Unlock()

	err := d.commit(group)

	d.wmu.Lock()
	d.wqueue = d.wqueue[len(group):]
	for _, g := range group {
		g.err = err
		g.done = true
	}
	d.wcond.Broadcast()
	d.wmu.Unlock()
	return err
}

func (d *db) commit(group []*writer) error {
	seq := d.seq.Load()
	first := seq + 1
	payloads := make([][]byte, len(group))
	for i, w := range group {
		payloads[i] = w.batch.encode(nil, seq+1)
		seq += uint64(len(w.batch.ops))
	}

	if err := d.makeRoom(); err != nil {
		return err
	}
	seg, err := d.wal.Append(payloads...)
	if err != nil {
		return err
	}

	d.mu.Lock()
	if seg != d.current.mem.seg {
		d.install(d.current.withMemtable(newMemtable(d.opts.Rand.Uint64(), seg)))
		d.bgCond.Broadcast()
	}
	mem := d.current.mem
	d.mu.Unlock()

	s := first
	for _, w := range group {
		for _, op := range w.batch.ops {
			mem.put(s, op.kind, op.key, op.value)
			s++
		}
	}
	d.seq.Store(seq)
	return nil
}

// makeRoom rotates the WAL when the memtable is full, stalling first if the
// flusher has fallen too far behind.
func (d *db) makeRoom() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	if d.bgErr != nil {
		return d.bgErr
	}
	if d.current.mem.size.Load() < d.opts.MemtableSize {
		return nil
	}
	for len(d.current.imm) >= maxImmutables && !d.closed && d.bgErr == nil {
		d.bgCond.Wait()
	}
	if d.closed {
		return ErrClosed
	}
	if d.bgErr != nil {
		return d.bgErr
	}
	_, err := d.wal.Rotate()
	return err
}

// install publishes v as the current version. Caller holds mu.
func (d *db) install(v *version) {
	old := d.current
	d.current = v
	old.unref()
}

func (d *db) acquire() (*version, uint64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return nil, 0, ErrClosed
	}
	v := d.current
	v.ref()
	return v, d.seq.Load(), nil
}

func (d *db) Get(key []byte) ([]byte, error) {
	v, seq, err := d.acquire()
	if err != nil {
		return nil, err
	}
	defer v.unref()
	if val, found, deleted := v.mem.get(key, seq); found {
		return result(val, deleted)
	}
	for i := len(v.imm) - 1; i >= 0; i-- {
		if val, found, deleted := v.imm[i].get(key, seq); found {
			return result(val, deleted)
		}
	}
	for _, t := range v.tables {
		val, found, deleted, err := t.get(key, seq)
		if err != nil {
			return nil, err
		}
		if found {
			return result(val, deleted)
		}
	}
	return nil, ErrNotFound
}

func result(val []byte, deleted bool) ([]byte, error) {
	if deleted {
		return nil, ErrNotFound
	}
	return append([]byte(nil), val...), nil
}

func (d *db) Scan(start, end []byte) Iterator {
	v, seq, err := d.acquire()
	if err != nil {
		return &dbIter{err: err}
	}
	children := []internalIter{v.mem.iter()}
	for i := len(v.imm) - 1; i >= 0; i-- {
		children = append(children, v.imm[i].iter())
	}
	for _, t := range v.tables {
		children = append(children, t.iter())
	}
	it := &dbIter{v: v, seq: seq, m: newMergeIter(children), end: end}
	it.m.seek(makeInternalKey(nil, start, seq, kindPut))
	return it
}

// dbIter turns the merged stream of versions into one visible value per
// user key: versions newer than the snapshot are skipped, the first
// remaining version wins, and tombstones hide the key.
type dbIter struct {
	v      *version
	seq    uint64
	m      *mergeIter
	end    []byte
	k, val []byte
	last   []byte
	err    error
}

func (it *dbIter) Next() bool {
	if it.err != nil {
		return false
	}
	for it.m.valid() {
		user, seq, kind := splitInternalKey(it.m.key())
		if it.end != nil && bytes.Compare(user, it.end) >= 0 {
			return false
		}
		if seq > it.seq || bytes.Equal(user, it.last) {
			it.m.next()
			continue
		}
		it.last = append(it.last[:0], user...)
		if kind == kindDelete {
			it.m.next()
			continue
		}
		it.k = append(it.k[:0], user...)
		it.val = append(it.val[:0], it.m.value()...)
		it.m.next()
		return true
	}
	return false
}

func (it *dbIter) Key() []byte   { return it.k }
func (it *dbIter) Value() []byte { return it.val }
func (it *dbIter) Err() error    { return it.err }

func (it *dbIter) Close() error {
	if it.m != nil {
		if err := it.m.close(); err != nil && it.err == nil {
			it.err = err
		}
		it.m = nil
		if it.v != nil {
			it.v.unref()
		}
	}
	return it.err
}

func (d *db) flushLoop() {
	defer d.bg.Done()
	for {
		d.mu.Lock()
		for len(d.current.imm) == 0 && !d.closed {
			d.bgCond.Wait()
		}
		if d.closed {
			d.mu.Unlock()
			return
		}
		m := d.current.imm[0]
		d.mu.Unlock()
		if err := d.flush(m); err != nil {
			d.fail(err)
			return
		}
	}
}

func (d *db) fail(err error) {
	d.mu.Lock()
	if d.bgErr == nil {
		d.bgErr = err
	}
	d.bgCond.Broadcast()
	d.mu.Unlock()
}

func (d *db) allocNum() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := d.nextNum
	d.nextNum++
	return n
}

func (d *db) flush(m *memtable) error {
	d.installMu.Lock()
	defer d.installMu.Unlock()

	// m was picked before the wait for installMu; a Restore that got in
	// first has discarded it, and flushing it would bring its keys back.
	d.mu.RLock()
	stale := len(d.current.imm) == 0 || d.current.imm[0] != m
	d.mu.RUnlock()
	if stale {
		return nil
	}

	t, err := d.writeTable(m.iter())
	if err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	d.mu.RLock()
	tables := d.current.tables
	d.mu.RUnlock()
	if maxSeq := m.maxSeq.Load(); maxSeq > d.flushedSeq {
		d.flushedSeq = maxSeq
	}
	man := &manifest{lastSeq: d.flushedSeq, logNum: m.seg + 1}
	if t != nil {
		man.tables = append(man.tables, t.meta)
	}
	for _, old := range tables {
		man.tables = append(man.tables, old.meta)
	}
	d.mu.Lock()
	man.nextNum = d.nextNum
	d.mu.Unlock()
	if err := writeManifest(d.dir, man); err != nil {
		return err
	}

	d.mu.Lock()
	d.install(d.current.withFlushed(m, t))
	d.bgCond.Broadcast()
	d.mu.Unlock()
	if t != nil {
		t.unref()
	}
	d.flushes.Add(1)
	return d.wal.DeleteBefore(m.seg + 1)
}

func (d *db) Stats() Stats {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return Stats{Flushes: d.flushes.Load(), Compactions: d.compactions.Load(), Tables: len(d.current.tables)}
}

// writeTable drains it into a new SSTable keeping the newest version of
// each key. Every reader of the new table has a sequence bound at least as
// high as anything in it, so older versions are never visible again.
// Returns nil if nothing survived.
func (d *db) writeTable(it internalIter) (*table, error) {
	return d.writeTableFiltered(it, false)
}

func (d *db) writeTableFiltered(it internalIter, dropTombstones bool) (*table, error) {
	defer it.close()
	num := d.allocNum()
	path := tablePath(d.dir, num)
	w, err := newTableWriter(path, num, d.opts.BlockSize)
	if err != nil {
		return nil, err
	}
	var last []byte
	for it.seekToFirst(); it.valid(); it.next() {
		user, _, kind := splitInternalKey(it.key())
		if bytes.Equal(user, last) {
			continue
		}
		last = append(last[:0], user...)
		if dropTombstones && kind == kindDelete {
			continue
		}
		if err := w.add(it.key(), it.value()); err != nil {
			w.abort()
			return nil, err
		}
	}
	if err := it.close(); err != nil {
		w.abort()
		return nil, err
	}
	meta, err := w.finish()
	if err != nil {
		w.abort()
		return nil, err
	}
	if meta.count == 0 {
		os.Remove(path)
		return nil, nil
	}
	return openTable(path, meta)
}

func (d *db) Sync() error {
	return d.wal.Sync()
}

func (d *db) Export(w io.Writer) error {
	v, seq, err := d.acquire()
	if err != nil {
		return err
	}
	defer v.unref()
	children := []internalIter{v.mem.iter()}
	for i := len(v.imm) - 1; i >= 0; i-- {
		children = append(children, v.imm[i].iter())
	}
	for _, t := range v.tables {
		children = append(children, t.iter())
	}
	return exportIter(newMergeIter(children), seq, w, d.opts.BlockSize)
}

// exportIter writes the versions visible at seq as one table: newest
// version per key, tombstones dropped.
func exportIter(m internalIter, seq uint64, w io.Writer, blockSize int) error {
	defer m.close()
	tw := newStreamWriter(w, blockSize)
	var last []byte
	for m.seekToFirst(); m.valid(); m.next() {
		user, s, kind := splitInternalKey(m.key())
		if s > seq || bytes.Equal(user, last) {
			continue
		}
		last = append(last[:0], user...)
		if kind == kindDelete {
			continue
		}
		if err := tw.add(m.key(), m.value()); err != nil {
			return err
		}
	}
	if err := m.close(); err != nil {
		return err
	}
	_, err := tw.finish()
	return err
}

// Restore copies r into a new table file, makes it the only live table,
// and starts a fresh memtable and WAL segment so nothing older survives a
// reopen. The caller must not write concurrently.
func (d *db) Restore(r io.Reader) error {
	d.installMu.Lock()
	defer d.installMu.Unlock()
	num := d.allocNum()
	path := tablePath(d.dir, num)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	size, err := io.Copy(f, r)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(path)
		return fmt.Errorf("restore: %w", err)
	}
	t, err := openTable(path, tableMeta{num: num, size: uint64(size)})
	if err != nil {
		os.Remove(path)
		return fmt.Errorf("restore: %w", err)
	}
	var maxSeq uint64
	it := t.iter()
	for it.seekToFirst(); it.valid(); it.next() {
		if t.meta.count == 0 {
			t.meta.smallest = append([]byte(nil), it.key()...)
		}
		t.meta.largest = append(t.meta.largest[:0], it.key()...)
		t.meta.count++
		if _, s, _ := splitInternalKey(it.key()); s > maxSeq {
			maxSeq = s
		}
	}
	if err := it.close(); err != nil {
		t.unref()
		os.Remove(path)
		return fmt.Errorf("restore: %w", err)
	}

	seg, err := d.wal.Rotate()
	if err != nil {
		t.unref()
		return err
	}
	d.mu.Lock()
	if maxSeq > d.seq.Load() {
		d.seq.Store(maxSeq)
	}
	d.flushedSeq = maxSeq
	man := &manifest{nextNum: d.nextNum, lastSeq: maxSeq, logNum: seg}
	if t.meta.count > 0 {
		man.tables = []tableMeta{t.meta}
	}
	d.mu.Unlock()
	if err := writeManifest(d.dir, man); err != nil {
		t.unref()
		return err
	}
	d.mu.Lock()
	old := d.current
	var tables []*table
	if t.meta.count > 0 {
		tables = []*table{t}
	}
	d.current = newVersion(newMemtable(d.opts.Rand.Uint64(), seg), nil, tables)
	d.mu.Unlock()
	for _, ot := range old.tables {
		ot.gone.Store(true)
	}
	old.unref()
	t.unref()
	if t.meta.count == 0 {
		os.Remove(path)
	}
	d.bgCond.Broadcast()
	return d.wal.DeleteBefore(seg)
}

func (d *db) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.bgCond.Broadcast()
	d.mu.Unlock()
	d.bg.Wait()

	err := d.wal.Close()
	d.mu.Lock()
	d.current.unref()
	d.mu.Unlock()
	return err
}
