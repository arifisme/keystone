package storage

import (
	"io"
	"sync"
)

// memDB is a DB that lives entirely in one memtable. It exists for the
// simulator, which must never touch a disk, and shares the snapshot
// format with the real engine so a snapshot moves between the two.
type memDB struct {
	mu     sync.RWMutex
	mem    *memtable
	seq    uint64
	seed   uint64
	closed bool
}

func NewMemory(seed uint64) DB {
	return &memDB{mem: newMemtable(seed, 0), seed: seed}
}

func (d *memDB) Get(key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return nil, ErrClosed
	}
	if val, found, deleted := d.mem.get(key, d.seq); found {
		return result(val, deleted)
	}
	return nil, ErrNotFound
}

func (d *memDB) Put(key, value []byte) error {
	var b Batch
	b.Put(key, value)
	return d.Write(&b)
}

func (d *memDB) Delete(key []byte) error {
	var b Batch
	b.Delete(key)
	return d.Write(&b)
}

func (d *memDB) Write(b *Batch) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	for _, op := range b.ops {
		d.seq++
		d.mem.put(d.seq, op.kind, op.key, op.value)
	}
	return nil
}

func (d *memDB) Scan(start, end []byte) Iterator {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return &dbIter{err: ErrClosed}
	}
	it := &dbIter{seq: d.seq, m: newMergeIter([]internalIter{d.mem.iter()}), end: end}
	it.m.seek(makeInternalKey(nil, start, d.seq, kindPut))
	return it
}

func (d *memDB) Sync() error {
	return nil
}

func (d *memDB) Export(w io.Writer) error {
	d.mu.RLock()
	mem, seq := d.mem, d.seq
	d.mu.RUnlock()
	return exportIter(mem.iter(), seq, w, 4096)
}

func (d *memDB) Restore(r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	t, err := openTableBytes(data)
	if err != nil {
		return err
	}
	defer t.unref()
	d.mu.Lock()
	defer d.mu.Unlock()
	mem := newMemtable(d.seed, 0)
	var maxSeq uint64
	it := t.iter()
	for it.seekToFirst(); it.valid(); it.next() {
		user, seq, kind := splitInternalKey(it.key())
		mem.put(seq, kind, user, it.value())
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	if err := it.close(); err != nil {
		return err
	}
	d.mem = mem
	if maxSeq > d.seq {
		d.seq = maxSeq
	}
	return nil
}

func (d *memDB) Stats() Stats {
	return Stats{}
}

func (d *memDB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	return nil
}
