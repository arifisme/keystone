package storage

import "sync/atomic"

// version is an immutable snapshot of the tiers a reader consults. Tables
// are newest first. A version holds one reference on each of its tables;
// the last reader to release the version releases them.
type version struct {
	mem    *memtable
	imm    []*memtable
	tables []*table
	refs   atomic.Int32
}

func newVersion(mem *memtable, imm []*memtable, tables []*table) *version {
	v := &version{mem: mem, imm: imm, tables: tables}
	v.refs.Store(1)
	for _, t := range tables {
		t.ref()
	}
	return v
}

func (v *version) ref() {
	v.refs.Add(1)
}

func (v *version) unref() {
	if v.refs.Add(-1) != 0 {
		return
	}
	for _, t := range v.tables {
		t.unref()
	}
}

func (v *version) withMemtable(mem *memtable) *version {
	imm := append(append([]*memtable(nil), v.imm...), v.mem)
	return newVersion(mem, imm, v.tables)
}

func (v *version) withFlushed(flushed *memtable, t *table) *version {
	var imm []*memtable
	for _, m := range v.imm {
		if m != flushed {
			imm = append(imm, m)
		}
	}
	tables := v.tables
	if t != nil {
		tables = append([]*table{t}, v.tables...)
	}
	return newVersion(v.mem, imm, tables)
}

// withCompacted replaces the contiguous run inputs with out, keeping age
// order intact.
func (v *version) withCompacted(inputs []*table, out *table) *version {
	tables := make([]*table, 0, len(v.tables))
	replaced := false
	for _, t := range v.tables {
		if isInput(t, inputs) {
			if !replaced && out != nil {
				tables = append(tables, out)
			}
			replaced = true
			continue
		}
		tables = append(tables, t)
	}
	return newVersion(v.mem, v.imm, tables)
}

func isInput(t *table, inputs []*table) bool {
	for _, in := range inputs {
		if in == t {
			return true
		}
	}
	return false
}
