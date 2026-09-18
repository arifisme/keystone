package storage

import "sync/atomic"

// memtable is the mutable in-memory tier. Each memtable corresponds to
// exactly one WAL segment, so a flushed memtable's segment can be deleted.
type memtable struct {
	list *skiplist
	seg  uint64
	size atomic.Int64
}

func newMemtable(seed, seg uint64) *memtable {
	return &memtable{list: newSkiplist(seed), seg: seg}
}

func (m *memtable) put(seq uint64, kind keyKind, key, value []byte) {
	ik := makeInternalKey(nil, key, seq, kind)
	var v []byte
	if kind == kindPut {
		v = append([]byte(nil), value...)
	}
	m.list.insert(ik, v)
	m.size.Add(int64(len(ik) + len(v) + 64))
}

// get returns the newest version of key with sequence <= seq. found is
// false when no version qualifies; deleted reports a tombstone.
func (m *memtable) get(key []byte, seq uint64) (value []byte, found, deleted bool) {
	n := m.list.seek(makeInternalKey(nil, key, seq, kindPut))
	if n == nil {
		return nil, false, false
	}
	user, _, kind := splitInternalKey(n.key)
	if string(user) != string(key) {
		return nil, false, false
	}
	if kind == kindDelete {
		return nil, true, true
	}
	return n.value, true, false
}

func (m *memtable) iter() *skiplistIter {
	return &skiplistIter{list: m.list}
}
