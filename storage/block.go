package storage

import (
	"encoding/binary"
	"errors"
	"sort"
)

// Block layout:
//
//	entry*      shared uvarint | unshared uvarint | vlen uvarint |
//	            key[shared:] | value
//	restarts    uint32 offset of each restart entry
//	nrestarts   uint32
//
// An entry shares a prefix with the entry before it. Every restartInterval
// entries a restart point stores the full key, which is where seek starts
// its linear scan after binary searching the restart offsets.
const restartInterval = 16

var errBadBlock = errors.New("sstable: malformed block")

type blockBuilder struct {
	buf      []byte
	restarts []uint32
	lastKey  []byte
	count    int
}

func (b *blockBuilder) add(key, value []byte) {
	shared := 0
	if b.count%restartInterval == 0 {
		b.restarts = append(b.restarts, uint32(len(b.buf)))
	} else {
		n := len(key)
		if len(b.lastKey) < n {
			n = len(b.lastKey)
		}
		for shared < n && key[shared] == b.lastKey[shared] {
			shared++
		}
	}
	b.buf = binary.AppendUvarint(b.buf, uint64(shared))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(key)-shared))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(value)))
	b.buf = append(b.buf, key[shared:]...)
	b.buf = append(b.buf, value...)
	b.lastKey = append(b.lastKey[:0], key...)
	b.count++
}

func (b *blockBuilder) size() int {
	return len(b.buf) + 4*len(b.restarts) + 4
}

func (b *blockBuilder) empty() bool {
	return b.count == 0
}

func (b *blockBuilder) finish() []byte {
	for _, r := range b.restarts {
		b.buf = binary.LittleEndian.AppendUint32(b.buf, r)
	}
	b.buf = binary.LittleEndian.AppendUint32(b.buf, uint32(len(b.restarts)))
	return b.buf
}

func (b *blockBuilder) reset() {
	b.buf = b.buf[:0]
	b.restarts = b.restarts[:0]
	b.lastKey = b.lastKey[:0]
	b.count = 0
}

type blockIter struct {
	data     []byte
	restarts []uint32
	off      int
	nextOff  int
	k, v     []byte
	err      error
}

func newBlockIter(block []byte) (*blockIter, error) {
	if len(block) < 4 {
		return nil, errBadBlock
	}
	n := int(binary.LittleEndian.Uint32(block[len(block)-4:]))
	end := len(block) - 4 - 4*n
	if end < 0 {
		return nil, errBadBlock
	}
	restarts := make([]uint32, n)
	for i := range restarts {
		restarts[i] = binary.LittleEndian.Uint32(block[end+4*i:])
		if int(restarts[i]) > end {
			return nil, errBadBlock
		}
	}
	return &blockIter{data: block[:end], restarts: restarts, off: -1}, nil
}

func (it *blockIter) seekToFirst() {
	it.k = it.k[:0]
	it.nextOff = 0
	it.next()
}

func (it *blockIter) seek(target []byte) {
	// Find the last restart whose key is < target, then scan forward.
	i := sort.Search(len(it.restarts), func(i int) bool {
		k, _, _, ok := it.decodeAt(int(it.restarts[i]), nil)
		return !ok || compareInternalKey(k, target) >= 0
	})
	if i > 0 {
		i--
	}
	if len(it.restarts) == 0 {
		it.off = -1
		return
	}
	it.k = it.k[:0]
	it.nextOff = int(it.restarts[i])
	it.next()
	for it.valid() && compareInternalKey(it.k, target) < 0 {
		it.next()
	}
}

func (it *blockIter) valid() bool {
	return it.off >= 0 && it.err == nil
}

func (it *blockIter) next() {
	if it.nextOff >= len(it.data) {
		it.off = -1
		return
	}
	it.off = it.nextOff
	k, v, n, ok := it.decodeAt(it.off, it.k)
	if !ok {
		it.err = errBadBlock
		it.off = -1
		return
	}
	it.k, it.v, it.nextOff = k, v, it.off+n
}

// decodeAt decodes the entry at off, reusing prev for the shared prefix.
func (it *blockIter) decodeAt(off int, prev []byte) (key, value []byte, n int, ok bool) {
	d := it.data[off:]
	shared, n1 := binary.Uvarint(d)
	unshared, n2 := binary.Uvarint(d[n1:])
	vlen, n3 := binary.Uvarint(d[n1+n2:])
	hdr := n1 + n2 + n3
	if n1 <= 0 || n2 <= 0 || n3 <= 0 || int(shared) > len(prev) || hdr+int(unshared)+int(vlen) > len(d) {
		return nil, nil, 0, false
	}
	key = append(prev[:shared], d[hdr:hdr+int(unshared)]...)
	value = d[hdr+int(unshared) : hdr+int(unshared)+int(vlen)]
	return key, value, hdr + int(unshared) + int(vlen), true
}

func (it *blockIter) key() []byte   { return it.k }
func (it *blockIter) value() []byte { return it.v }
func (it *blockIter) close() error  { return it.err }
