package storage

import (
	"encoding/binary"
	"errors"
)

// Batch is a set of writes applied atomically: all of them are in the WAL
// record or none are.
type Batch struct {
	ops  []batchOp
	size int
}

type batchOp struct {
	kind  keyKind
	key   []byte
	value []byte
}

func (b *Batch) Put(key, value []byte) {
	b.ops = append(b.ops, batchOp{kindPut, key, value})
	b.size += len(key) + len(value)
}

func (b *Batch) Delete(key []byte) {
	b.ops = append(b.ops, batchOp{kindDelete, key, nil})
	b.size += len(key)
}

func (b *Batch) Len() int {
	return len(b.ops)
}

// WAL record payload:
//
//	seq uint64 | count uint32 | (kind uint8 | klen uvarint | key |
//	                            vlen uvarint | value)*
//
// seq is the sequence of the first op; the i-th op has seq+i.
func (b *Batch) encode(dst []byte, seq uint64) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, seq)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(b.ops)))
	for _, op := range b.ops {
		dst = append(dst, byte(op.kind))
		dst = binary.AppendUvarint(dst, uint64(len(op.key)))
		dst = append(dst, op.key...)
		dst = binary.AppendUvarint(dst, uint64(len(op.value)))
		dst = append(dst, op.value...)
	}
	return dst
}

var errBadBatch = errors.New("storage: malformed batch record")

func decodeBatch(p []byte) (seq uint64, b *Batch, err error) {
	if len(p) < 12 {
		return 0, nil, errBadBatch
	}
	seq = binary.LittleEndian.Uint64(p)
	count := binary.LittleEndian.Uint32(p[8:])
	p = p[12:]
	b = &Batch{}
	for i := uint32(0); i < count; i++ {
		if len(p) < 1 {
			return 0, nil, errBadBatch
		}
		kind := keyKind(p[0])
		p = p[1:]
		klen, n := binary.Uvarint(p)
		if n <= 0 || uint64(len(p)-n) < klen {
			return 0, nil, errBadBatch
		}
		key := p[n : n+int(klen)]
		p = p[n+int(klen):]
		vlen, n := binary.Uvarint(p)
		if n <= 0 || uint64(len(p)-n) < vlen {
			return 0, nil, errBadBatch
		}
		value := p[n : n+int(vlen)]
		p = p[n+int(vlen):]
		b.ops = append(b.ops, batchOp{kind, key, value})
	}
	return seq, b, nil
}
