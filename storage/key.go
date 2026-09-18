package storage

import (
	"bytes"
	"encoding/binary"
)

type keyKind uint8

const (
	kindDelete keyKind = 0
	kindPut    keyKind = 1
)

// An internal key is the user key followed by an 8-byte big-endian trailer
// of (sequence << 8 | kind). Ordering is user key ascending, then sequence
// descending, so the newest version of a key sorts first.
const trailerSize = 8

func makeInternalKey(dst, user []byte, seq uint64, kind keyKind) []byte {
	dst = append(dst[:0], user...)
	var t [trailerSize]byte
	binary.BigEndian.PutUint64(t[:], seq<<8|uint64(kind))
	return append(dst, t[:]...)
}

func userKey(ik []byte) []byte {
	return ik[:len(ik)-trailerSize]
}

func splitInternalKey(ik []byte) (user []byte, seq uint64, kind keyKind) {
	t := binary.BigEndian.Uint64(ik[len(ik)-trailerSize:])
	return ik[:len(ik)-trailerSize], t >> 8, keyKind(t & 0xff)
}

func compareInternalKey(a, b []byte) int {
	if c := bytes.Compare(userKey(a), userKey(b)); c != 0 {
		return c
	}
	ta := binary.BigEndian.Uint64(a[len(a)-trailerSize:])
	tb := binary.BigEndian.Uint64(b[len(b)-trailerSize:])
	switch {
	case ta > tb:
		return -1
	case ta < tb:
		return 1
	}
	return 0
}
