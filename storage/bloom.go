package storage

import "encoding/binary"

// bloomBitsPerKey of 10 with 7 probes gives roughly a 1% false positive
// rate.
const (
	bloomBitsPerKey = 10
	bloomProbes     = 7
)

// bloomFilter layout: one byte holding the probe count, then the bit array.
// Probe i tests bit (h1 + i*h2) mod nbits, with h1 and h2 the two halves
// of a 64-bit hash of the user key.
type bloomFilter []byte

func buildBloom(keys [][]byte) bloomFilter {
	nbits := len(keys) * bloomBitsPerKey
	if nbits < 64 {
		nbits = 64
	}
	nbytes := (nbits + 7) / 8
	nbits = nbytes * 8
	f := make(bloomFilter, 1+nbytes)
	f[0] = bloomProbes
	bits := f[1:]
	for _, k := range keys {
		h1, h2 := bloomHashes(k)
		for i := uint32(0); i < bloomProbes; i++ {
			pos := (h1 + i*h2) % uint32(nbits)
			bits[pos/8] |= 1 << (pos % 8)
		}
	}
	return f
}

func (f bloomFilter) mayContain(key []byte) bool {
	if len(f) < 2 {
		return true
	}
	probes := uint32(f[0])
	bits := f[1:]
	nbits := uint32(len(bits) * 8)
	h1, h2 := bloomHashes(key)
	for i := uint32(0); i < probes; i++ {
		pos := (h1 + i*h2) % nbits
		if bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

func bloomHashes(key []byte) (uint32, uint32) {
	h := hash64(key, 0x9ae16a3b2f90404f)
	return uint32(h), uint32(h>>32) | 1
}

// hash64 is MurmurHash64A.
func hash64(data []byte, seed uint64) uint64 {
	const m = 0xc6a4a7935bd1e995
	const r = 47
	h := seed ^ (uint64(len(data)) * m)
	for len(data) >= 8 {
		k := binary.LittleEndian.Uint64(data)
		k *= m
		k ^= k >> r
		k *= m
		h ^= k
		h *= m
		data = data[8:]
	}
	if len(data) > 0 {
		var tail [8]byte
		copy(tail[:], data)
		h ^= binary.LittleEndian.Uint64(tail[:])
		h *= m
	}
	h ^= h >> r
	h *= m
	h ^= h >> r
	return h
}
