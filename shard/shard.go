// Package shard splits the key space into hash ranges served by separate
// Raft groups and routes requests to them.
package shard

import (
	"hash/fnv"
	"math"

	"github.com/arifisme/keystone/proto"
)

const (
	// MetaGroup holds the shard map. It is an ordinary key-value group
	// that serves no user keys.
	MetaGroup uint64 = 1
	// Shards is the fixed number of hash ranges.
	Shards = 16
)

// mapKey is where the shard map lives inside the meta group. The leading
// NUL keeps it clear of any key a client would write.
var mapKey = []byte("\x00keystone/shardmap")

// ShardOf maps a key to its hash range: the 64-bit hash space is cut into
// Shards equal intervals. FNV alone leaves the high bits of short, similar
// keys nearly constant, so the hash is run through a final avalanche step
// before the top bits pick the range.
func ShardOf(key []byte) int {
	f := fnv.New64a()
	f.Write(key)
	h := f.Sum64()
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return int(h / (math.MaxUint64/Shards + 1))
}

// DefaultMap deals shards round-robin over groups, which must be sorted
// and must not include the meta group.
func DefaultMap(groups []uint64) *pb.ShardMap {
	m := &pb.ShardMap{Groups: make([]uint64, Shards)}
	for i := range m.Groups {
		m.Groups[i] = groups[i%len(groups)]
	}
	return m
}
