// Package keyhash maps keys to shards. It lives apart from shard so the
// state machine can tell which shard a key belongs to without depending
// on the router.
package keyhash

import (
	"hash/fnv"
	"math"
)

// Shards is the fixed number of hash ranges.
const Shards = 16

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
