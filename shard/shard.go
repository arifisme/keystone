// Package shard splits the key space into hash ranges served by separate
// Raft groups and routes requests to them.
package shard

import (
	"github.com/arifisme/keystone/internal/keyhash"
	"github.com/arifisme/keystone/proto"
)

const (
	// MetaGroup holds the shard map. It is an ordinary key-value group
	// that serves no user keys.
	MetaGroup uint64 = 1
	Shards           = keyhash.Shards
)

// mapKey is where the shard map lives inside the meta group. The leading
// NUL keeps it clear of any key a client would write.
var mapKey = []byte("\x00keystone/shardmap")

func ShardOf(key []byte) int {
	return keyhash.ShardOf(key)
}

// DefaultMap deals shards round-robin over groups, which must be sorted
// and must not include the meta group.
func DefaultMap(groups []uint64) *pb.ShardMap {
	m := &pb.ShardMap{Groups: make([]uint64, Shards), Moving: make([]uint64, Shards)}
	for i := range m.Groups {
		m.Groups[i] = groups[i%len(groups)]
	}
	return m
}
