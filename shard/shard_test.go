package shard

import (
	"fmt"
	"testing"
)

func TestShardOfSpreadsShortSequentialKeys(t *testing.T) {
	for _, n := range []int{64, 10000} {
		seen := map[int]int{}
		for i := 0; i < n; i++ {
			s := ShardOf([]byte(fmt.Sprintf("key%d", i)))
			if s < 0 || s >= Shards {
				t.Fatalf("shard %d out of range", s)
			}
			seen[s]++
		}
		if len(seen) != Shards {
			t.Fatalf("%d keys hit only %d of %d shards: %v", n, len(seen), Shards, seen)
		}
		for s, c := range seen {
			if c < n/Shards/3 {
				t.Fatalf("%d keys: shard %d got only %d", n, s, c)
			}
		}
	}
}

func TestDefaultMapDealsShardsRoundRobin(t *testing.T) {
	m := DefaultMap([]uint64{2, 3, 4})
	counts := map[uint64]int{}
	for i, g := range m.Groups {
		if want := uint64(2 + i%3); g != want {
			t.Fatalf("shard %d -> group %d, want %d", i, g, want)
		}
		counts[g]++
	}
	if len(counts) != 3 {
		t.Fatalf("groups used: %v", counts)
	}
}
