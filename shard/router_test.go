package shard

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/arifisme/keystone/kv"
	"github.com/arifisme/keystone/proto"
	"github.com/arifisme/keystone/raft"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

// shardedCluster is nine nodes: three shard groups of three replicas,
// with the meta group co-hosted on the first three.
type shardedCluster struct {
	t      *testing.T
	peers  map[raft.NodeID]string
	groups map[uint64][]raft.NodeID
	dirs   map[raft.NodeID]string
	nodes  map[raft.NodeID]*kv.Node
}

func startSharded(t *testing.T) *shardedCluster {
	t.Helper()
	c := &shardedCluster{t: t, peers: map[raft.NodeID]string{}, dirs: map[raft.NodeID]string{}, nodes: map[raft.NodeID]*kv.Node{}}
	for i := 1; i <= 9; i++ {
		c.peers[raft.NodeID(i)] = freeAddr(t)
		c.dirs[raft.NodeID(i)] = t.TempDir()
	}
	c.groups = map[uint64][]raft.NodeID{
		MetaGroup: {1, 2, 3},
		2:         {1, 2, 3},
		3:         {4, 5, 6},
		4:         {7, 8, 9},
	}
	for id := range c.peers {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func (c *shardedCluster) start(id raft.NodeID) {
	c.t.Helper()
	n, err := kv.Open(kv.NodeOptions{
		ID:            id,
		Peers:         c.peers,
		Groups:        c.groups,
		Dir:           c.dirs[id],
		Addr:          c.peers[id],
		Rand:          rand.New(rand.NewSource(int64(id))),
		TickInterval:  10 * time.Millisecond,
		ElectionTick:  10,
		HeartbeatTick: 1,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	r, err := NewRouter(n, c.groups)
	if err != nil {
		c.t.Fatal(err)
	}
	n.Serve(r)
	c.nodes[id] = n
}

func (c *shardedCluster) leaderOf(group uint64) raft.NodeID {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range c.groups[group] {
			if n, ok := c.nodes[id]; ok && n.Group(group).Status().State == raft.Leader {
				return id
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("group %d has no leader", group)
	return 0
}

func (c *shardedCluster) dial(ids ...raft.NodeID) *kv.Client {
	c.t.Helper()
	var eps []string
	for _, id := range ids {
		eps = append(eps, c.peers[id])
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cl, err := kv.Dial(ctx, eps, kv.ClientOptions{AttemptTimeout: time.Second})
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(cl.Close)
	return cl
}

func TestKeysLandInTheGroupTheShardMapNames(t *testing.T) {
	c := startSharded(t)
	cl := c.dial(5, 9, 1)
	ctx := context.Background()
	for i := 0; i < 60; i++ {
		if err := cl.Put(ctx, []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	m := DefaultMap([]uint64{2, 3, 4})
	direct := map[uint64]*kv.Client{}
	for _, g := range []uint64{2, 3, 4} {
		var eps []string
		for _, id := range c.groups[g] {
			eps = append(eps, c.peers[id])
		}
		dc, err := kv.NewClient(eps, kv.ClientOptions{Group: g, AttemptTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		defer dc.Close()
		direct[g] = dc
	}
	hits := map[uint64]int{}
	for i := 0; i < 60; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		want := m.Groups[ShardOf(key)]
		for g, dc := range direct {
			var res *pb.GetResponse
			err := dc.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
				var err error
				res, err = cli.Get(ctx, &pb.GetRequest{Key: key, Group: g})
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			if res.Found != (g == want) {
				t.Fatalf("%s found=%v in group %d, shard map says group %d", key, res.Found, g, want)
			}
			if res.Found {
				hits[g]++
			}
		}
	}
	if len(hits) != 3 {
		t.Fatalf("keys spread over %d groups: %v", len(hits), hits)
	}
	v, found, err := cl.Get(ctx, []byte("key7"), pb.ReadMode_READ_INDEX)
	if err != nil || !found || string(v) != "7" {
		t.Fatalf("routed get = %q %v %v", v, found, err)
	}
}

func TestScanMergesAcrossShardsInOrder(t *testing.T) {
	c := startSharded(t)
	cl := c.dial(4)
	ctx := context.Background()
	for ch := 'a'; ch <= 'z'; ch++ {
		if err := cl.Put(ctx, []byte(string(ch)), []byte(string(ch))); err != nil {
			t.Fatal(err)
		}
	}
	kvs, err := cl.Scan(ctx, nil, nil, 0, pb.ReadMode_READ_INDEX)
	if err != nil || len(kvs) != 26 {
		t.Fatalf("scan = %d keys, %v", len(kvs), err)
	}
	for i, kv := range kvs {
		if string(kv.Key) != string(rune('a'+i)) {
			t.Fatalf("position %d = %q", i, kv.Key)
		}
	}
	kvs, err = cl.Scan(ctx, []byte("f"), []byte("k"), 3, pb.ReadMode_LOG)
	if err != nil || len(kvs) != 3 || string(kvs[0].Key) != "f" || string(kvs[2].Key) != "h" {
		t.Fatalf("bounded scan = %v, %v", kvs, err)
	}
}

func TestKillingOneShardsLeaderLeavesOtherShardsServing(t *testing.T) {
	c := startSharded(t)
	cl := c.dial(2, 5, 8)
	ctx := context.Background()
	m := DefaultMap([]uint64{2, 3, 4})
	keysIn := func(g uint64) []string {
		var out []string
		for i := 0; len(out) < 5; i++ {
			k := fmt.Sprintf("k%d", i)
			if m.Groups[ShardOf([]byte(k))] == g {
				out = append(out, k)
			}
		}
		return out
	}
	for _, g := range []uint64{2, 3, 4} {
		for _, k := range keysIn(g) {
			if err := cl.Put(ctx, []byte(k), []byte("v")); err != nil {
				t.Fatal(err)
			}
		}
	}
	victim := c.leaderOf(3)
	c.nodes[victim].Stop()
	delete(c.nodes, victim)

	for _, k := range keysIn(4) {
		start := time.Now()
		if _, found, err := cl.Get(ctx, []byte(k), pb.ReadMode_READ_INDEX); err != nil || !found {
			t.Fatalf("group 4 key %s: %v %v", k, found, err)
		}
		if d := time.Since(start); d > 500*time.Millisecond {
			t.Fatalf("group 4 read took %v while group 3 was failing over", d)
		}
	}
	for _, k := range keysIn(3) {
		if err := cl.Put(ctx, []byte(k), []byte("after")); err != nil {
			t.Fatalf("group 3 write after leader loss: %v", err)
		}
	}
	if newLeader := c.leaderOf(3); newLeader == victim {
		t.Fatal("dead node reported as leader")
	}
}
