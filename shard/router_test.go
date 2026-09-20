package shard

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

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
	c.t.Cleanup(r.Close)
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

func (c *shardedCluster) direct(g uint64) *kv.Client {
	c.t.Helper()
	var eps []string
	for _, id := range c.groups[g] {
		eps = append(eps, c.peers[id])
	}
	dc, err := kv.NewClient(eps, kv.ClientOptions{Group: g, AttemptTimeout: time.Second})
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(dc.Close)
	return dc
}

func directGet(t *testing.T, dc *kv.Client, g uint64, key string) (string, bool, error) {
	t.Helper()
	var res *pb.GetResponse
	err := dc.Do(context.Background(), func(ctx context.Context, cli pb.KVClient) error {
		var err error
		res, err = cli.Get(ctx, &pb.GetRequest{Key: []byte(key), Group: g})
		return err
	})
	if err != nil {
		return "", false, err
	}
	return string(res.Value), res.Found, nil
}

func TestMoveShardRelocatesKeysAndRoutingFollows(t *testing.T) {
	c := startSharded(t)
	cl := c.dial(3, 6)
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		if err := cl.Put(ctx, []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	m := DefaultMap([]uint64{2, 3, 4})
	shard := ShardOf([]byte("key1"))
	src := m.Groups[shard]
	dest := uint64(2)
	if src == dest {
		dest = 3
	}
	var moving []string
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("key%d", i)
		if ShardOf([]byte(k)) == shard {
			moving = append(moving, k)
		}
	}
	if len(moving) == 0 {
		t.Fatal("no keys in the chosen shard")
	}

	// Keep writing one key in the moving shard the whole time; every
	// acknowledged write must be visible afterwards.
	stop := make(chan struct{})
	var lastAcked string
	var writes, failures int
	done := make(chan struct{})
	go func() {
		defer close(done)
		w := c.dial(9)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			v := fmt.Sprintf("w%d", i)
			wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := w.Put(wctx, []byte(moving[0]), []byte(v))
			cancel()
			if err != nil {
				failures++
				continue
			}
			lastAcked = v
			writes++
		}
	}()

	err := cl.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		_, err := cli.MoveShard(ctx, &pb.MoveShardRequest{Shard: uint32(shard), Group: dest})
		return err
	})
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	close(stop)
	<-done
	if writes == 0 {
		t.Fatal("writer never succeeded")
	}
	t.Logf("shard %d: group %d -> %d, %d keys, writer: %d ok, %d failed during the move", shard, src, dest, len(moving), writes, failures)

	srcClient, destClient := c.direct(src), c.direct(dest)
	for _, k := range moving {
		want := k[3:]
		if k == moving[0] {
			want = lastAcked
		}
		v, found, err := directGet(t, destClient, dest, k)
		if err != nil || !found || v != want {
			t.Fatalf("%s in destination = %q %v %v, want %q", k, v, found, err, want)
		}
		if _, _, err := directGet(t, srcClient, src, k); !isMoving(err) {
			t.Fatalf("%s still readable in source: %v", k, err)
		}
		rv, rfound, err := cl.Get(ctx, []byte(k), pb.ReadMode_READ_INDEX)
		if err != nil || !rfound || string(rv) != want {
			t.Fatalf("routed %s = %q %v %v", k, rv, rfound, err)
		}
	}
	var sm *pb.ShardMap
	cl.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		var err error
		sm, err = cli.GetShardMap(ctx, &pb.ShardMapRequest{})
		return err
	})
	if sm.Groups[shard] != dest || sm.Moving[shard] != 0 {
		t.Fatalf("map entry = %d moving %d", sm.Groups[shard], sm.Moving[shard])
	}
	if err := cl.Put(ctx, []byte(moving[1%len(moving)]), []byte("after")); err != nil {
		t.Fatal(err)
	}
	if v, found, _ := directGet(t, destClient, dest, moving[1%len(moving)]); !found || v != "after" {
		t.Fatalf("write after move landed elsewhere: %q %v", v, found)
	}
	kvs, err := cl.Scan(ctx, nil, nil, 0, pb.ReadMode_READ_INDEX)
	if err != nil || len(kvs) != 200 {
		t.Fatalf("scan after move: %d keys, %v", len(kvs), err)
	}
}

// The shard holds more than one gRPC message may, so neither a page of the
// source nor an import chunk can be cut by key count alone.
func TestMoveShardHoldingMoreThanOneMessageCanCarry(t *testing.T) {
	c := startSharded(t)
	cl := c.dial(3, 6)
	ctx := context.Background()
	shard := 5
	src := DefaultMap([]uint64{2, 3, 4}).Groups[shard]
	dest := uint64(2)
	if src == dest {
		dest = 3
	}
	pairs := keysInShard(shard, 16, 300<<10)
	for _, p := range pairs {
		if err := cl.Put(ctx, p.Key, p.Value); err != nil {
			t.Fatal(err)
		}
	}
	err := cl.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		_, err := cli.MoveShard(ctx, &pb.MoveShardRequest{Shard: uint32(shard), Group: dest})
		return err
	})
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	destClient := c.direct(dest)
	for _, p := range pairs {
		v, found, err := directGet(t, destClient, dest, string(p.Key))
		if err != nil || !found || len(v) != len(p.Value) {
			t.Fatalf("%s in destination: %d bytes, found %v, %v", p.Key, len(v), found, err)
		}
	}
}

// The test takes the first steps of a move by hand and stops, which is
// the state a router leaves behind when it dies in the middle of the copy.
func TestMoveShardInterruptedHalfwayIsFinishedByRunningItAgain(t *testing.T) {
	c := startSharded(t)
	cl := c.dial(3, 6)
	ctx := context.Background()
	shard := 5
	src := DefaultMap([]uint64{2, 3, 4}).Groups[shard]
	dest := uint64(2)
	if src == dest {
		dest = 3
	}
	pairs := keysInShard(shard, 20, 10)
	for _, p := range pairs {
		if err := cl.Put(ctx, p.Key, p.Value); err != nil {
			t.Fatal(err)
		}
	}

	meta, srcClient, destClient := c.direct(MetaGroup), c.direct(src), c.direct(dest)
	err := meta.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		got, err := cli.Get(ctx, &pb.GetRequest{Key: mapKey, Mode: pb.ReadMode_LOG, Group: MetaGroup})
		if err != nil {
			return err
		}
		var m pb.ShardMap
		if err := proto.Unmarshal(got.Value, &m); err != nil {
			return err
		}
		m.Moving[shard] = dest
		marked, _ := proto.Marshal(&m)
		res, err := cli.Cas(ctx, &pb.CasRequest{Key: mapKey, Expected: got.Value, Value: marked, Group: MetaGroup})
		if err == nil && !res.Success {
			t.Fatal("could not mark the map")
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	admin := func(dc *kv.Client, g uint64, cmd *pb.Command) {
		t.Helper()
		err := dc.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
			_, err := cli.Admin(ctx, &pb.AdminRequest{Group: g, Command: cmd})
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	admin(srcClient, src, &pb.Command{Op: &pb.Command_Freeze{Freeze: &pb.FreezeOp{Shard: uint32(shard)}}})
	admin(destClient, dest, &pb.Command{Op: &pb.Command_Import{Import: &pb.ImportOp{Shard: uint32(shard), Kvs: pairs[:10]}}})

	stuck, cancel := context.WithTimeout(ctx, time.Second)
	if err := cl.Put(stuck, pairs[0].Key, []byte("while stuck")); err == nil {
		t.Fatal("test setup: the shard should refuse writes until the move is finished")
	}
	cancel()

	// The client retries a refusal as if it were a leader change, so a
	// move that is refused shows up as this deadline passing.
	again, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	err = cl.Do(again, func(ctx context.Context, cli pb.KVClient) error {
		_, err := cli.MoveShard(ctx, &pb.MoveShardRequest{Shard: uint32(shard), Group: dest})
		return err
	})
	if err != nil {
		t.Fatalf("running the move again: %v", err)
	}
	for _, p := range pairs {
		v, found, err := directGet(t, destClient, dest, string(p.Key))
		if err != nil || !found || v != string(p.Value) {
			t.Fatalf("%s in destination = %q %v %v", p.Key, v, found, err)
		}
	}
	if err := cl.Put(ctx, pairs[0].Key, []byte("after")); err != nil {
		t.Fatalf("write after the move was finished: %v", err)
	}
	if v, found, _ := directGet(t, destClient, dest, string(pairs[0].Key)); !found || v != "after" {
		t.Fatalf("write after the move landed elsewhere: %q %v", v, found)
	}
}
