package kv

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/arifisme/keystone/proto"
	"github.com/arifisme/keystone/raft"
)

type testCluster struct {
	t     *testing.T
	peers map[raft.NodeID]string
	dirs  map[raft.NodeID]string
	nodes map[raft.NodeID]*Node
	snap  uint64
}

func startCluster(t *testing.T, n int, snap uint64) *testCluster {
	t.Helper()
	c := &testCluster{t: t, peers: map[raft.NodeID]string{}, dirs: map[raft.NodeID]string{}, nodes: map[raft.NodeID]*Node{}, snap: snap}
	for i := 1; i <= n; i++ {
		id := raft.NodeID(i)
		c.peers[id] = freeAddr(t)
		c.dirs[id] = t.TempDir()
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

func (c *testCluster) start(id raft.NodeID) {
	c.t.Helper()
	var members []raft.NodeID
	for m := range c.peers {
		members = append(members, m)
	}
	n, err := Open(NodeOptions{
		ID:                id,
		Peers:             c.peers,
		Groups:            map[uint64][]raft.NodeID{1: members},
		Dir:               c.dirs[id],
		Addr:              c.peers[id],
		Rand:              rand.New(rand.NewSource(int64(id))),
		TickInterval:      10 * time.Millisecond,
		ElectionTick:      10,
		HeartbeatTick:     1,
		SnapshotThreshold: c.snap,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	n.Serve(n.Server())
	c.nodes[id] = n
}

func (c *testCluster) kill(id raft.NodeID) {
	c.nodes[id].Stop()
	delete(c.nodes, id)
}

func (c *testCluster) endpoints() []string {
	var out []string
	for _, addr := range c.peers {
		out = append(out, addr)
	}
	return out
}

func (c *testCluster) waitLeader() *Node {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			if n.Group(1).Status().State == raft.Leader {
				return n
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return nil
}

func (c *testCluster) dial() *Client {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cl, err := Dial(ctx, c.endpoints(), ClientOptions{AttemptTimeout: 500 * time.Millisecond})
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(cl.Close)
	return cl
}

func TestClientOperationsThroughGRPC(t *testing.T) {
	c := startCluster(t, 3, 0)
	c.waitLeader()
	cl := c.dial()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := cl.Put(ctx, []byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for _, mode := range []pb.ReadMode{pb.ReadMode_READ_INDEX, pb.ReadMode_LOG} {
		v, found, err := cl.Get(ctx, []byte("k03"), mode)
		if err != nil || !found || string(v) != "v3" {
			t.Fatalf("get mode %v = %q %v %v", mode, v, found, err)
		}
		if _, found, err := cl.Get(ctx, []byte("nope"), mode); err != nil || found {
			t.Fatalf("missing key found=%v err=%v", found, err)
		}
		kvs, err := cl.Scan(ctx, []byte("k02"), []byte("k05"), 0, mode)
		if err != nil || len(kvs) != 3 || string(kvs[0].Key) != "k02" || string(kvs[2].Value) != "v4" {
			t.Fatalf("scan mode %v = %v, %v", mode, kvs, err)
		}
	}
	swapped, _, _, err := cl.Cas(ctx, []byte("k00"), []byte("wrong"), []byte("x"))
	if err != nil || swapped {
		t.Fatalf("cas with wrong expectation swapped=%v err=%v", swapped, err)
	}
	swapped, current, found, err := cl.Cas(ctx, []byte("k00"), []byte("v0"), []byte("x"))
	if err != nil || !swapped || !found || string(current) != "v0" {
		t.Fatalf("cas = %v %q %v %v", swapped, current, found, err)
	}
	if err := cl.Delete(ctx, []byte("k00")); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := cl.Get(ctx, []byte("k00"), pb.ReadMode_READ_INDEX); found {
		t.Fatal("deleted key still found")
	}
}

func TestClientFollowsNewLeaderAfterLeaderDies(t *testing.T) {
	c := startCluster(t, 5, 0)
	old := c.waitLeader()
	cl := c.dial()
	ctx := context.Background()
	if err := cl.Put(ctx, []byte("before"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	oldID := old.Group(1).Status().ID
	c.kill(oldID)
	if err := cl.Put(ctx, []byte("after"), []byte("2")); err != nil {
		t.Fatalf("put after leader death: %v", err)
	}
	if v, found, err := cl.Get(ctx, []byte("before"), pb.ReadMode_READ_INDEX); err != nil || !found || string(v) != "1" {
		t.Fatalf("before = %q %v %v", v, found, err)
	}
	c.start(oldID)
	deadline := time.Now().Add(5 * time.Second)
	for {
		l := c.waitLeader()
		if c.nodes[oldID].Group(1).Status().Applied >= l.Group(1).Status().Commit {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted node did not catch up: %+v vs %+v", c.nodes[oldID].Group(1).Status(), l.Group(1).Status())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if v, found, _ := c.nodes[oldID].Group(1).sm.Get([]byte("after")); !found || string(v) != "2" {
		t.Fatalf("restarted node missing write made while down: %q %v", v, found)
	}
}

func TestNodeRejoinsThroughSnapshot(t *testing.T) {
	c := startCluster(t, 3, 40)
	l := c.waitLeader()
	var follower raft.NodeID
	for id := range c.nodes {
		if id != l.Group(1).Status().ID {
			follower = id
			break
		}
	}
	c.kill(follower)
	cl := c.dial()
	ctx := context.Background()
	for i := 0; i < 150; i++ {
		if err := cl.Put(ctx, []byte(fmt.Sprintf("k%03d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if s := l.Group(1).Status(); s.FirstIndex <= 1 {
		t.Fatalf("leader never compacted: %+v", s)
	}
	c.start(follower)
	deadline := time.Now().Add(10 * time.Second)
	for c.nodes[follower].Group(1).Status().Applied < l.Group(1).Status().Commit {
		if time.Now().After(deadline) {
			t.Fatalf("follower stuck: %+v", c.nodes[follower].Group(1).Status())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if s := c.nodes[follower].Group(1).Status(); s.FirstIndex <= 1 {
		t.Fatalf("follower caught up without a snapshot: %+v", s)
	}
	kvs, err := c.nodes[follower].Group(1).sm.Scan(nil, nil, 0)
	if err != nil || len(kvs) != 150 {
		t.Fatalf("follower has %d keys, %v", len(kvs), err)
	}
}

func TestRetriedWriteIsAppliedOnce(t *testing.T) {
	c := startCluster(t, 3, 0)
	l := c.waitLeader()
	conn, err := grpc.NewClient(c.peers[l.Group(1).Status().ID], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	raw := pb.NewKVClient(conn)
	ctx := context.Background()
	reg, err := raw.RegisterClient(ctx, &pb.RegisterRequest{})
	if err != nil {
		t.Fatal(err)
	}
	sess := &pb.Session{ClientId: reg.ClientId, Seq: 1}
	if _, err := raw.Cas(ctx, &pb.CasRequest{Session: sess, Key: []byte("k"), Value: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	// The same request again, as a client whose reply was lost would send.
	res, err := raw.Cas(ctx, &pb.CasRequest{Session: sess, Key: []byte("k"), Value: []byte("first")})
	if err != nil || !res.Success {
		t.Fatalf("retry = %+v, %v", res, err)
	}
	get, err := raw.Get(ctx, &pb.GetRequest{Key: []byte("k")})
	if err != nil || string(get.Value) != "first" {
		t.Fatalf("k = %q, %v", get.Value, err)
	}
}
