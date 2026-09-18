package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/arifisme/keystone/kv"
	"github.com/arifisme/keystone/raft"
)

// cluster is an in-process cluster on loopback with production timing.
type cluster struct {
	peers map[raft.NodeID]string
	dirs  map[raft.NodeID]string
	nodes map[raft.NodeID]*kv.Node
	batch int
	tmp   string
}

func startCluster(n int, batched bool, dir string) *cluster {
	c := &cluster{peers: map[raft.NodeID]string{}, dirs: map[raft.NodeID]string{}, nodes: map[raft.NodeID]*kv.Node{}}
	if !batched {
		c.batch = 1
	}
	if dir == "" {
		tmp, err := os.MkdirTemp("", "keystone-bench")
		if err != nil {
			fail(err)
		}
		c.tmp = tmp
		dir = tmp
	}
	for i := 1; i <= n; i++ {
		id := raft.NodeID(i)
		c.peers[id] = freeAddr()
		c.dirs[id] = filepath.Join(dir, fmt.Sprintf("n%d", i))
	}
	for id := range c.peers {
		c.start(id)
	}
	return c
}

func (c *cluster) start(id raft.NodeID) {
	var members []raft.NodeID
	for m := range c.peers {
		members = append(members, m)
	}
	n, err := kv.Open(kv.NodeOptions{
		ID:               id,
		Peers:            c.peers,
		Groups:           map[uint64][]raft.NodeID{1: members},
		Dir:              c.dirs[id],
		Addr:             c.peers[id],
		MaxProposalBatch: c.batch,
	})
	if err != nil {
		fail(err)
	}
	n.Serve(n.Server())
	c.nodes[id] = n
}

func (c *cluster) electionTimeout() time.Duration {
	return time.Second
}

func (c *cluster) leader() raft.NodeID {
	for id, n := range c.nodes {
		if n.Group(1).Status().State == raft.Leader {
			return id
		}
	}
	return 0
}

func (c *cluster) waitLeader() {
	for c.leader() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *cluster) kill(id raft.NodeID) {
	c.nodes[id].Stop()
	delete(c.nodes, id)
}

func (c *cluster) endpoints() []string {
	var out []string
	for _, addr := range c.peers {
		out = append(out, addr)
	}
	return out
}

func (c *cluster) dial() *kv.Client {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := kv.Dial(ctx, c.endpoints(), kv.ClientOptions{AttemptTimeout: 5 * time.Second})
	if err != nil {
		fail(err)
	}
	return cl
}

func (c *cluster) stop() {
	for _, n := range c.nodes {
		n.Stop()
	}
	if c.tmp != "" {
		os.RemoveAll(c.tmp)
	}
}

func freeAddr() string {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fail(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
