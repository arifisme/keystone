package kv

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"time"

	"google.golang.org/grpc"

	"github.com/arifisme/keystone/proto"
	"github.com/arifisme/keystone/raft"
	"github.com/arifisme/keystone/storage"
)

// Node is one server process. It hosts a replica of every Raft group it
// is a member of, each with its own engine and log, behind one listener
// shared by the peer transport and the client service.
type Node struct {
	cfg       NodeOptions
	groups    map[uint64]*Group
	transport *Transport
	grpc      *grpc.Server
	lis       net.Listener
	metrics   *metrics
	http      *http.Server
}

// Group is one Raft group's replica on this node.
type Group struct {
	ID   uint64
	db   storage.DB
	log  *raft.DiskStore
	sm   *StateMachine
	raft *raft.Node
}

type NodeOptions struct {
	ID    raft.NodeID
	Peers map[raft.NodeID]string
	// Groups maps each group to its members. The node hosts the groups
	// that list it.
	Groups map[uint64][]raft.NodeID
	Dir    string
	// Addr is the listen address; the entry in Peers for ID is what other
	// nodes and clients dial, which may differ behind NAT or Docker.
	Addr string
	// MetricsAddr serves Prometheus metrics and pprof when set.
	MetricsAddr string
	Rand        *rand.Rand

	TickInterval      time.Duration
	ElectionTick      int
	HeartbeatTick     int
	SnapshotThreshold uint64
	// MaxProposalBatch caps how many queued proposals share one log
	// append; zero means no cap.
	MaxProposalBatch int
}

// Open prepares every hosted group and the listener. Nothing is served
// until Serve is called with the client-facing service.
func Open(cfg NodeOptions) (*Node, error) {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 100 * time.Millisecond
	}
	if cfg.ElectionTick <= 0 {
		cfg.ElectionTick = 10
	}
	if cfg.HeartbeatTick <= 0 {
		cfg.HeartbeatTick = 1
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 10000
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(cfg.ID)))
	}
	n := &Node{cfg: cfg, groups: map[uint64]*Group{}, transport: NewTransport(cfg.ID, cfg.Peers)}
	n.metrics = newMetrics(n)
	for _, id := range sortedGroups(cfg.Groups) {
		members := cfg.Groups[id]
		if !contains(members, cfg.ID) {
			continue
		}
		g, err := n.openGroup(id, members)
		if err != nil {
			n.closeGroups()
			n.transport.Close()
			return nil, fmt.Errorf("group %d: %w", id, err)
		}
		n.groups[id] = g
	}
	if len(n.groups) == 0 {
		n.transport.Close()
		return nil, fmt.Errorf("node %d is not a member of any group", cfg.ID)
	}
	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		n.closeGroups()
		n.transport.Close()
		return nil, err
	}
	n.lis = lis
	n.grpc = grpc.NewServer(grpc.UnaryInterceptor(n.metrics.interceptor()))
	n.transport.Register(n.grpc)
	if cfg.MetricsAddr != "" {
		mlis, err := net.Listen("tcp", cfg.MetricsAddr)
		if err != nil {
			lis.Close()
			n.closeGroups()
			n.transport.Close()
			return nil, err
		}
		n.http = &http.Server{Handler: n.metrics.handler()}
		go n.http.Serve(mlis)
	}
	return n, nil
}

func (n *Node) openGroup(id uint64, members []raft.NodeID) (*Group, error) {
	cfg := n.cfg
	dir := filepath.Join(cfg.Dir, fmt.Sprintf("g%d", id))
	db, err := storage.Open(filepath.Join(dir, "kv"), storage.Options{
		Sync:   storage.SyncInterval(50 * time.Millisecond),
		Rand:   cfg.Rand,
		OnSync: func(d time.Duration) { n.metrics.fsync.Observe(d.Seconds()) },
	})
	if err != nil {
		return nil, fmt.Errorf("open engine: %w", err)
	}
	log, err := raft.OpenDiskStore(filepath.Join(dir, "raft"))
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("open raft log: %w", err)
	}
	sm, err := NewStateMachine(db)
	if err != nil {
		log.Close()
		db.Close()
		return nil, err
	}
	peers := append([]raft.NodeID(nil), members...)
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	rn, err := raft.NewNode(raft.NodeConfig{
		Config: raft.Config{
			ID:            cfg.ID,
			Peers:         peers,
			ElectionTick:  cfg.ElectionTick,
			HeartbeatTick: cfg.HeartbeatTick,
			Store:         log,
			Transport:     n.transport.Group(id),
			Rand:          rand.New(rand.NewSource(cfg.Rand.Int63())),
		},
		StateMachine:      sm,
		Clock:             raft.SystemClock{},
		TickInterval:      cfg.TickInterval,
		SnapshotThreshold: cfg.SnapshotThreshold,
		MaxProposalBatch:  cfg.MaxProposalBatch,
	})
	if err != nil {
		log.Close()
		db.Close()
		return nil, err
	}
	return &Group{ID: id, db: db, log: log, sm: sm, raft: rn}, nil
}

// Serve starts every group and the listener, exposing service to clients.
func (n *Node) Serve(service pb.KVServer) {
	pb.RegisterKVServer(n.grpc, service)
	for _, g := range n.groups {
		go g.raft.Run()
	}
	go n.metrics.monitor()
	go n.grpc.Serve(n.lis)
}

func (n *Node) ID() raft.NodeID {
	return n.cfg.ID
}

func (n *Node) Peers() map[raft.NodeID]string {
	return n.cfg.Peers
}

// Group returns the hosted replica of id, or nil.
func (n *Node) Group(id uint64) *Group {
	return n.groups[id]
}

// Groups lists hosted group ids in ascending order.
func (n *Node) Groups() []uint64 {
	return sortedGroups(n.groups)
}

func (n *Node) Server() *Server {
	return &Server{node: n}
}

func (g *Group) Status() raft.Status {
	return g.raft.Status()
}

func (n *Node) Stop() error {
	n.grpc.Stop()
	if n.http != nil {
		n.http.Close()
	}
	close(n.metrics.stop)
	for _, g := range n.groups {
		g.raft.Stop()
	}
	n.transport.Close()
	return n.closeGroups()
}

func (n *Node) closeGroups() error {
	var err error
	for _, g := range n.groups {
		if lerr := g.log.Close(); err == nil {
			err = lerr
		}
		if derr := g.db.Close(); err == nil {
			err = derr
		}
	}
	return err
}

func sortedGroups[T any](m map[uint64]T) []uint64 {
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func contains(ids []raft.NodeID, id raft.NodeID) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}
