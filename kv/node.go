package kv

import (
	"fmt"
	"math/rand"
	"net"
	"path/filepath"
	"sort"
	"time"

	"google.golang.org/grpc"

	"github.com/arifisme/keystone/raft"
	"github.com/arifisme/keystone/storage"
)

// Node is one server process: an engine, a Raft node and the gRPC surface
// for both peers and clients.
type Node struct {
	cfg       NodeOptions
	db        storage.DB
	log       *raft.DiskStore
	sm        *StateMachine
	raft      *raft.Node
	transport *Transport
	grpc      *grpc.Server
}

type NodeOptions struct {
	ID    raft.NodeID
	Peers map[raft.NodeID]string
	Dir   string
	// Addr is the listen address; the entry in Peers for ID is what other
	// nodes and clients dial, which may differ behind NAT or Docker.
	Addr string
	Rand *rand.Rand

	TickInterval      time.Duration
	ElectionTick      int
	HeartbeatTick     int
	SnapshotThreshold uint64
}

func Start(cfg NodeOptions) (*Node, error) {
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
	db, err := storage.Open(filepath.Join(cfg.Dir, "kv"), storage.Options{Sync: storage.SyncInterval(50 * time.Millisecond), Rand: cfg.Rand})
	if err != nil {
		return nil, fmt.Errorf("open engine: %w", err)
	}
	log, err := raft.OpenDiskStore(filepath.Join(cfg.Dir, "raft"))
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
	var ids []raft.NodeID
	for id := range cfg.Peers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	transport := NewTransport(cfg.ID, cfg.Peers)
	rn, err := raft.NewNode(raft.NodeConfig{
		Config: raft.Config{
			ID:            cfg.ID,
			Peers:         ids,
			ElectionTick:  cfg.ElectionTick,
			HeartbeatTick: cfg.HeartbeatTick,
			Store:         log,
			Transport:     transport,
			Rand:          cfg.Rand,
		},
		StateMachine:      sm,
		Clock:             raft.SystemClock{},
		TickInterval:      cfg.TickInterval,
		SnapshotThreshold: cfg.SnapshotThreshold,
	})
	if err != nil {
		transport.Close()
		log.Close()
		db.Close()
		return nil, err
	}
	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		transport.Close()
		log.Close()
		db.Close()
		return nil, err
	}
	g := grpc.NewServer()
	transport.Register(g)
	NewServer(rn, sm, cfg.Peers).Register(g)
	n := &Node{cfg: cfg, db: db, log: log, sm: sm, raft: rn, transport: transport, grpc: g}
	go rn.Run()
	go g.Serve(lis)
	return n, nil
}

func (n *Node) Status() raft.Status {
	return n.raft.Status()
}

func (n *Node) Stop() error {
	n.grpc.Stop()
	n.raft.Stop()
	n.transport.Close()
	err := n.log.Close()
	if derr := n.db.Close(); err == nil {
		err = derr
	}
	return err
}
