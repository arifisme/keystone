package kv

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/arifisme/keystone/proto"
	"github.com/arifisme/keystone/raft"
)

// Transport carries Raft messages for every group on this node over one
// gRPC stream per peer. Send never blocks: each peer has a bounded queue
// and a message that does not fit is dropped, which Raft's retries
// tolerate. Incoming messages are demultiplexed by group.
type Transport struct {
	pb.UnimplementedRaftServer
	self  raft.NodeID
	peers map[raft.NodeID]string

	mu     sync.Mutex
	groups map[uint64]chan raft.Message
	queues map[raft.NodeID]chan *pb.RaftMessage
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

const (
	sendQueue    = 4096
	redialDelay  = 100 * time.Millisecond
	maxRedialGap = 2 * time.Second
)

func NewTransport(self raft.NodeID, peers map[raft.NodeID]string) *Transport {
	ctx, cancel := context.WithCancel(context.Background())
	t := &Transport{
		self:   self,
		peers:  peers,
		groups: make(map[uint64]chan raft.Message),
		queues: make(map[raft.NodeID]chan *pb.RaftMessage),
		ctx:    ctx,
		cancel: cancel,
	}
	for id, addr := range peers {
		if id == self {
			continue
		}
		q := make(chan *pb.RaftMessage, sendQueue)
		t.queues[id] = q
		t.wg.Add(1)
		go t.deliver(addr, q)
	}
	return t
}

// Group returns the raft.Transport for one group hosted on this node.
func (t *Transport) Group(id uint64) raft.Transport {
	t.mu.Lock()
	defer t.mu.Unlock()
	ch, ok := t.groups[id]
	if !ok {
		ch = make(chan raft.Message, 1024)
		t.groups[id] = ch
	}
	return &groupTransport{t: t, group: id, recv: ch}
}

type groupTransport struct {
	t     *Transport
	group uint64
	recv  chan raft.Message
}

func (g *groupTransport) Send(to raft.NodeID, m raft.Message) {
	q, ok := g.t.queues[to]
	if !ok {
		return
	}
	pm := toProto(m)
	pm.Group = g.group
	select {
	case q <- pm:
	default:
	}
}

func (g *groupTransport) Recv() <-chan raft.Message {
	return g.recv
}

// deliver owns the connection to one peer, reopening the stream after
// any failure with backoff. The backoff starts over once a connection has
// worked: a peer that restarts must hear from the leader again before its
// election timeout, not after the delay its downtime built up.
func (t *Transport) deliver(addr string, q <-chan *pb.RaftMessage) {
	defer t.wg.Done()
	delay := redialDelay
	for {
		connected, err := t.stream(addr, q)
		if err == nil {
			return
		}
		if connected {
			delay = redialDelay
		}
		select {
		case <-t.ctx.Done():
			return
		case <-time.After(delay):
		}
		if delay *= 2; delay > maxRedialGap {
			delay = maxRedialGap
		}
	}
}

// stream sends until the stream breaks; a nil error means shutdown. It
// reports whether the peer was reached: opening the stream waits for the
// connection, so a stream that opened had one.
func (t *Transport) stream(addr string, q <-chan *pb.RaftMessage) (connected bool, err error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false, err
	}
	defer conn.Close()
	st, err := pb.NewRaftClient(conn).Stream(t.ctx)
	if err != nil {
		return false, err
	}
	for {
		select {
		case <-t.ctx.Done():
			st.CloseSend()
			return true, nil
		case m := <-q:
			if err := st.Send(m); err != nil {
				return true, err
			}
		}
	}
}

func (t *Transport) Stream(st pb.Raft_StreamServer) error {
	for {
		m, err := st.Recv()
		if err != nil {
			return st.SendAndClose(&pb.StreamAck{})
		}
		t.mu.Lock()
		ch, ok := t.groups[m.Group]
		t.mu.Unlock()
		if !ok {
			continue
		}
		select {
		case ch <- fromProto(m):
		case <-t.ctx.Done():
			return nil
		}
	}
}

func (t *Transport) Register(s *grpc.Server) {
	pb.RegisterRaftServer(s, t)
}

func (t *Transport) Close() {
	t.cancel()
	t.wg.Wait()
}

func toProto(m raft.Message) *pb.RaftMessage {
	out := &pb.RaftMessage{
		Type:          uint32(m.Type),
		From:          uint64(m.From),
		To:            uint64(m.To),
		Term:          m.Term,
		Index:         m.Index,
		LogTerm:       m.LogTerm,
		Commit:        m.Commit,
		Reject:        m.Reject,
		ConflictTerm:  m.ConflictTerm,
		ConflictIndex: m.ConflictIndex,
		ReadId:        m.ReadID,
		Data:          m.Data,
		Offset:        m.Offset,
		Done:          m.Done,
	}
	if len(m.Entries) > 0 {
		out.Entries = make([]*pb.LogEntry, len(m.Entries))
		for i, e := range m.Entries {
			out.Entries[i] = &pb.LogEntry{Index: e.Index, Term: e.Term, Type: uint32(e.Type), Data: e.Data}
		}
	}
	return out
}

func fromProto(m *pb.RaftMessage) raft.Message {
	out := raft.Message{
		Type:          raft.MsgType(m.Type),
		From:          raft.NodeID(m.From),
		To:            raft.NodeID(m.To),
		Term:          m.Term,
		Index:         m.Index,
		LogTerm:       m.LogTerm,
		Commit:        m.Commit,
		Reject:        m.Reject,
		ConflictTerm:  m.ConflictTerm,
		ConflictIndex: m.ConflictIndex,
		ReadID:        m.ReadId,
		Data:          m.Data,
		Offset:        m.Offset,
		Done:          m.Done,
	}
	if len(m.Entries) > 0 {
		out.Entries = make([]raft.Entry, len(m.Entries))
		for i, e := range m.Entries {
			out.Entries[i] = raft.Entry{Index: e.Index, Term: e.Term, Type: raft.EntryType(e.Type), Data: e.Data}
		}
	}
	return out
}
