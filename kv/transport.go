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

// Transport carries Raft messages over one gRPC stream per peer. Send
// never blocks: each peer has a bounded queue and a message that does not
// fit is dropped, which Raft's retries tolerate. Incoming streams feed one
// channel that the node drains.
type Transport struct {
	pb.UnimplementedRaftServer
	self  raft.NodeID
	peers map[raft.NodeID]string
	recv  chan raft.Message

	mu     sync.Mutex
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
		recv:   make(chan raft.Message, 1024),
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

func (t *Transport) Send(to raft.NodeID, m raft.Message) {
	q, ok := t.queues[to]
	if !ok {
		return
	}
	select {
	case q <- toProto(m):
	default:
	}
}

func (t *Transport) Recv() <-chan raft.Message {
	return t.recv
}

// deliver owns the connection to one peer, reopening the stream after
// any failure with backoff.
func (t *Transport) deliver(addr string, q <-chan *pb.RaftMessage) {
	defer t.wg.Done()
	delay := redialDelay
	for {
		if err := t.stream(addr, q); err == nil {
			return
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

// stream sends until the stream breaks; a nil return means shutdown.
func (t *Transport) stream(addr string, q <-chan *pb.RaftMessage) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	st, err := pb.NewRaftClient(conn).Stream(t.ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-t.ctx.Done():
			st.CloseSend()
			return nil
		case m := <-q:
			if err := st.Send(m); err != nil {
				return err
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
		select {
		case t.recv <- fromProto(m):
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
