package kv

import (
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/arifisme/keystone/raft"
)

func serveTransport(t *testing.T, tr *Transport, addr string) *grpc.Server {
	t.Helper()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	tr.Register(s)
	go s.Serve(lis)
	return s
}

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

func TestTransportDeliversMessagesAndSurvivesPeerRestart(t *testing.T) {
	peers := map[raft.NodeID]string{1: freeAddr(t), 2: freeAddr(t)}
	t1 := NewTransport(1, peers)
	t2 := NewTransport(2, peers)
	defer t1.Close()
	defer t2.Close()
	s1 := serveTransport(t, t1, peers[1])
	defer s1.Stop()
	s2 := serveTransport(t, t2, peers[2])

	g1, g2 := t1.Group(7), t2.Group(7)
	send := func(i uint64) {
		g1.Send(2, raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: i, Entries: []raft.Entry{{Index: i, Term: i, Data: []byte("x")}}})
	}
	recv := func() raft.Message {
		select {
		case m := <-g2.Recv():
			return m
		case <-time.After(5 * time.Second):
			t.Fatal("no message")
			return raft.Message{}
		}
	}
	send(1)
	if m := recv(); m.Term != 1 || len(m.Entries) != 1 || m.Entries[0].Index != 1 || m.From != 1 {
		t.Fatalf("got %+v", m)
	}

	s2.Stop()
	time.Sleep(50 * time.Millisecond)
	t2b := NewTransport(2, peers)
	defer t2b.Close()
	g2b := t2b.Group(7)
	s2b := serveTransport(t, t2b, peers[2])
	defer s2b.Stop()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		send(2)
		select {
		case m := <-g2b.Recv():
			if m.Term == 2 {
				return
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatal("messages never resumed after peer restart")
}

func TestMessageRoundTripsThroughProto(t *testing.T) {
	in := raft.Message{Type: raft.MsgSnap, From: 3, To: 1, Term: 9, Index: 8, LogTerm: 7, Commit: 6, Reject: true, ConflictTerm: 5, ConflictIndex: 4, ReadID: 3, Data: []byte("d"), Offset: 2, Done: true, Entries: []raft.Entry{{Index: 1, Term: 1, Type: raft.EntryNoop, Data: []byte("e")}}}
	out := fromProto(toProto(in))
	if out.Type != in.Type || out.From != in.From || out.Term != in.Term || out.ReadID != in.ReadID || !out.Done || out.Entries[0].Type != raft.EntryNoop || string(out.Entries[0].Data) != "e" || out.ConflictIndex != 4 {
		t.Fatalf("round trip lost fields: %+v", out)
	}
}

func TestTransportKeepsGroupsApart(t *testing.T) {
	peers := map[raft.NodeID]string{1: freeAddr(t), 2: freeAddr(t)}
	t1 := NewTransport(1, peers)
	t2 := NewTransport(2, peers)
	defer t1.Close()
	defer t2.Close()
	s2 := serveTransport(t, t2, peers[2])
	defer s2.Stop()
	a, b := t2.Group(1), t2.Group(2)
	t1.Group(2).Send(2, raft.Message{Type: raft.MsgVote, From: 1, To: 2, Term: 3})
	select {
	case m := <-b.Recv():
		if m.Term != 3 {
			t.Fatalf("got %+v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("group 2 never received")
	}
	select {
	case m := <-a.Recv():
		t.Fatalf("group 1 received group 2's message %+v", m)
	case <-time.After(50 * time.Millisecond):
	}
}
