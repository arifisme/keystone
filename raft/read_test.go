package raft

import (
	"testing"
	"time"
)

func TestReadIndexCompletesAfterMajorityHeartbeat(t *testing.T) {
	store := NewMemStore()
	tr := &capture{}
	r := newRaft(t, 1, []NodeID{1, 2, 3}, store, tr)
	r.becomeCandidate()
	r.Step(Message{Type: MsgVoteResp, From: 2, Term: 1})
	tr.take()
	if _, err := r.ReadIndex(); err != ErrLeaderNotReady {
		t.Fatalf("read before own-term commit: %v", err)
	}
	r.Step(Message{Type: MsgAppResp, From: 2, Term: 1, Index: 1})
	if r.commit != 1 {
		t.Fatalf("commit = %d", r.commit)
	}
	tr.take()

	id, err := r.ReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	sent := tr.take()
	if len(sent) != 2 || sent[0].ReadID != id || sent[1].ReadID != id {
		t.Fatalf("heartbeats %+v", sent)
	}
	if ready := r.TakeReady(); len(ready) != 0 {
		t.Fatal("read completed before any response")
	}
	r.Step(Message{Type: MsgAppResp, From: 3, Term: 1, Index: 1, ReadID: id})
	ready := r.TakeReady()
	if len(ready) != 1 || ready[0].ID != id || ready[0].Index != 1 {
		t.Fatalf("ready = %+v", ready)
	}
}

func TestReadIndexRoundIsAbandonedOnStepDown(t *testing.T) {
	store := NewMemStore()
	tr := &capture{}
	r := newRaft(t, 1, []NodeID{1, 2, 3}, store, tr)
	r.becomeCandidate()
	r.Step(Message{Type: MsgVoteResp, From: 2, Term: 1})
	r.Step(Message{Type: MsgAppResp, From: 2, Term: 1, Index: 1})
	id, _ := r.ReadIndex()
	r.Step(Message{Type: MsgVote, From: 3, Term: 5, Index: 1, LogTerm: 1})
	r.Step(Message{Type: MsgAppResp, From: 3, Term: 1, Index: 1, ReadID: id})
	if len(r.TakeReady()) != 0 || r.state != Follower {
		t.Fatal("stale read round completed after losing leadership")
	}
}

func TestNodeReadIndexObservesPrecedingWrites(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()
	l := c.waitForLeader(5 * electionTimeout())
	for i := 0; i < 20; i++ {
		p := l.Propose([]byte("w"))
		<-p.Done()
		if p.Err != nil {
			t.Fatal(p.Err)
		}
	}
	rd := l.ReadIndex()
	select {
	case <-rd.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("read never completed")
	}
	if rd.Err != nil {
		t.Fatal(rd.Err)
	}
	if rd.Index < 20 || l.Status().Applied < rd.Index {
		t.Fatalf("read index %d applied %d", rd.Index, l.Status().Applied)
	}
	for _, id := range c.ids {
		if id == l.Status().ID {
			continue
		}
		frd := c.nodes[id].ReadIndex()
		c.nodes[id].Flush()
		<-frd.Done()
		if frd.Err != ErrNotLeader {
			t.Fatalf("follower read err = %v", frd.Err)
		}
	}
}

// Node 3 is silent, so the round needs node 2, which is behind the
// leader's compaction point and in the middle of a snapshot transfer.
func TestReadIndexIsConfirmedByAFollowerThatIsReceivingASnapshot(t *testing.T) {
	store := NewMemStore()
	tr := &capture{}
	r := newRaft(t, 1, []NodeID{1, 2, 3}, store, tr)
	r.becomeCandidate()
	r.Step(Message{Type: MsgVoteResp, From: 3, Term: 1})
	if _, err := r.Propose([][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatal(err)
	}
	r.Step(Message{Type: MsgAppResp, From: 3, Term: 1, Index: 3})
	if r.commit != 3 {
		t.Fatalf("commit = %d", r.commit)
	}
	if err := store.Compact(3, 1, []byte("snapshot")); err != nil {
		t.Fatal(err)
	}
	r.Step(Message{Type: MsgAppResp, From: 2, Term: 1, Reject: true, ConflictIndex: 1})
	if sent := tr.take(); sent[len(sent)-1].Type != MsgSnap {
		t.Fatalf("test needs a snapshot transfer to node 2 under way, sent %+v", sent)
	}

	id, err := r.ReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	var probe Message
	for _, m := range tr.take() {
		if m.To == 2 {
			probe = m
		}
	}
	if probe.ReadID != id || len(probe.Data) != 0 {
		t.Fatalf("to node 2: %+v, want a read probe without snapshot data", probe)
	}

	followerTr := &capture{}
	follower := newRaft(t, 2, []NodeID{1, 2, 3}, NewMemStore(), followerTr)
	follower.Step(probe)
	for _, m := range followerTr.take() {
		r.Step(m)
	}
	if ready := r.TakeReady(); len(ready) != 1 || ready[0].ID != id {
		t.Fatalf("ready = %+v, want round %d confirmed by node 2", ready, id)
	}
	for _, m := range tr.take() {
		if m.Type == MsgSnap {
			t.Fatal("the answer to a read probe set off another snapshot chunk")
		}
	}
}
