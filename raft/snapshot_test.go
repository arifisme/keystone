package raft

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestFollowerRejoinsThroughSnapshotAfterLogCompaction(t *testing.T) {
	c := newCluster(t, 3)
	c.snap = 20
	defer c.stop()
	for _, id := range c.ids {
		c.kill(id)
		c.start(id)
	}
	l := c.waitForLeader(5 * electionTimeout())
	var lagging NodeID
	for _, id := range c.ids {
		if id != l.Status().ID {
			lagging = id
			break
		}
	}
	c.kill(lagging)
	for i := 0; i < 100; i++ {
		p := l.Propose([]byte(fmt.Sprintf("entry-%03d-with-some-padding", i)))
		<-p.Done()
		if p.Err != nil {
			t.Fatal(p.Err)
		}
	}
	if s := l.Status(); s.FirstIndex <= 1 {
		t.Fatalf("leader never compacted: %+v", s)
	}
	c.start(lagging)
	got := c.waitApplied(lagging, 100, 10*time.Second)
	want := c.sms[l.Status().ID].entries()
	if len(got) != len(want) {
		t.Fatalf("lagging node applied %d entries, leader %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Index != want[i].Index || !bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("entry %d differs: %+v vs %+v", i, got[i], want[i])
		}
	}
	if s := c.nodes[lagging].Status(); s.FirstIndex <= 1 {
		t.Fatalf("lagging node should have installed a snapshot: %+v", s)
	}
	p := l.Propose([]byte("after"))
	<-p.Done()
	c.waitApplied(lagging, 101, 5*time.Second)
}

func TestSnapshotChunksAreReassembledInOrder(t *testing.T) {
	store := NewMemStore()
	tr := &capture{}
	r := newRaft(t, 2, []NodeID{1, 2, 3}, store, tr)
	data := bytes.Repeat([]byte("0123456789"), 10)

	r.Step(Message{Type: MsgSnap, From: 1, Term: 2, Index: 50, LogTerm: 2, Data: data[:40], Offset: 0})
	resp := tr.take()[0]
	if resp.Reject || resp.Offset != 40 {
		t.Fatalf("first chunk resp %+v", resp)
	}
	r.Step(Message{Type: MsgSnap, From: 1, Term: 2, Index: 50, LogTerm: 2, Data: data[80:], Offset: 80, Done: true})
	resp = tr.take()[0]
	if !resp.Reject {
		t.Fatalf("out-of-order chunk accepted: %+v", resp)
	}
	r.Step(Message{Type: MsgSnap, From: 1, Term: 2, Index: 50, LogTerm: 2, Data: data[:40], Offset: 0})
	tr.take()
	r.Step(Message{Type: MsgSnap, From: 1, Term: 2, Index: 50, LogTerm: 2, Data: data[40:80], Offset: 40})
	tr.take()
	r.Step(Message{Type: MsgSnap, From: 1, Term: 2, Index: 50, LogTerm: 2, Data: data[80:], Offset: 80, Done: true})
	resp = tr.take()[0]
	if resp.Reject || !resp.Done || resp.Index != 50 {
		t.Fatalf("final resp %+v", resp)
	}
	idx, term, got, _ := store.Snapshot()
	if idx != 50 || term != 2 || !bytes.Equal(got, data) {
		t.Fatalf("installed %d/%d %d bytes", idx, term, len(got))
	}
	if ri, rd, ok := r.TakeRestored(); !ok || ri != 50 || !bytes.Equal(rd, data) {
		t.Fatal("restored snapshot not handed to driver")
	}
	if r.commit != 50 {
		t.Fatalf("commit = %d", r.commit)
	}
}
