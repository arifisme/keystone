package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func (c *cluster) propose(l *Node, data string) *Proposal {
	p := l.Propose([]byte(data))
	return p
}

func (c *cluster) waitApplied(id NodeID, n int, within time.Duration) []Entry {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if got := c.sms[id].entries(); len(got) >= n {
			return got
		}
		time.Sleep(time.Millisecond)
	}
	c.t.Fatalf("node %d applied %d of %d entries (seed %d): %s", id, len(c.sms[id].entries()), n, c.seed, statusLine(c))
	return nil
}

func TestReplicationAppliesIdenticalSequenceOnEveryNode(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()
	l := c.waitForLeader(5 * electionTimeout())
	var props []*Proposal
	for i := 0; i < 100; i++ {
		props = append(props, c.propose(l, fmt.Sprintf("cmd%d", i)))
	}
	for i, p := range props {
		select {
		case <-p.Done():
		case <-time.After(5 * time.Second):
			t.Fatalf("proposal %d never settled", i)
		}
		if p.Err != nil {
			t.Fatalf("proposal %d: %v", i, p.Err)
		}
		if want := fmt.Sprintf("ok:cmd%d", i); string(p.Result) != want {
			t.Fatalf("proposal %d result %q", i, p.Result)
		}
	}
	first := c.waitApplied(1, 100, 5*time.Second)
	for _, id := range c.ids[1:] {
		got := c.waitApplied(id, 100, 5*time.Second)
		for i := range first {
			if got[i].Index != first[i].Index || got[i].Term != first[i].Term || !bytes.Equal(got[i].Data, first[i].Data) {
				t.Fatalf("node %d entry %d differs: %+v vs %+v", id, i, got[i], first[i])
			}
		}
	}
}

func TestReplicationCatchesUpAFollowerThatMissedEntries(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()
	l := c.waitForLeader(5 * electionTimeout())
	var lagging NodeID
	for _, id := range c.ids {
		if id != l.Status().ID {
			lagging = id
			break
		}
	}
	c.kill(lagging)
	for i := 0; i < 50; i++ {
		p := c.propose(l, fmt.Sprintf("while-down-%d", i))
		<-p.Done()
		if p.Err != nil {
			t.Fatal(p.Err)
		}
	}
	c.start(lagging)
	got := c.waitApplied(lagging, 50, 5*time.Second)
	for i := 0; i < 50; i++ {
		if want := fmt.Sprintf("while-down-%d", i); string(got[i].Data) != want {
			t.Fatalf("lagging node entry %d = %q, want %q", i, got[i].Data, want)
		}
	}
}

func TestProposalOnFollowerFailsWithNotLeader(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()
	l := c.waitForLeader(5 * electionTimeout())
	for _, id := range c.ids {
		if id == l.Status().ID {
			continue
		}
		p := c.nodes[id].Propose([]byte("x"))
		c.nodes[id].Flush()
		<-p.Done()
		if p.Err != ErrNotLeader {
			t.Fatalf("follower proposal err = %v", p.Err)
		}
	}
}

// capture is a Transport that records what a single Raft sends.
type capture struct {
	sent []Message
}

func (c *capture) Send(to NodeID, m Message) { c.sent = append(c.sent, m) }
func (c *capture) Recv() <-chan Message      { return nil }

func (c *capture) take() []Message {
	out := c.sent
	c.sent = nil
	return out
}

func newRaft(t *testing.T, id NodeID, peers []NodeID, store LogStore, tr Transport) *Raft {
	t.Helper()
	r, err := New(Config{ID: id, Peers: peers, ElectionTick: 10, HeartbeatTick: 1, Store: store, Transport: tr, Rand: rand.New(rand.NewSource(int64(id)))})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// A leader in term 3 holds an entry from term 2 that is already on a
// majority. It must not commit that entry by counting replicas; it commits
// only once its own term-3 entry is replicated (Raft §5.4.2, figure 8).
func TestLeaderCommitsOnlyOwnTermEntriesByCounting(t *testing.T) {
	peers := []NodeID{1, 2, 3}
	store := NewMemStore()
	store.Append([]Entry{{Index: 1, Term: 1, Type: EntryNoop}, {Index: 2, Term: 2, Type: EntryNormal, Data: []byte("old")}})
	store.SetState(2, 1)
	tr := &capture{}
	r := newRaft(t, 1, peers, store, tr)

	r.becomeCandidate()
	r.Step(Message{Type: MsgVoteResp, From: 2, Term: 3})
	if r.state != Leader || r.term != 3 {
		t.Fatalf("state %v term %d", r.state, r.term)
	}
	tr.take()

	// Node 2 reports it holds everything through index 2 (the old-term
	// entry), but not the term-3 no-op at index 3.
	r.Step(Message{Type: MsgAppResp, From: 2, Term: 3, Index: 2})
	if r.commit != 0 {
		t.Fatalf("committed %d by counting an old-term entry", r.commit)
	}
	r.Step(Message{Type: MsgAppResp, From: 2, Term: 3, Index: 3})
	if r.commit != 3 {
		t.Fatalf("commit = %d after own-term entry replicated, want 3", r.commit)
	}
}

func TestFollowerRejectsAppendWithConflictTermAndFirstIndex(t *testing.T) {
	store := NewMemStore()
	store.Append([]Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 2}, {Index: 4, Term: 2}, {Index: 5, Term: 2},
	})
	tr := &capture{}
	r := newRaft(t, 2, []NodeID{1, 2, 3}, store, tr)

	r.Step(Message{Type: MsgApp, From: 1, Term: 3, Index: 5, LogTerm: 3})
	resp := tr.take()[0]
	if !resp.Reject || resp.ConflictTerm != 2 || resp.ConflictIndex != 3 {
		t.Fatalf("resp = %+v, want reject with term 2 first index 3", resp)
	}

	r.Step(Message{Type: MsgApp, From: 1, Term: 3, Index: 9, LogTerm: 3})
	resp = tr.take()[0]
	if !resp.Reject || resp.ConflictTerm != 0 || resp.ConflictIndex != 6 {
		t.Fatalf("resp = %+v, want reject at last+1", resp)
	}

	r.Step(Message{Type: MsgApp, From: 1, Term: 3, Index: 2, LogTerm: 1, Entries: []Entry{{Index: 3, Term: 3, Data: []byte("new")}}, Commit: 3})
	resp = tr.take()[0]
	if resp.Reject || resp.Index != 3 {
		t.Fatalf("resp = %+v, want success through 3", resp)
	}
	if store.LastIndex() != 3 {
		t.Fatalf("conflicting suffix not truncated, last = %d", store.LastIndex())
	}
	if term, _ := store.Term(3); term != 3 {
		t.Fatalf("term at 3 = %d", term)
	}
	if r.commit != 3 {
		t.Fatalf("commit = %d", r.commit)
	}
}

func TestLeaderBacktracksUsingConflictTerm(t *testing.T) {
	store := NewMemStore()
	store.Append([]Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}, {Index: 4, Term: 4}, {Index: 5, Term: 4},
	})
	store.SetState(4, 1)
	tr := &capture{}
	r := newRaft(t, 1, []NodeID{1, 2, 3}, store, tr)
	r.becomeCandidate()
	r.Step(Message{Type: MsgVoteResp, From: 2, Term: 5})
	tr.take()

	// Follower 2 has terms 1,1,1,2,2,2: it conflicts at index 4 with
	// term 2. The leader has no term-2 entries, so it jumps to the
	// follower's first index of that term.
	r.Step(Message{Type: MsgAppResp, From: 2, Term: 5, Reject: true, ConflictTerm: 2, ConflictIndex: 4})
	if r.next[2] != 4 {
		t.Fatalf("next = %d, want 4", r.next[2])
	}
	sent := tr.take()
	if len(sent) != 1 || sent[0].Index != 3 || sent[0].LogTerm != 1 {
		t.Fatalf("resent %+v, want probe at prev index 3", sent)
	}

	// Follower 3 has terms 1,1,1,1,1: conflict term 1 whose first index
	// is 1; the leader holds term 1 through index 3, so it resumes at 4.
	r.Step(Message{Type: MsgAppResp, From: 3, Term: 5, Reject: true, ConflictTerm: 1, ConflictIndex: 1})
	if r.next[3] != 4 {
		t.Fatalf("next = %d, want 4", r.next[3])
	}
}
