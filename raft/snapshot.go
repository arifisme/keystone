package raft

import "fmt"

// A snapshot moves in chunks with one in flight at a time. Each chunk is
// acknowledged with the offset the receiver expects next, and a chunk the
// receiver cannot place restarts the transfer from the beginning. The
// leader's heartbeat resends the outstanding chunk, so a lost message only
// costs a tick.
type snapTransfer struct {
	index  uint64
	term   uint64
	data   []byte
	offset uint64
}

func (r *Raft) sendSnapshot(to NodeID) {
	idx, term, data, err := r.store.Snapshot()
	if err != nil {
		panic(fmt.Sprintf("raft: load snapshot: %v", err))
	}
	x := r.snapOut[to]
	if x == nil || x.index != idx {
		x = &snapTransfer{index: idx, term: term, data: data}
		r.snapOut[to] = x
	}
	r.sendChunk(to, x)
}

func (r *Raft) sendChunk(to NodeID, x *snapTransfer) {
	end := x.offset + uint64(r.snapChunk)
	if end > uint64(len(x.data)) {
		end = uint64(len(x.data))
	}
	r.send(Message{
		Type:    MsgSnap,
		To:      to,
		Index:   x.index,
		LogTerm: x.term,
		Data:    x.data[x.offset:end],
		Offset:  x.offset,
		Done:    end == uint64(len(x.data)),
	})
}

func (r *Raft) handleSnapshot(m Message) {
	if r.state != Follower {
		r.becomeFollower(m.Term, m.From)
	}
	r.lead = m.From
	r.elapsed = 0

	if m.Index <= r.commit {
		r.send(Message{Type: MsgSnapResp, To: m.From, Index: r.commit, Done: true})
		return
	}
	in := r.snapIn
	if m.Offset == 0 {
		in = &snapTransfer{index: m.Index, term: m.LogTerm}
		r.snapIn = in
	}
	if in == nil || in.index != m.Index || m.Offset != uint64(len(in.data)) {
		// Either a duplicate we already have or a chunk we cannot place.
		expect := uint64(0)
		if in != nil && in.index == m.Index {
			expect = uint64(len(in.data))
		}
		r.send(Message{Type: MsgSnapResp, To: m.From, Index: m.Index, Offset: expect, Reject: in == nil || in.index != m.Index || m.Offset > expect})
		return
	}
	in.data = append(in.data, m.Data...)
	if !m.Done {
		r.send(Message{Type: MsgSnapResp, To: m.From, Index: m.Index, Offset: uint64(len(in.data))})
		return
	}
	r.snapIn = nil
	if err := r.store.Compact(in.index, in.term, in.data); err != nil {
		panic(fmt.Sprintf("raft: install snapshot: %v", err))
	}
	r.commit = in.index
	r.restored = in
	r.send(Message{Type: MsgSnapResp, To: m.From, Index: in.index, Done: true})
}

// TakeRestored returns the snapshot installed by the last Step, if any,
// so the driver can load it into the state machine.
func (r *Raft) TakeRestored() (index uint64, data []byte, ok bool) {
	if r.restored == nil {
		return 0, nil, false
	}
	x := r.restored
	r.restored = nil
	return x.index, x.data, true
}

func (r *Raft) handleSnapshotResp(m Message) {
	x := r.snapOut[m.From]
	if m.Done {
		delete(r.snapOut, m.From)
		if m.Index > r.match[m.From] {
			r.match[m.From] = m.Index
		}
		if m.Index+1 > r.next[m.From] {
			r.next[m.From] = m.Index + 1
		}
		if r.maybeCommit() {
			r.broadcastAppend()
		} else {
			r.sendAppend(m.From, 0)
		}
		return
	}
	if x == nil || x.index != m.Index {
		return
	}
	if m.Reject {
		x.offset = 0
	} else if m.Offset > x.offset {
		x.offset = m.Offset
	}
	r.sendChunk(m.From, x)
}
