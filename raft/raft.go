// Package raft implements the Raft consensus algorithm over opaque byte
// entries. It has no knowledge of the key-value layer above it.
//
// Raft is a synchronous state machine: Step consumes one message, Tick
// advances one logical tick, and every effect leaves through the LogStore
// and Transport handed in at construction. Node wraps it for production;
// the simulator drives it directly.
package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
)

type State uint8

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

var (
	ErrNotLeader = errors.New("raft: not leader")
	// ErrLeaderNotReady means the leader has not yet committed an entry of
	// its own term; retry after the next commit.
	ErrLeaderNotReady = errors.New("raft: leader has not committed in its term")
)

type Config struct {
	ID    NodeID
	Peers []NodeID
	// ElectionTick is the lower bound of the election timeout in ticks; the
	// timeout is drawn from [ElectionTick, 2*ElectionTick).
	ElectionTick  int
	HeartbeatTick int
	// MaxBatch caps entries per AppendEntries message.
	MaxBatch int
	// SnapshotChunk is the payload size of one InstallSnapshot message.
	SnapshotChunk int
	Store         LogStore
	Transport     Transport
	Rand          *rand.Rand
}

type Raft struct {
	id    NodeID
	peers []NodeID
	store LogStore
	tr    Transport
	rng   *rand.Rand

	electionTick  int
	heartbeatTick int
	maxBatch      int
	snapChunk     int

	term   uint64
	vote   NodeID
	state  State
	lead   NodeID
	commit uint64

	votes map[NodeID]bool
	next  map[NodeID]uint64
	match map[NodeID]uint64

	// elapsed counts ticks since the last heartbeat received (follower,
	// candidate) or sent (leader). timeout is this term's random draw.
	elapsed int
	timeout int

	// reads are ReadIndex rounds awaiting a majority of heartbeat
	// responses; ready holds the rounds that reached it.
	reads      map[uint64]*readRound
	nextReadID uint64
	ready      []ReadReady

	// snapOut tracks one snapshot transfer per follower; snapIn is the
	// one being received. restored is set for the driver once a received
	// snapshot has been installed in the store.
	snapOut  map[NodeID]*snapTransfer
	snapIn   *snapTransfer
	restored *snapTransfer
}

type readRound struct {
	index uint64
	acks  map[NodeID]bool
}

// ReadReady reports that read round ID is safe to serve once the state
// machine has applied Index.
type ReadReady struct {
	ID    uint64
	Index uint64
}

func New(cfg Config) (*Raft, error) {
	if cfg.ID == None {
		return nil, errors.New("raft: node id required")
	}
	if len(cfg.Peers) == 0 || cfg.ElectionTick <= 0 || cfg.HeartbeatTick <= 0 {
		return nil, errors.New("raft: peers, election tick and heartbeat tick required")
	}
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 256
	}
	if cfg.SnapshotChunk <= 0 {
		cfg.SnapshotChunk = 512 << 10
	}
	term, vote, err := cfg.Store.State()
	if err != nil {
		return nil, fmt.Errorf("raft: load state: %w", err)
	}
	r := &Raft{
		id:            cfg.ID,
		peers:         cfg.Peers,
		store:         cfg.Store,
		tr:            cfg.Transport,
		rng:           cfg.Rand,
		electionTick:  cfg.ElectionTick,
		heartbeatTick: cfg.HeartbeatTick,
		maxBatch:      cfg.MaxBatch,
		snapChunk:     cfg.SnapshotChunk,
		term:          term,
		vote:          vote,
	}
	r.commit, _, _, _ = r.store.Snapshot()
	r.resetTimeout()
	return r, nil
}

func (r *Raft) ID() NodeID          { return r.id }
func (r *Raft) Term() uint64        { return r.term }
func (r *Raft) State() State        { return r.state }
func (r *Raft) Leader() NodeID      { return r.lead }
func (r *Raft) CommitIndex() uint64 { return r.commit }

func (r *Raft) quorum() int {
	return len(r.peers)/2 + 1
}

func (r *Raft) resetTimeout() {
	r.elapsed = 0
	r.timeout = r.electionTick + r.rng.Intn(r.electionTick)
}

func (r *Raft) send(m Message) {
	m.From = r.id
	m.Term = r.term
	r.tr.Send(m.To, m)
}

func (r *Raft) lastIndexAndTerm() (uint64, uint64) {
	last := r.store.LastIndex()
	t, err := r.store.Term(last)
	if err != nil {
		panic(fmt.Sprintf("raft: term of last index %d: %v", last, err))
	}
	return last, t
}

func (r *Raft) termAt(index uint64) uint64 {
	t, err := r.store.Term(index)
	if err != nil {
		panic(fmt.Sprintf("raft: term of %d: %v", index, err))
	}
	return t
}

func (r *Raft) persistState() {
	if err := r.store.SetState(r.term, r.vote); err != nil {
		panic(fmt.Sprintf("raft: persist state: %v", err))
	}
}

func (r *Raft) Tick() {
	r.elapsed++
	if r.state == Leader {
		if r.elapsed >= r.heartbeatTick {
			r.elapsed = 0
			r.broadcastAppend()
		}
		return
	}
	if r.elapsed >= r.timeout {
		r.becomeCandidate()
	}
}

// becomeFollower adopts term. The election timer restarts only when a
// leader is known or the node is stepping down from candidate or leader.
// A follower that merely learns of a higher term from a vote request it
// rejects keeps its countdown: restarting it would let a node with a
// stale log, timing out again and again, keep the one node whose log can
// win from ever standing.
func (r *Raft) becomeFollower(term uint64, lead NodeID) {
	if term > r.term {
		r.term = term
		r.vote = None
		r.persistState()
	}
	restart := lead != None || r.state != Follower
	r.state = Follower
	r.lead = lead
	r.votes, r.next, r.match = nil, nil, nil
	r.reads, r.snapOut = nil, nil
	if restart {
		r.resetTimeout()
	}
}

func (r *Raft) becomeCandidate() {
	r.term++
	r.vote = r.id
	r.persistState()
	r.state = Candidate
	r.lead = None
	r.votes = map[NodeID]bool{r.id: true}
	r.resetTimeout()
	if r.countVotes() {
		r.becomeLeader()
		return
	}
	last, lastTerm := r.lastIndexAndTerm()
	for _, p := range r.peers {
		if p != r.id {
			r.send(Message{Type: MsgVote, To: p, Index: last, LogTerm: lastTerm})
		}
	}
}

func (r *Raft) countVotes() bool {
	granted := 0
	for _, g := range r.votes {
		if g {
			granted++
		}
	}
	return granted >= r.quorum()
}

func (r *Raft) becomeLeader() {
	r.state = Leader
	r.lead = r.id
	r.elapsed = 0
	last := r.store.LastIndex()
	r.next = make(map[NodeID]uint64, len(r.peers))
	r.match = make(map[NodeID]uint64, len(r.peers))
	for _, p := range r.peers {
		r.next[p] = last + 1
	}
	r.reads = map[uint64]*readRound{}
	r.snapOut = map[NodeID]*snapTransfer{}
	// The no-op lets entries from earlier terms commit under the own-term
	// rule without waiting for a client proposal.
	r.appendEntries([]Entry{{Type: EntryNoop}})
}

// appendEntries assigns indexes and the current term, replicates, and
// persists. Leader only.
//
// Followers that are caught up get the batch before the local write, so
// their disks work while the leader's does instead of after it. That is
// safe because the leader's own replica is counted toward a majority only
// once the write has returned: if it crashes in between, the entries are
// merely uncommitted entries on some followers, which the next leader
// keeps or truncates like any others.
func (r *Raft) appendEntries(entries []Entry) uint64 {
	last := r.store.LastIndex()
	for i := range entries {
		entries[i].Index = last + 1 + uint64(i)
		entries[i].Term = r.term
	}
	prevTerm := r.termAt(last)
	early := make(map[NodeID]bool, len(r.peers))
	for _, p := range r.peers {
		if p == r.id || r.next[p] != last+1 || len(entries) > r.maxBatch {
			continue
		}
		early[p] = true
		r.next[p] = last + 1 + uint64(len(entries))
		r.send(Message{Type: MsgApp, To: p, Index: last, LogTerm: prevTerm, Entries: entries, Commit: r.commit})
	}
	if err := r.store.Append(entries); err != nil {
		panic(fmt.Sprintf("raft: append: %v", err))
	}
	r.match[r.id] = last + uint64(len(entries))
	for _, p := range r.peers {
		if p != r.id && !early[p] {
			r.sendAppend(p, 0)
		}
	}
	r.maybeCommit()
	return last + 1
}

// Propose appends one entry per payload and returns the index of the
// first. Whether they commit is learned through the commit index.
func (r *Raft) Propose(payloads [][]byte) (uint64, error) {
	if r.state != Leader {
		return 0, ErrNotLeader
	}
	entries := make([]Entry, len(payloads))
	for i, p := range payloads {
		entries[i] = Entry{Type: EntryNormal, Data: p}
	}
	return r.appendEntries(entries), nil
}

// ReadIndex starts a heartbeat round that confirms this node is still
// leader. Once a majority answers, the round appears in TakeReady with the
// commit index recorded here, which any read may then be served at. The
// leader must have committed an entry of its own term first, or its commit
// index might still trail what an earlier leader committed.
func (r *Raft) ReadIndex() (uint64, error) {
	if r.state != Leader {
		return 0, ErrNotLeader
	}
	if r.termAt(r.commit) != r.term {
		return 0, ErrLeaderNotReady
	}
	r.nextReadID++
	id := r.nextReadID
	r.reads[id] = &readRound{index: r.commit, acks: map[NodeID]bool{r.id: true}}
	if r.quorum() == 1 {
		r.completeRead(id)
		return id, nil
	}
	for _, p := range r.peers {
		if p != r.id {
			r.sendAppend(p, id)
		}
	}
	return id, nil
}

func (r *Raft) ackRead(id uint64, from NodeID) {
	round, ok := r.reads[id]
	if !ok {
		return
	}
	round.acks[from] = true
	if len(round.acks) >= r.quorum() {
		r.completeRead(id)
	}
}

func (r *Raft) completeRead(id uint64) {
	r.ready = append(r.ready, ReadReady{ID: id, Index: r.reads[id].index})
	delete(r.reads, id)
}

// TakeReady returns and clears the read rounds that reached a majority.
func (r *Raft) TakeReady() []ReadReady {
	out := r.ready
	r.ready = nil
	return out
}

func (r *Raft) Step(m Message) {
	if m.Term < r.term {
		r.rejectStale(m)
		return
	}
	if m.Term > r.term {
		lead := None
		if m.Type == MsgApp || m.Type == MsgSnap {
			lead = m.From
		}
		r.becomeFollower(m.Term, lead)
	}
	switch m.Type {
	case MsgVote:
		r.handleVote(m)
	case MsgVoteResp:
		r.handleVoteResp(m)
	case MsgApp:
		r.handleAppend(m)
	case MsgAppResp:
		if r.state == Leader {
			r.handleAppendResp(m)
		}
	case MsgSnap:
		r.handleSnapshot(m)
	case MsgSnapResp:
		if r.state == Leader {
			r.handleSnapshotResp(m)
		}
	}
}

// rejectStale answers a message from an earlier term so the sender learns
// the current one.
func (r *Raft) rejectStale(m Message) {
	switch m.Type {
	case MsgVote:
		r.send(Message{Type: MsgVoteResp, To: m.From, Reject: true})
	case MsgApp:
		r.send(Message{Type: MsgAppResp, To: m.From, Reject: true, ReadID: m.ReadID})
	case MsgSnap:
		r.send(Message{Type: MsgSnapResp, To: m.From, Reject: true})
	}
}

func (r *Raft) handleVote(m Message) {
	// A follower that hears from a live leader refuses to help unseat it.
	canVote := r.vote == m.From || (r.vote == None && r.lead == None)
	last, lastTerm := r.lastIndexAndTerm()
	upToDate := m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= last)
	if !canVote || !upToDate {
		r.send(Message{Type: MsgVoteResp, To: m.From, Reject: true})
		return
	}
	r.vote = m.From
	r.persistState()
	r.elapsed = 0
	r.send(Message{Type: MsgVoteResp, To: m.From})
}

func (r *Raft) handleVoteResp(m Message) {
	if r.state != Candidate {
		return
	}
	r.votes[m.From] = !m.Reject
	if r.countVotes() {
		r.becomeLeader()
		return
	}
	rejected := 0
	for _, g := range r.votes {
		if !g {
			rejected++
		}
	}
	if rejected >= r.quorum() {
		r.becomeFollower(r.term, None)
	}
}

func (r *Raft) broadcastAppend() {
	for _, p := range r.peers {
		if p != r.id {
			r.sendAppend(p, 0)
		}
	}
}

// sendAppend ships the follower's next batch and advances its next index
// at once, without waiting for the acknowledgement, so consecutive batches
// pipeline instead of the same entries going out again on every
// response. If a batch is lost the next heartbeat probes at the advanced
// index, the follower rejects it, and backtracking resends from where it
// really is.
func (r *Raft) sendAppend(to NodeID, readID uint64) {
	next := r.next[to]
	first := r.store.FirstIndex()
	if next < first {
		r.sendSnapshot(to)
		return
	}
	prevTerm := r.termAt(next - 1)
	last := r.store.LastIndex()
	var entries []Entry
	if next <= last {
		hi := next + uint64(r.maxBatch)
		if hi > last+1 {
			hi = last + 1
		}
		var err error
		if entries, err = r.store.Entries(next, hi); err != nil {
			panic(fmt.Sprintf("raft: entries [%d,%d): %v", next, hi, err))
		}
		r.next[to] = hi
	}
	r.send(Message{
		Type:    MsgApp,
		To:      to,
		Index:   next - 1,
		LogTerm: prevTerm,
		Entries: entries,
		Commit:  r.commit,
		ReadID:  readID,
	})
}

func (r *Raft) handleAppend(m Message) {
	if r.state != Follower {
		r.becomeFollower(m.Term, m.From)
	}
	r.lead = m.From
	r.elapsed = 0

	// Everything up to the commit index is identical on every node, so a
	// probe below it is answered without inspection.
	if m.Index < r.commit {
		r.send(Message{Type: MsgAppResp, To: m.From, Index: r.commit, ReadID: m.ReadID})
		return
	}
	last := r.store.LastIndex()
	if m.Index > last {
		r.send(Message{Type: MsgAppResp, To: m.From, Reject: true, ConflictIndex: last + 1, ReadID: m.ReadID})
		return
	}
	if t := r.termAt(m.Index); t != m.LogTerm {
		r.send(Message{
			Type:          MsgAppResp,
			To:            m.From,
			Reject:        true,
			ConflictTerm:  t,
			ConflictIndex: r.firstIndexOfTerm(m.Index, t),
			ReadID:        m.ReadID,
		})
		return
	}
	for i, e := range m.Entries {
		if e.Index > last || r.termAt(e.Index) != e.Term {
			if err := r.store.Append(m.Entries[i:]); err != nil {
				panic(fmt.Sprintf("raft: append: %v", err))
			}
			break
		}
	}
	lastNew := m.Index + uint64(len(m.Entries))
	if m.Commit > r.commit {
		r.commit = min(m.Commit, lastNew)
	}
	r.send(Message{Type: MsgAppResp, To: m.From, Index: lastNew, ReadID: m.ReadID})
}

// firstIndexOfTerm walks back from index over entries of term, stopping at
// the compaction boundary.
func (r *Raft) firstIndexOfTerm(index, term uint64) uint64 {
	first := r.store.FirstIndex()
	for index > first && r.termAt(index-1) == term {
		index--
	}
	return index
}

func (r *Raft) handleAppendResp(m Message) {
	// Any response in the current term, accepted or not, shows the
	// follower still recognizes this leader.
	if m.ReadID != 0 {
		r.ackRead(m.ReadID, m.From)
	}
	if m.Reject {
		next := m.ConflictIndex
		if m.ConflictTerm > 0 {
			// If the leader holds entries of the conflicting term, the
			// follower's log agrees up to the last of them.
			if last, ok := r.lastIndexOfTerm(m.ConflictTerm); ok {
				next = last + 1
			}
		}
		if next <= r.match[m.From] {
			next = r.match[m.From] + 1
		}
		if next < 1 {
			next = 1
		}
		r.next[m.From] = next
		r.sendAppend(m.From, 0)
		return
	}
	if m.Index > r.match[m.From] {
		r.match[m.From] = m.Index
	}
	if m.Index+1 > r.next[m.From] {
		r.next[m.From] = m.Index + 1
	}
	if r.maybeCommit() {
		r.broadcastAppend()
	} else if r.next[m.From] <= r.store.LastIndex() {
		r.sendAppend(m.From, 0)
	}
}

func (r *Raft) lastIndexOfTerm(term uint64) (uint64, bool) {
	first := r.store.FirstIndex()
	for i := r.store.LastIndex(); i >= first; i-- {
		t := r.termAt(i)
		if t == term {
			return i, true
		}
		if t < term {
			break
		}
	}
	return 0, false
}

// maybeCommit advances the commit index to the highest index replicated
// on a majority, but only if that entry is from the current term. An
// entry from an earlier term could still be overwritten by a leader that
// never saw it, no matter how many nodes hold it (Raft §5.4.2).
func (r *Raft) maybeCommit() bool {
	matches := make([]uint64, 0, len(r.peers))
	for _, p := range r.peers {
		matches = append(matches, r.match[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	n := matches[r.quorum()-1]
	if n <= r.commit || r.termAt(n) != r.term {
		return false
	}
	r.commit = n
	return true
}
