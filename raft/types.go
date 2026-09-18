package raft

// NodeID identifies a member of a Raft group. Zero is never a valid ID.
type NodeID uint64

// None marks the absence of a node, e.g. no vote cast this term.
const None NodeID = 0

type EntryType uint8

const (
	// EntryNormal carries a state machine command.
	EntryNormal EntryType = iota
	// EntryNoop is appended by a new leader so that entries from earlier
	// terms can be committed under the own-term rule.
	EntryNoop
)

type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

type MsgType uint8

const (
	MsgVote MsgType = iota + 1
	MsgVoteResp
	MsgApp
	MsgAppResp
	MsgSnap
	MsgSnapResp
)

func (t MsgType) String() string {
	switch t {
	case MsgVote:
		return "Vote"
	case MsgVoteResp:
		return "VoteResp"
	case MsgApp:
		return "App"
	case MsgAppResp:
		return "AppResp"
	case MsgSnap:
		return "Snap"
	case MsgSnapResp:
		return "SnapResp"
	}
	return "Unknown"
}

// Message is the single wire type exchanged between nodes. Which fields are
// meaningful depends on Type:
//
//	MsgVote      Index, LogTerm = candidate's last log index and term.
//	MsgVoteResp  Reject = vote denied.
//	MsgApp       Index, LogTerm = prevLogIndex, prevLogTerm; Entries; Commit
//	             = leader commit index; ReadID = pending ReadIndex heartbeat.
//	MsgAppResp   on success Index = follower's new match index; on Reject
//	             ConflictTerm, ConflictIndex drive fast backtracking. ReadID
//	             is echoed from the request.
//	MsgSnap      Index, LogTerm = snapshot's last included index and term;
//	             Data = chunk at Offset; Done marks the final chunk.
//	MsgSnapResp  Index = follower's last index after install; Reject if the
//	             chunk was refused and the transfer must restart.
type Message struct {
	Type MsgType
	From NodeID
	To   NodeID
	Term uint64

	Index   uint64
	LogTerm uint64
	Entries []Entry
	Commit  uint64
	Reject  bool

	ConflictTerm  uint64
	ConflictIndex uint64

	ReadID uint64

	Data   []byte
	Offset uint64
	Done   bool
}
