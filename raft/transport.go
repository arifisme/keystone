package raft

// Transport moves messages between nodes. Send must not block the caller;
// a transport that cannot deliver drops the message, and Raft's retries
// cover the loss.
type Transport interface {
	Send(to NodeID, msg Message)
	Recv() <-chan Message
}
