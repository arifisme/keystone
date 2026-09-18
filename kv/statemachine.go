package kv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/arifisme/keystone/proto"
	"github.com/arifisme/keystone/raft"
	"github.com/arifisme/keystone/storage"
)

// Key layout in the engine:
//
//	'k' user key            value
//	's' client id (8 bytes) seq (8 bytes) | cached Result
//	'm' "applied"           last applied index (8 bytes)
//
// Every Apply writes its effects, the session update and the applied
// index in one batch, so a crash can never leave the state machine
// between entries. Keeping sessions in the engine also makes them part of
// the snapshot for free.
const (
	prefixKey     = 'k'
	prefixSession = 's'
	prefixMeta    = 'm'
)

var appliedKey = []byte("mapplied")

type session struct {
	seq    uint64
	result []byte
}

type StateMachine struct {
	db storage.DB

	mu       sync.Mutex
	applied  uint64
	sessions map[uint64]session
}

func NewStateMachine(db storage.DB) (*StateMachine, error) {
	s := &StateMachine{db: db}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *StateMachine) load() error {
	s.sessions = map[uint64]session{}
	s.applied = 0
	v, err := s.db.Get(appliedKey)
	if err == nil {
		s.applied = binary.BigEndian.Uint64(v)
	} else if err != storage.ErrNotFound {
		return err
	}
	it := s.db.Scan([]byte{prefixSession}, []byte{prefixSession + 1})
	for it.Next() {
		id := binary.BigEndian.Uint64(it.Key()[1:])
		s.sessions[id] = decodeSession(it.Value())
	}
	return it.Close()
}

func userKey(key []byte) []byte {
	return append([]byte{prefixKey}, key...)
}

func sessionKey(id uint64) []byte {
	k := make([]byte, 9)
	k[0] = prefixSession
	binary.BigEndian.PutUint64(k[1:], id)
	return k
}

func encodeSession(seq uint64, result []byte) []byte {
	v := make([]byte, 8, 8+len(result))
	binary.BigEndian.PutUint64(v, seq)
	return append(v, result...)
}

func decodeSession(v []byte) session {
	return session{seq: binary.BigEndian.Uint64(v), result: append([]byte(nil), v[8:]...)}
}

func (s *StateMachine) LastApplied() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied
}

func (s *StateMachine) Apply(e raft.Entry) []byte {
	var cmd pb.Command
	if err := proto.Unmarshal(e.Data, &cmd); err != nil {
		panic(fmt.Sprintf("kv: entry %d is not a command: %v", e.Index, err))
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var batch storage.Batch
	var result []byte
	if sess, ok := s.sessions[cmd.ClientId]; ok && cmd.ClientId != 0 && cmd.Seq <= sess.seq {
		result = sess.result
	} else {
		res := s.execute(&cmd, e.Index, &batch)
		result, _ = proto.Marshal(res)
		if cmd.ClientId != 0 {
			batch.Put(sessionKey(cmd.ClientId), encodeSession(cmd.Seq, result))
			s.sessions[cmd.ClientId] = session{seq: cmd.Seq, result: result}
		}
		if res.ClientId != 0 {
			batch.Put(sessionKey(res.ClientId), encodeSession(0, nil))
			s.sessions[res.ClientId] = session{}
		}
	}
	var idx [8]byte
	binary.BigEndian.PutUint64(idx[:], e.Index)
	batch.Put(appliedKey, idx[:])
	if err := s.db.Write(&batch); err != nil {
		panic(fmt.Sprintf("kv: apply %d: %v", e.Index, err))
	}
	s.applied = e.Index
	return result
}

func (s *StateMachine) execute(cmd *pb.Command, index uint64, batch *storage.Batch) *pb.Result {
	res := &pb.Result{}
	switch op := cmd.Op.(type) {
	case *pb.Command_Register:
		res.ClientId = index
	case *pb.Command_Put:
		batch.Put(userKey(op.Put.Key), op.Put.Value)
		res.Success = true
	case *pb.Command_Delete:
		batch.Delete(userKey(op.Delete.Key))
		res.Success = true
	case *pb.Command_Cas:
		cur, found, err := s.get(op.Cas.Key)
		if err != nil {
			panic(fmt.Sprintf("kv: cas read: %v", err))
		}
		res.Found, res.Value = found, cur
		if op.Cas.Expected == nil {
			res.Success = !found
		} else {
			res.Success = found && bytes.Equal(cur, op.Cas.Expected)
		}
		if res.Success {
			batch.Put(userKey(op.Cas.Key), op.Cas.Value)
		}
	case *pb.Command_Get:
		cur, found, err := s.get(op.Get.Key)
		if err != nil {
			panic(fmt.Sprintf("kv: get: %v", err))
		}
		res.Found, res.Value = found, cur
	case *pb.Command_Scan:
		kvs, err := s.scan(op.Scan.Start, op.Scan.End, int(op.Scan.Limit))
		if err != nil {
			panic(fmt.Sprintf("kv: scan: %v", err))
		}
		res.Kvs = kvs
	}
	return res
}

func (s *StateMachine) get(key []byte) ([]byte, bool, error) {
	v, err := s.db.Get(userKey(key))
	if err == storage.ErrNotFound {
		return nil, false, nil
	}
	return v, err == nil, err
}

func (s *StateMachine) scan(start, end []byte, limit int) ([]*pb.KeyValue, error) {
	hi := []byte{prefixKey + 1}
	if len(end) > 0 {
		hi = userKey(end)
	}
	it := s.db.Scan(userKey(start), hi)
	var kvs []*pb.KeyValue
	for it.Next() {
		if limit > 0 && len(kvs) >= limit {
			break
		}
		kvs = append(kvs, &pb.KeyValue{Key: append([]byte(nil), it.Key()[1:]...), Value: append([]byte(nil), it.Value()...)})
	}
	return kvs, it.Close()
}

// Get serves a read outside the log; the caller has confirmed through
// ReadIndex that the state is current enough.
func (s *StateMachine) Get(key []byte) ([]byte, bool, error) {
	return s.get(key)
}

func (s *StateMachine) Scan(start, end []byte, limit int) ([]*pb.KeyValue, error) {
	return s.scan(start, end, limit)
}

func (s *StateMachine) Snapshot() (io.ReadCloser, error) {
	if err := s.db.Sync(); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := s.db.Export(&buf); err != nil {
		return nil, err
	}
	return io.NopCloser(&buf), nil
}

func (s *StateMachine) Restore(r io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.db.Restore(r); err != nil {
		return err
	}
	return s.load()
}
