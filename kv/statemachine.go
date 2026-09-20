package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/arifisme/keystone/internal/keyhash"
	"github.com/arifisme/keystone/proto"
	"github.com/arifisme/keystone/raft"
	"github.com/arifisme/keystone/storage"
)

// Key layout in the engine:
//
//	'k' user key            value
//	's' client id (8 bytes) seq (8 bytes) | cached Result
//	'm' "applied"           last applied index (8 bytes)
//	'm' "shard/" + n        shard state: 0 open after an import, 1 frozen, 2 gone
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

// shardState is what this group may do with a shard's keys. A shard this
// group has never heard of is open.
type shardState byte

const (
	shardOpen   shardState = 0
	shardFrozen shardState = 1
	shardGone   shardState = 2
)

type StateMachine struct {
	db storage.DB

	mu       sync.Mutex
	applied  uint64
	sessions map[uint64]session
	shards   map[int]shardState
}

func NewStateMachine(db storage.DB) (*StateMachine, error) {
	s := &StateMachine{db: db}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func shardKey(shard int) []byte {
	return []byte(fmt.Sprintf("mshard/%02d", shard))
}

func (s *StateMachine) load() error {
	s.sessions = map[uint64]session{}
	s.shards = map[int]shardState{}
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
	if err := it.Close(); err != nil {
		return err
	}
	it = s.db.Scan([]byte("mshard/"), []byte("mshard0"))
	for it.Next() {
		var shard int
		fmt.Sscanf(string(it.Key()[len("mshard/"):]), "%d", &shard)
		s.shards[shard] = shardState(it.Value()[0])
	}
	return it.Close()
}

func (s *StateMachine) setShard(batch *storage.Batch, shard int, state shardState) {
	batch.Put(shardKey(shard), []byte{byte(state)})
	s.shards[shard] = state
}

// writable reports whether a key's shard accepts writes here.
func (s *StateMachine) writable(key []byte) bool {
	return s.shards[keyhash.ShardOf(key)] == shardOpen
}

// readable reports whether a key's shard still holds data here.
func (s *StateMachine) readable(key []byte) bool {
	return s.shards[keyhash.ShardOf(key)] != shardGone
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
		if !s.writable(op.Put.Key) {
			res.Moving = true
			break
		}
		batch.Put(userKey(op.Put.Key), op.Put.Value)
		res.Success = true
	case *pb.Command_Delete:
		if !s.writable(op.Delete.Key) {
			res.Moving = true
			break
		}
		batch.Delete(userKey(op.Delete.Key))
		res.Success = true
	case *pb.Command_Cas:
		if !s.writable(op.Cas.Key) {
			res.Moving = true
			break
		}
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
		if !s.readable(op.Get.Key) {
			res.Moving = true
			break
		}
		cur, found, err := s.get(op.Get.Key)
		if err != nil {
			panic(fmt.Sprintf("kv: get: %v", err))
		}
		res.Found, res.Value = found, cur
	case *pb.Command_Scan:
		kvs, err := s.scan(op.Scan.Start, op.Scan.End, int(op.Scan.Limit), int(op.Scan.MaxBytes))
		if err != nil {
			panic(fmt.Sprintf("kv: scan: %v", err))
		}
		res.Kvs = kvs
	case *pb.Command_Freeze:
		s.setShard(batch, int(op.Freeze.Shard), shardFrozen)
		res.Success = true
	case *pb.Command_Import:
		// A shard with a state here other than gone is complete here: it
		// was imported to the end, or it started here and is frozen for a
		// move away. Clients may have written to it since, so a chunk that
		// arrives now, late or from a move run twice, must not land.
		// Success stays false, which tells the mover the copy is complete.
		if state, ok := s.shards[int(op.Import.Shard)]; ok && state != shardGone {
			break
		}
		for _, kv := range op.Import.Kvs {
			batch.Put(userKey(kv.Key), kv.Value)
		}
		if op.Import.Last {
			s.setShard(batch, int(op.Import.Shard), shardOpen)
		}
		res.Success = true
	case *pb.Command_Purge:
		shard := int(op.Purge.Shard)
		it := s.db.Scan([]byte{prefixKey}, []byte{prefixKey + 1})
		for it.Next() {
			if keyhash.ShardOf(it.Key()[1:]) == shard {
				batch.Delete(append([]byte(nil), it.Key()...))
			}
		}
		if err := it.Close(); err != nil {
			panic(fmt.Sprintf("kv: purge: %v", err))
		}
		s.setShard(batch, shard, shardGone)
		res.Success = true
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

// scan returns at most limit pairs, and stops after the pair with which
// the keys and values returned reach maxBytes. Zero means no bound.
func (s *StateMachine) scan(start, end []byte, limit, maxBytes int) ([]*pb.KeyValue, error) {
	hi := []byte{prefixKey + 1}
	if len(end) > 0 {
		hi = userKey(end)
	}
	it := s.db.Scan(userKey(start), hi)
	var kvs []*pb.KeyValue
	size := 0
	for it.Next() {
		if limit > 0 && len(kvs) >= limit {
			break
		}
		kvs = append(kvs, &pb.KeyValue{Key: append([]byte(nil), it.Key()[1:]...), Value: append([]byte(nil), it.Value()...)})
		if size += len(it.Key()) - 1 + len(it.Value()); maxBytes > 0 && size >= maxBytes {
			break
		}
	}
	return kvs, it.Close()
}

// Get serves a read outside the log; the caller has confirmed through
// ReadIndex that the state is current enough. ErrMoving means the key's
// shard has left this group.
func (s *StateMachine) Get(key []byte) ([]byte, bool, error) {
	s.mu.Lock()
	readable := s.readable(key)
	s.mu.Unlock()
	if !readable {
		return nil, false, ErrMoving
	}
	return s.get(key)
}

var ErrMoving = errors.New("kv: shard is moving")

func (s *StateMachine) Scan(start, end []byte, limit, maxBytes int) ([]*pb.KeyValue, error) {
	return s.scan(start, end, limit, maxBytes)
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
