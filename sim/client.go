package sim

import (
	"fmt"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"
	"google.golang.org/protobuf/proto"

	"github.com/arifisme/keystone/proto"
	"github.com/arifisme/keystone/raft"
)

type opKind uint8

const (
	opRegister opKind = iota
	opGet
	opPut
	opDelete
	opCas
	opScan
)

func (k opKind) String() string {
	return [...]string{"register", "get", "put", "delete", "cas", "scan"}[k]
}

// clientOp is one logical operation. It keeps its sequence number across
// retries, exactly as the real client does, so the history records one
// invocation and one response no matter how many attempts it took.
type clientOp struct {
	kind     opKind
	key      string
	value    string
	expected string
	absent   bool
	mode     pb.ReadMode
	seq      uint64
	invoked  time.Duration
	attempt  uint64
	node     *node
	prop     *raft.Proposal
	read     *raft.Read
}

type client struct {
	id       int
	clientID uint64
	seq      uint64
	hint     raft.NodeID
	op       *clientOp
	done     int
	// attempts counts every issue across operations, so a timeout event
	// can tell whether it still refers to the attempt it was armed for.
	attempts uint64
}

const (
	opTimeout  = 400 * time.Millisecond
	retryDelay = 20 * time.Millisecond
)

func (s *Sim) clientEvent(e *event) {
	c := s.clients[e.client]
	if c.op == nil {
		if s.now >= s.cfg.Faults+s.cfg.Quiet/2 {
			return
		}
		c.op = s.newOp(c)
	}
	s.issue(c)
}

func (s *Sim) newOp(c *client) *clientOp {
	op := &clientOp{invoked: s.now}
	if c.clientID == 0 {
		op.kind = opRegister
		return op
	}
	c.seq++
	op.seq = c.seq
	op.key = fmt.Sprintf("k%d", s.rng.Intn(s.cfg.Keys))
	switch r := s.rng.Intn(100); {
	case r < 30:
		op.kind = opGet
	case r < 60:
		op.kind = opPut
		op.value = fmt.Sprintf("c%d-%d", c.id, op.seq)
	case r < 70:
		op.kind = opDelete
	case r < 90:
		op.kind = opCas
		op.value = fmt.Sprintf("c%d-%d", c.id, op.seq)
		if s.rng.Intn(3) == 0 {
			op.absent = true
		} else {
			op.expected = fmt.Sprintf("c%d-%d", s.rng.Intn(s.cfg.Clients), 1+s.rng.Intn(int(c.seq)))
		}
	default:
		op.kind = opScan
	}
	if s.rng.Intn(2) == 0 {
		op.mode = pb.ReadMode_LOG
	}
	return op
}

func (s *Sim) issue(c *client) {
	op := c.op
	var target *node
	if c.hint != raft.None && s.nodes[c.hint-1].up {
		target = s.nodes[c.hint-1]
	} else {
		target = s.randomUp()
	}
	if target == nil {
		s.schedule(retryDelay, &event{kind: evClient, client: c.id})
		return
	}
	op.attempt++
	c.attempts++
	op.node = target
	if op.attempt > 1 {
		s.stats.Retries++
	}
	s.run(target, func() {
		if (op.kind == opGet || op.kind == opScan) && op.mode == pb.ReadMode_READ_INDEX {
			op.read = target.rn.ReadIndex()
		} else {
			op.prop = target.rn.Propose(s.encode(c, op))
		}
		target.rn.Flush()
	})
	s.schedule(opTimeout, &event{kind: evClientTimeout, client: c.id, attempt: c.attempts})
}

func (s *Sim) encode(c *client, op *clientOp) []byte {
	cmd := &pb.Command{ClientId: c.clientID, Seq: op.seq}
	switch op.kind {
	case opRegister:
		cmd.Op = &pb.Command_Register{Register: &pb.RegisterOp{}}
	case opGet:
		cmd.Op = &pb.Command_Get{Get: &pb.GetOp{Key: []byte(op.key)}}
	case opPut:
		cmd.Op = &pb.Command_Put{Put: &pb.PutOp{Key: []byte(op.key), Value: []byte(op.value)}}
	case opDelete:
		cmd.Op = &pb.Command_Delete{Delete: &pb.DeleteOp{Key: []byte(op.key)}}
	case opCas:
		cas := &pb.CasOp{Key: []byte(op.key), Value: []byte(op.value)}
		if !op.absent {
			cas.Expected = []byte(op.expected)
		}
		cmd.Op = &pb.Command_Cas{Cas: cas}
	case opScan:
		cmd.Op = &pb.Command_Scan{Scan: &pb.ScanOp{}}
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		panic(err)
	}
	return data
}

func (s *Sim) clientTimeout(e *event) {
	c := s.clients[e.client]
	if c.op == nil || c.attempts != e.attempt || (c.op.prop == nil && c.op.read == nil) {
		return
	}
	s.stats.Timeouts++
	s.abandon(c)
	c.hint = raft.None
	s.issue(c)
}

func (s *Sim) abandon(c *client) {
	c.op.prop, c.op.read, c.op.node = nil, nil, nil
}

// settle collects responses for every in-flight operation.
func (s *Sim) settle() {
	for _, c := range s.clients {
		op := c.op
		if op == nil || op.node == nil {
			continue
		}
		switch {
		case op.prop != nil:
			select {
			case <-op.prop.Done():
			default:
				continue
			}
			if op.prop.Err != nil {
				s.retry(c)
				continue
			}
			var res pb.Result
			if err := proto.Unmarshal(op.prop.Result, &res); err != nil {
				panic(err)
			}
			s.respond(c, &res)
		case op.read != nil:
			select {
			case <-op.read.Done():
			default:
				continue
			}
			if op.read.Err != nil {
				s.retry(c)
				continue
			}
			res := &pb.Result{}
			if op.kind == opGet {
				v, found, err := op.node.sm.inner.Get([]byte(op.key))
				if err != nil {
					panic(err)
				}
				res.Found, res.Value = found, v
			} else {
				kvs, err := op.node.sm.inner.Scan(nil, nil, 0)
				if err != nil {
					panic(err)
				}
				res.Kvs = kvs
			}
			s.respond(c, res)
		}
	}
}

func (s *Sim) retry(c *client) {
	if c.op.node.up {
		c.hint = c.op.node.rn.Status().Leader
	}
	s.abandon(c)
	s.schedule(retryDelay, &event{kind: evClient, client: c.id})
}

func (s *Sim) respond(c *client, res *pb.Result) {
	op := c.op
	c.hint = op.node.id
	switch op.kind {
	case opRegister:
		c.clientID = res.ClientId
	case opScan:
		for i := 1; i < len(res.Kvs); i++ {
			if string(res.Kvs[i-1].Key) >= string(res.Kvs[i].Key) {
				s.violation("scan returned unsorted keys %q >= %q", res.Kvs[i-1].Key, res.Kvs[i].Key)
			}
		}
	default:
		s.history = append(s.history, porcupine.Operation{
			ClientId: c.id,
			Input:    kvInput{kind: op.kind, key: op.key, value: op.value, expected: op.expected, absent: op.absent},
			Output:   kvOutput{value: string(res.Value), found: res.Found, ok: res.Success},
			Call:     int64(op.invoked),
			Return:   int64(s.now),
		})
	}
	c.done++
	s.stats.Ops++
	c.op = nil
	s.schedule(time.Duration(s.rng.Int63n(int64(30*time.Millisecond))), &event{kind: evClient, client: c.id})
}

type kvInput struct {
	kind     opKind
	key      string
	value    string
	expected string
	absent   bool
}

type kvOutput struct {
	value string
	found bool
	ok    bool
}

// kvModel is a register per key with put, delete, get and compare-and-
// swap. The empty string is "absent"; values written by the simulator are
// never empty.
var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(kvInput).key
			byKey[k] = append(byKey[k], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(string)
		in := input.(kvInput)
		out := output.(kvOutput)
		switch in.kind {
		case opPut:
			return true, in.value
		case opDelete:
			return true, ""
		case opGet:
			return out.found == (st != "") && out.value == st, st
		case opCas:
			matches := (in.absent && st == "") || (!in.absent && st == in.expected)
			if out.ok != matches || out.found != (st != "") || out.value != st {
				return false, st
			}
			if matches {
				return true, in.value
			}
			return true, st
		}
		return false, st
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(kvInput)
		out := output.(kvOutput)
		return fmt.Sprintf("%s(%s,%q,exp=%q/%v) -> %q found=%v ok=%v", in.kind, in.key, in.value, in.expected, in.absent, out.value, out.found, out.ok)
	},
}
