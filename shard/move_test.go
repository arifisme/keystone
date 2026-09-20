package shard

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/arifisme/keystone/kv"
	"github.com/arifisme/keystone/proto"
)

// fakeGroups is one gRPC server standing in for the meta group and every
// shard group. It keeps the shard map and the source group's keys, and
// records each step of a move in the order the router took it, which a
// real cluster cannot show.
type fakeGroups struct {
	pb.UnimplementedKVServer

	mu           sync.Mutex
	shard        int
	shardMap     []byte
	source       []*pb.KeyValue
	steps        []string
	pageBounds   []uint32
	chunkBytes   []int
	alreadyHolds bool
}

func (f *fakeGroups) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &pb.GetResponse{Found: true, Value: f.shardMap}, nil
}

func (f *fakeGroups) Cas(_ context.Context, req *pb.CasRequest) (*pb.CasResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !bytes.Equal(req.Expected, f.shardMap) {
		return &pb.CasResponse{Found: true, Current: f.shardMap}, nil
	}
	var before, after pb.ShardMap
	proto.Unmarshal(f.shardMap, &before)
	proto.Unmarshal(req.Value, &after)
	step := "mark"
	if after.Groups[f.shard] != before.Groups[f.shard] {
		step = "flip"
	}
	f.steps = append(f.steps, step)
	f.shardMap = req.Value
	return &pb.CasResponse{Success: true}, nil
}

func (f *fakeGroups) Scan(_ context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pageBounds = append(f.pageBounds, req.MaxBytes)
	res := &pb.ScanResponse{}
	size := 0
	for _, kv := range f.source {
		if bytes.Compare(kv.Key, req.Start) < 0 {
			continue
		}
		if req.Limit > 0 && len(res.Kvs) >= int(req.Limit) {
			break
		}
		res.Kvs = append(res.Kvs, kv)
		if size += len(kv.Key) + len(kv.Value); req.MaxBytes > 0 && size >= int(req.MaxBytes) {
			break
		}
	}
	return res, nil
}

func (f *fakeGroups) Admin(_ context.Context, req *pb.AdminRequest) (*pb.AdminResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	res := &pb.Result{Success: true}
	switch op := req.Command.Op.(type) {
	case *pb.Command_Freeze:
		f.steps = append(f.steps, fmt.Sprintf("freeze@%d", req.Group))
	case *pb.Command_Purge:
		f.steps = append(f.steps, fmt.Sprintf("purge@%d", req.Group))
		f.source = nil
	case *pb.Command_Import:
		f.steps = append(f.steps, fmt.Sprintf("import@%d", req.Group))
		size := 0
		for _, kv := range op.Import.Kvs {
			size += len(kv.Key) + len(kv.Value)
		}
		f.chunkBytes = append(f.chunkBytes, size)
		res.Success = !f.alreadyHolds
	}
	return &pb.AdminResponse{Result: res}, nil
}

// startFakeMove serves fake groups 2, 3 and 4 with every shard in group 2
// and returns a router wired to them. moving is the map's entry for a move
// of shard already under way, zero for none.
func startFakeMove(t *testing.T, shard int, moving uint64, source []*pb.KeyValue) (*Router, *fakeGroups) {
	t.Helper()
	m := DefaultMap([]uint64{2})
	m.Moving[shard] = moving
	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(source, func(i, j int) bool { return bytes.Compare(source[i].Key, source[j].Key) < 0 })
	f := &fakeGroups{shard: shard, shardMap: data, source: source}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterKVServer(srv, f)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	r := &Router{clients: map[uint64]*kv.Client{}, groups: []uint64{2, 3, 4}, stop: make(chan struct{})}
	for _, g := range []uint64{MetaGroup, 2, 3, 4} {
		c, err := kv.NewClient([]string{lis.Addr().String()}, kv.ClientOptions{Group: g})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		if g == MetaGroup {
			r.meta = c
		} else {
			r.clients[g] = c
		}
	}
	return r, f
}

// keysInShard returns n pairs whose keys hash to shard.
func keysInShard(shard, n, valueSize int) []*pb.KeyValue {
	var out []*pb.KeyValue
	for i := 0; len(out) < n; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		if ShardOf(k) == shard {
			out = append(out, &pb.KeyValue{Key: k, Value: bytes.Repeat([]byte("x"), valueSize)})
		}
	}
	return out
}

func TestMoveCopiesLargeValuesInRequestsGRPCWillCarry(t *testing.T) {
	r, f := startFakeMove(t, 5, 0, keysInShard(5, 12, 300<<10))
	if _, err := r.MoveShard(context.Background(), &pb.MoveShardRequest{Shard: 5, Group: 3}); err != nil {
		t.Fatal(err)
	}
	for _, bound := range f.pageBounds {
		if bound == 0 || bound > kv.MaxRequestBytes {
			t.Fatalf("scanned the source with page bounds %v, want every page bounded by %d bytes", f.pageBounds, kv.MaxRequestBytes)
		}
	}
	total := 0
	for _, size := range f.chunkBytes {
		if size > kv.MaxRequestBytes {
			t.Fatalf("import chunks of %v bytes, want none over %d", f.chunkBytes, kv.MaxRequestBytes)
		}
		total += size
	}
	if want := 12 * (300<<10 + len("key0")); total < want {
		t.Fatalf("imported %d bytes, the shard holds at least %d", total, want)
	}
}
