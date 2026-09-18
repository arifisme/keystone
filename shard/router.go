package shard

import (
	"bytes"
	"context"
	"sort"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/arifisme/keystone/kv"
	"github.com/arifisme/keystone/proto"
	"github.com/arifisme/keystone/raft"
)

// Router is the client-facing service on every node of a sharded
// cluster. A request that names a group is served by the local replica;
// one that does not is hashed to a shard, mapped to a group through the
// shard map, and forwarded to that group's leader with the caller's
// session untouched. Sessions are issued by the meta group so their ids
// are unique across groups; every group accepts a session it has not
// seen and starts tracking it.
type Router struct {
	pb.UnimplementedKVServer
	local   *kv.Server
	meta    *kv.Client
	clients map[uint64]*kv.Client
	groups  []uint64

	mu     sync.RWMutex
	shards []uint64
}

func NewRouter(node *kv.Node, groups map[uint64][]raft.NodeID) (*Router, error) {
	r := &Router{local: node.Server(), clients: map[uint64]*kv.Client{}}
	for id, members := range groups {
		var addrs []string
		for _, m := range members {
			addrs = append(addrs, node.Peers()[m])
		}
		c, err := kv.NewClient(addrs, kv.ClientOptions{Group: id})
		if err != nil {
			return nil, err
		}
		if id == MetaGroup {
			r.meta = c
			continue
		}
		r.clients[id] = c
		r.groups = append(r.groups, id)
	}
	if r.meta == nil {
		return nil, status.Error(codes.InvalidArgument, "no meta group configured")
	}
	if len(r.groups) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no shard groups configured")
	}
	sort.Slice(r.groups, func(i, j int) bool { return r.groups[i] < r.groups[j] })
	return r, nil
}

// shardMap returns the cached map, loading it from the meta group on
// first use. A fresh cluster has none; the first router to notice writes
// the default map with a compare-and-swap so concurrent routers agree.
func (r *Router) shardMap(ctx context.Context) ([]uint64, error) {
	r.mu.RLock()
	m := r.shards
	r.mu.RUnlock()
	if m != nil {
		return m, nil
	}
	for {
		var res *pb.GetResponse
		err := r.meta.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
			var err error
			res, err = cli.Get(ctx, &pb.GetRequest{Key: mapKey, Mode: pb.ReadMode_LOG, Group: MetaGroup})
			return err
		})
		if err != nil {
			return nil, err
		}
		if res.Found {
			var sm pb.ShardMap
			if err := proto.Unmarshal(res.Value, &sm); err != nil {
				return nil, status.Error(codes.Internal, "shard map is corrupt")
			}
			r.mu.Lock()
			r.shards = sm.Groups
			r.mu.Unlock()
			return sm.Groups, nil
		}
		data, _ := proto.Marshal(DefaultMap(r.groups))
		err = r.meta.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
			_, err := cli.Cas(ctx, &pb.CasRequest{Key: mapKey, Value: data, Group: MetaGroup})
			return err
		})
		if err != nil {
			return nil, err
		}
	}
}

func (r *Router) groupFor(ctx context.Context, key []byte) (uint64, *kv.Client, error) {
	if len(key) == 0 {
		return 0, nil, status.Error(codes.InvalidArgument, "empty key")
	}
	m, err := r.shardMap(ctx)
	if err != nil {
		return 0, nil, err
	}
	g := m[ShardOf(key)]
	c, ok := r.clients[g]
	if !ok {
		return 0, nil, status.Errorf(codes.Internal, "shard map names unknown group %d", g)
	}
	return g, c, nil
}

func (r *Router) RegisterClient(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if req.Group != 0 {
		return r.local.RegisterClient(ctx, req)
	}
	var res *pb.RegisterResponse
	err := r.meta.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		var err error
		res, err = cli.RegisterClient(ctx, &pb.RegisterRequest{Group: MetaGroup})
		return err
	})
	return res, err
}

func (r *Router) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if req.Group != 0 {
		return r.local.Put(ctx, req)
	}
	g, c, err := r.groupFor(ctx, req.Key)
	if err != nil {
		return nil, err
	}
	fwd := proto.Clone(req).(*pb.PutRequest)
	fwd.Group = g
	var res *pb.PutResponse
	err = c.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		var err error
		res, err = cli.Put(ctx, fwd)
		return err
	})
	return res, err
}

func (r *Router) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if req.Group != 0 {
		return r.local.Delete(ctx, req)
	}
	g, c, err := r.groupFor(ctx, req.Key)
	if err != nil {
		return nil, err
	}
	fwd := proto.Clone(req).(*pb.DeleteRequest)
	fwd.Group = g
	var res *pb.DeleteResponse
	err = c.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		var err error
		res, err = cli.Delete(ctx, fwd)
		return err
	})
	return res, err
}

func (r *Router) Cas(ctx context.Context, req *pb.CasRequest) (*pb.CasResponse, error) {
	if req.Group != 0 {
		return r.local.Cas(ctx, req)
	}
	g, c, err := r.groupFor(ctx, req.Key)
	if err != nil {
		return nil, err
	}
	fwd := proto.Clone(req).(*pb.CasRequest)
	fwd.Group = g
	var res *pb.CasResponse
	err = c.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		var err error
		res, err = cli.Cas(ctx, fwd)
		return err
	})
	return res, err
}

func (r *Router) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req.Group != 0 {
		return r.local.Get(ctx, req)
	}
	g, c, err := r.groupFor(ctx, req.Key)
	if err != nil {
		return nil, err
	}
	fwd := proto.Clone(req).(*pb.GetRequest)
	fwd.Group = g
	var res *pb.GetResponse
	err = c.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		var err error
		res, err = cli.Get(ctx, fwd)
		return err
	})
	return res, err
}

// Scan asks every group for its part of the range and merges the sorted
// results. Each group answers from its own consistent point; the merged
// result is not a single snapshot across shards.
func (r *Router) Scan(ctx context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	if req.Group != 0 {
		return r.local.Scan(ctx, req)
	}
	if _, err := r.shardMap(ctx); err != nil {
		return nil, err
	}
	results := make([][]*pb.KeyValue, len(r.groups))
	errs := make([]error, len(r.groups))
	var wg sync.WaitGroup
	for i, g := range r.groups {
		fwd := proto.Clone(req).(*pb.ScanRequest)
		fwd.Group = g
		wg.Add(1)
		go func(i int, c *kv.Client) {
			defer wg.Done()
			errs[i] = c.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
				res, err := cli.Scan(ctx, fwd)
				if err != nil {
					return err
				}
				results[i] = res.Kvs
				return nil
			})
		}(i, r.clients[g])
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	var merged []*pb.KeyValue
	for _, part := range results {
		merged = append(merged, part...)
	}
	sort.Slice(merged, func(i, j int) bool { return bytes.Compare(merged[i].Key, merged[j].Key) < 0 })
	if req.Limit > 0 && len(merged) > int(req.Limit) {
		merged = merged[:req.Limit]
	}
	return &pb.ScanResponse{Kvs: merged}, nil
}
