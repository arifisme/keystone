package shard

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

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
//
// The shard map is cached and refreshed every second, and at once when a
// group answers that a shard is moving.
type Router struct {
	pb.UnimplementedKVServer
	local   *kv.Server
	meta    *kv.Client
	clients map[uint64]*kv.Client
	groups  []uint64

	mu     sync.RWMutex
	shards *pb.ShardMap
	raw    []byte
	stop   chan struct{}
}

const (
	mapRefresh   = time.Second
	movingRetry  = 50 * time.Millisecond
	movingTries  = 4
	importChunk  = 1000
	movingStatus = "shard is moving"
)

func NewRouter(node *kv.Node, groups map[uint64][]raft.NodeID) (*Router, error) {
	r := &Router{local: node.Server(), clients: map[uint64]*kv.Client{}, stop: make(chan struct{})}
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
	go r.refreshLoop()
	return r, nil
}

func (r *Router) Close() {
	close(r.stop)
}

func (r *Router) refreshLoop() {
	t := time.NewTicker(mapRefresh)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), mapRefresh)
			r.reload(ctx)
			cancel()
		}
	}
}

// reload fetches the shard map from the meta group. A fresh cluster has
// none; the first router to notice writes the default assignment with a
// compare-and-swap so concurrent routers agree.
func (r *Router) reload(ctx context.Context) (*pb.ShardMap, error) {
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
			if len(sm.Moving) != len(sm.Groups) {
				sm.Moving = make([]uint64, len(sm.Groups))
			}
			r.mu.Lock()
			r.shards, r.raw = &sm, res.Value
			r.mu.Unlock()
			return &sm, nil
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

func (r *Router) shardMap(ctx context.Context) (*pb.ShardMap, error) {
	r.mu.RLock()
	m := r.shards
	r.mu.RUnlock()
	if m != nil {
		return m, nil
	}
	return r.reload(ctx)
}

func (r *Router) groupFor(ctx context.Context, key []byte) (uint64, *kv.Client, error) {
	if len(key) == 0 {
		return 0, nil, status.Error(codes.InvalidArgument, "empty key")
	}
	m, err := r.shardMap(ctx)
	if err != nil {
		return 0, nil, err
	}
	g := m.Groups[ShardOf(key)]
	c, ok := r.clients[g]
	if !ok {
		return 0, nil, status.Errorf(codes.Internal, "shard map names unknown group %d", g)
	}
	return g, c, nil
}

func isMoving(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Aborted && st.Message() == movingStatus
}

// forward routes one keyed request. When the group answers that the
// shard is moving, the map is reloaded and the request tried again a few
// times; if it is still moving the caller gets Unavailable and retries.
func (r *Router) forward(ctx context.Context, key []byte, op func(ctx context.Context, cli pb.KVClient, group uint64) error) error {
	for attempt := 0; ; attempt++ {
		g, c, err := r.groupFor(ctx, key)
		if err != nil {
			return err
		}
		err = c.Do(ctx, func(ctx context.Context, cli pb.KVClient) error { return op(ctx, cli, g) })
		if !isMoving(err) {
			return err
		}
		if attempt == movingTries {
			return status.Error(codes.Unavailable, movingStatus)
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(movingRetry):
		}
		if _, err := r.reload(ctx); err != nil {
			return err
		}
	}
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
	var res *pb.PutResponse
	err := r.forward(ctx, req.Key, func(ctx context.Context, cli pb.KVClient, g uint64) error {
		fwd := proto.Clone(req).(*pb.PutRequest)
		fwd.Group = g
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
	var res *pb.DeleteResponse
	err := r.forward(ctx, req.Key, func(ctx context.Context, cli pb.KVClient, g uint64) error {
		fwd := proto.Clone(req).(*pb.DeleteRequest)
		fwd.Group = g
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
	var res *pb.CasResponse
	err := r.forward(ctx, req.Key, func(ctx context.Context, cli pb.KVClient, g uint64) error {
		fwd := proto.Clone(req).(*pb.CasRequest)
		fwd.Group = g
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
	var res *pb.GetResponse
	err := r.forward(ctx, req.Key, func(ctx context.Context, cli pb.KVClient, g uint64) error {
		fwd := proto.Clone(req).(*pb.GetRequest)
		fwd.Group = g
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

func (r *Router) Admin(ctx context.Context, req *pb.AdminRequest) (*pb.AdminResponse, error) {
	return r.local.Admin(ctx, req)
}

func (r *Router) GetShardMap(ctx context.Context, _ *pb.ShardMapRequest) (*pb.ShardMap, error) {
	return r.reload(ctx)
}

// MoveShard is a stop-the-shard move: the source group stops writing the
// shard, its keys are copied into the destination through that group's
// log, the meta group flips the map entry, and the source drops the keys
// but keeps the shard closed so a router with a stale map cannot write
// there. Writes to the shard fail with Unavailable while it moves and
// clients retry.
func (r *Router) MoveShard(ctx context.Context, req *pb.MoveShardRequest) (*pb.MoveShardResponse, error) {
	shard := int(req.Shard)
	if shard < 0 || shard >= Shards {
		return nil, status.Errorf(codes.InvalidArgument, "shard %d out of range", shard)
	}
	dest, ok := r.clients[req.Group]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown group %d", req.Group)
	}
	m, err := r.reload(ctx)
	if err != nil {
		return nil, err
	}
	src := m.Groups[shard]
	if src == req.Group {
		return &pb.MoveShardResponse{}, nil
	}
	if m.Moving[shard] != 0 && m.Moving[shard] != req.Group {
		return nil, status.Errorf(codes.FailedPrecondition, "shard %d is moving to group %d; run that move again to finish it first", shard, m.Moving[shard])
	}
	r.mu.RLock()
	marked := r.raw
	r.mu.RUnlock()
	// A map that already names this move is one that was interrupted, and
	// every step below can be taken a second time.
	if m.Moving[shard] == 0 {
		m.Moving[shard] = req.Group
		if marked, err = r.swapMap(ctx, marked, m); err != nil {
			return nil, err
		}
	}
	if _, err := r.admin(ctx, r.clients[src], src, &pb.Command{Op: &pb.Command_Freeze{Freeze: &pb.FreezeOp{Shard: uint32(shard)}}}); err != nil {
		return nil, err
	}
	if err := r.copyShard(ctx, shard, r.clients[src], src, dest, req.Group); err != nil {
		return nil, err
	}
	m.Groups[shard] = req.Group
	m.Moving[shard] = 0
	if _, err := r.swapMap(ctx, marked, m); err != nil {
		return nil, err
	}
	if _, err := r.admin(ctx, r.clients[src], src, &pb.Command{Op: &pb.Command_Purge{Purge: &pb.PurgeOp{Shard: uint32(shard)}}}); err != nil {
		return nil, err
	}
	return &pb.MoveShardResponse{}, nil
}

// swapMap replaces the stored map only if it still equals expected.
func (r *Router) swapMap(ctx context.Context, expected []byte, m *pb.ShardMap) ([]byte, error) {
	data, err := proto.Marshal(m)
	if err != nil {
		return nil, err
	}
	var swapped bool
	err = r.meta.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		res, err := cli.Cas(ctx, &pb.CasRequest{Key: mapKey, Expected: expected, Value: data, Group: MetaGroup})
		if err != nil {
			return err
		}
		swapped = res.Success
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !swapped {
		return nil, status.Error(codes.Aborted, "shard map changed underneath the move")
	}
	r.mu.Lock()
	r.shards, r.raw = m, data
	r.mu.Unlock()
	return data, nil
}

func (r *Router) admin(ctx context.Context, c *kv.Client, group uint64, cmd *pb.Command) (*pb.Result, error) {
	var res *pb.AdminResponse
	err := c.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		var err error
		res, err = cli.Admin(ctx, &pb.AdminRequest{Group: group, Command: cmd})
		return err
	})
	if err != nil {
		return nil, err
	}
	return res.Result, nil
}

// copyShard pages through the source group and imports the shard's keys
// in chunks; the last chunk opens the shard in the destination. Pages and
// chunks are bounded by kv.MaxRequestBytes as well as by count, which keeps
// both under what gRPC will carry: no pair is larger than that, so a chunk
// never is, and a page is over it by one pair at most.
func (r *Router) copyShard(ctx context.Context, shard int, src *kv.Client, srcID uint64, dest *kv.Client, destID uint64) error {
	var start []byte
	var chunk []*pb.KeyValue
	chunkBytes := 0
	// held reports that the destination refused the chunk because it holds
	// the whole shard already, from an earlier run of this move.
	flush := func(last bool) (held bool, err error) {
		res, err := r.admin(ctx, dest, destID, &pb.Command{Op: &pb.Command_Import{Import: &pb.ImportOp{Shard: uint32(shard), Kvs: chunk, Last: last}}})
		chunk, chunkBytes = nil, 0
		if err != nil {
			return false, err
		}
		return !res.Success, nil
	}
	for {
		var page []*pb.KeyValue
		err := src.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
			res, err := cli.Scan(ctx, &pb.ScanRequest{Start: start, Limit: importChunk, MaxBytes: kv.MaxRequestBytes, Mode: pb.ReadMode_LOG, Group: srcID})
			if err != nil {
				return err
			}
			page = res.Kvs
			return nil
		})
		if err != nil {
			return err
		}
		if len(page) == 0 {
			_, err := flush(true)
			return err
		}
		for _, pair := range page {
			if ShardOf(pair.Key) != shard {
				continue
			}
			size := len(pair.Key) + len(pair.Value)
			if len(chunk) >= importChunk || (len(chunk) > 0 && chunkBytes+size > kv.MaxRequestBytes) {
				if held, err := flush(false); err != nil || held {
					return err
				}
			}
			chunk = append(chunk, pair)
			chunkBytes += size
		}
		start = append(append([]byte(nil), page[len(page)-1].Key...), 0)
	}
}
