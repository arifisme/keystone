package kv

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/arifisme/keystone/proto"
)

// Server serves the hosted groups over gRPC. Writes and log reads go
// through the log; ReadIndex reads wait for a leadership check and then
// read the state machine directly. A request without a group is served by
// the only hosted group, which is the unsharded deployment.
type Server struct {
	pb.UnimplementedKVServer
	node *Node
}

func (s *Server) group(id uint64) (*Group, error) {
	if id == 0 {
		if len(s.node.groups) != 1 {
			return nil, status.Error(codes.InvalidArgument, "request names no group")
		}
		for _, g := range s.node.groups {
			return g, nil
		}
	}
	g, ok := s.node.groups[id]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "group %d is not hosted here", id)
	}
	return g, nil
}

func (s *Server) propose(ctx context.Context, g *Group, cmd *pb.Command) (*pb.Result, error) {
	data, err := proto.Marshal(cmd)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	p := g.raft.Propose(data)
	select {
	case <-p.Done():
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if p.Err != nil {
		return nil, s.notLeader(g, p.Err)
	}
	var res pb.Result
	if err := proto.Unmarshal(p.Result, &res); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if res.Moving {
		return nil, errMoving
	}
	return &res, nil
}

// errMoving tells a router its shard map is stale. Aborted is not retried
// by the client library, so the router gets to reload the map first.
var errMoving = status.Error(codes.Aborted, "shard is moving")

// readIndex blocks until a read may be served from local state.
func (s *Server) readIndex(ctx context.Context, g *Group) error {
	rd := g.raft.ReadIndex()
	select {
	case <-rd.Done():
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
	if rd.Err != nil {
		return s.notLeader(g, rd.Err)
	}
	return nil
}

func (s *Server) notLeader(g *Group, err error) error {
	st := status.New(codes.FailedPrecondition, err.Error())
	lead := g.raft.Status().Leader
	if detailed, derr := st.WithDetails(&pb.NotLeader{LeaderId: uint64(lead), LeaderAddr: s.node.cfg.Peers[lead]}); derr == nil {
		st = detailed
	}
	return st.Err()
}

func (s *Server) RegisterClient(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	g, err := s.group(req.Group)
	if err != nil {
		return nil, err
	}
	res, err := s.propose(ctx, g, &pb.Command{Op: &pb.Command_Register{Register: &pb.RegisterOp{}}})
	if err != nil {
		return nil, err
	}
	return &pb.RegisterResponse{ClientId: res.ClientId}, nil
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	g, err := s.group(req.Group)
	if err != nil {
		return nil, err
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	cmd := &pb.Command{Op: &pb.Command_Put{Put: &pb.PutOp{Key: req.Key, Value: req.Value}}}
	setSession(cmd, req.Session)
	if _, err := s.propose(ctx, g, cmd); err != nil {
		return nil, err
	}
	return &pb.PutResponse{}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	g, err := s.group(req.Group)
	if err != nil {
		return nil, err
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	cmd := &pb.Command{Op: &pb.Command_Delete{Delete: &pb.DeleteOp{Key: req.Key}}}
	setSession(cmd, req.Session)
	if _, err := s.propose(ctx, g, cmd); err != nil {
		return nil, err
	}
	return &pb.DeleteResponse{}, nil
}

func (s *Server) Cas(ctx context.Context, req *pb.CasRequest) (*pb.CasResponse, error) {
	g, err := s.group(req.Group)
	if err != nil {
		return nil, err
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	cmd := &pb.Command{Op: &pb.Command_Cas{Cas: &pb.CasOp{Key: req.Key, Expected: req.Expected, Value: req.Value}}}
	setSession(cmd, req.Session)
	res, err := s.propose(ctx, g, cmd)
	if err != nil {
		return nil, err
	}
	return &pb.CasResponse{Success: res.Success, Found: res.Found, Current: res.Value}, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	g, err := s.group(req.Group)
	if err != nil {
		return nil, err
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	if req.Mode == pb.ReadMode_LOG {
		cmd := &pb.Command{Op: &pb.Command_Get{Get: &pb.GetOp{Key: req.Key}}}
		setSession(cmd, req.Session)
		res, err := s.propose(ctx, g, cmd)
		if err != nil {
			return nil, err
		}
		return &pb.GetResponse{Found: res.Found, Value: res.Value}, nil
	}
	if err := s.readIndex(ctx, g); err != nil {
		return nil, err
	}
	v, found, err := g.sm.Get(req.Key)
	if err == ErrMoving {
		return nil, errMoving
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.GetResponse{Found: found, Value: v}, nil
}

// Admin runs a shard management command through the group's log.
func (s *Server) Admin(ctx context.Context, req *pb.AdminRequest) (*pb.AdminResponse, error) {
	g, err := s.group(req.Group)
	if err != nil {
		return nil, err
	}
	switch req.Command.GetOp().(type) {
	case *pb.Command_Freeze, *pb.Command_Import, *pb.Command_Purge:
	default:
		return nil, status.Error(codes.InvalidArgument, "not an admin command")
	}
	res, err := s.propose(ctx, g, req.Command)
	if err != nil {
		return nil, err
	}
	return &pb.AdminResponse{Result: res}, nil
}

func (s *Server) Scan(ctx context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	g, err := s.group(req.Group)
	if err != nil {
		return nil, err
	}
	if req.Mode == pb.ReadMode_LOG {
		cmd := &pb.Command{Op: &pb.Command_Scan{Scan: &pb.ScanOp{Start: req.Start, End: req.End, Limit: req.Limit}}}
		setSession(cmd, req.Session)
		res, err := s.propose(ctx, g, cmd)
		if err != nil {
			return nil, err
		}
		return &pb.ScanResponse{Kvs: res.Kvs}, nil
	}
	if err := s.readIndex(ctx, g); err != nil {
		return nil, err
	}
	kvs, err := g.sm.Scan(req.Start, req.End, int(req.Limit))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.ScanResponse{Kvs: kvs}, nil
}

func setSession(cmd *pb.Command, sess *pb.Session) {
	if sess != nil {
		cmd.ClientId, cmd.Seq = sess.ClientId, sess.Seq
	}
}
