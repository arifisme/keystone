package kv

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/arifisme/keystone/proto"
	"github.com/arifisme/keystone/raft"
)

// Server exposes one Raft group's state machine over gRPC. Writes and log
// reads go through the log; ReadIndex reads wait for a leadership check
// and then read the state machine directly.
type Server struct {
	pb.UnimplementedKVServer
	node  *raft.Node
	sm    *StateMachine
	peers map[raft.NodeID]string
}

func NewServer(node *raft.Node, sm *StateMachine, peers map[raft.NodeID]string) *Server {
	return &Server{node: node, sm: sm, peers: peers}
}

func (s *Server) Register(g *grpc.Server) {
	pb.RegisterKVServer(g, s)
}

func (s *Server) propose(ctx context.Context, cmd *pb.Command) (*pb.Result, error) {
	data, err := proto.Marshal(cmd)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	p := s.node.Propose(data)
	select {
	case <-p.Done():
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if p.Err != nil {
		return nil, s.notLeader(p.Err)
	}
	var res pb.Result
	if err := proto.Unmarshal(p.Result, &res); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &res, nil
}

// readIndex blocks until a read may be served from local state.
func (s *Server) readIndex(ctx context.Context) error {
	rd := s.node.ReadIndex()
	select {
	case <-rd.Done():
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
	if rd.Err != nil {
		return s.notLeader(rd.Err)
	}
	return nil
}

func (s *Server) notLeader(err error) error {
	st := status.New(codes.FailedPrecondition, err.Error())
	lead := s.node.Status().Leader
	if detailed, derr := st.WithDetails(&pb.NotLeader{LeaderId: uint64(lead), LeaderAddr: s.peers[lead]}); derr == nil {
		st = detailed
	}
	return st.Err()
}

func (s *Server) RegisterClient(ctx context.Context, _ *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	res, err := s.propose(ctx, &pb.Command{Op: &pb.Command_Register{Register: &pb.RegisterOp{}}})
	if err != nil {
		return nil, err
	}
	return &pb.RegisterResponse{ClientId: res.ClientId}, nil
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	cmd := &pb.Command{Op: &pb.Command_Put{Put: &pb.PutOp{Key: req.Key, Value: req.Value}}}
	setSession(cmd, req.Session)
	if _, err := s.propose(ctx, cmd); err != nil {
		return nil, err
	}
	return &pb.PutResponse{}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	cmd := &pb.Command{Op: &pb.Command_Delete{Delete: &pb.DeleteOp{Key: req.Key}}}
	setSession(cmd, req.Session)
	if _, err := s.propose(ctx, cmd); err != nil {
		return nil, err
	}
	return &pb.DeleteResponse{}, nil
}

func (s *Server) Cas(ctx context.Context, req *pb.CasRequest) (*pb.CasResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	cmd := &pb.Command{Op: &pb.Command_Cas{Cas: &pb.CasOp{Key: req.Key, Expected: req.Expected, Value: req.Value}}}
	setSession(cmd, req.Session)
	res, err := s.propose(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return &pb.CasResponse{Success: res.Success, Found: res.Found, Current: res.Value}, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	if req.Mode == pb.ReadMode_LOG {
		cmd := &pb.Command{Op: &pb.Command_Get{Get: &pb.GetOp{Key: req.Key}}}
		setSession(cmd, req.Session)
		res, err := s.propose(ctx, cmd)
		if err != nil {
			return nil, err
		}
		return &pb.GetResponse{Found: res.Found, Value: res.Value}, nil
	}
	if err := s.readIndex(ctx); err != nil {
		return nil, err
	}
	v, found, err := s.sm.Get(req.Key)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.GetResponse{Found: found, Value: v}, nil
}

func (s *Server) Scan(ctx context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	if req.Mode == pb.ReadMode_LOG {
		cmd := &pb.Command{Op: &pb.Command_Scan{Scan: &pb.ScanOp{Start: req.Start, End: req.End, Limit: req.Limit}}}
		setSession(cmd, req.Session)
		res, err := s.propose(ctx, cmd)
		if err != nil {
			return nil, err
		}
		return &pb.ScanResponse{Kvs: res.Kvs}, nil
	}
	if err := s.readIndex(ctx); err != nil {
		return nil, err
	}
	kvs, err := s.sm.Scan(req.Start, req.End, int(req.Limit))
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
