package kv

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/arifisme/keystone/proto"
)

type stubKV struct {
	pb.UnimplementedKVServer
	put func() error
}

func (s *stubKV) Put(context.Context, *pb.PutRequest) (*pb.PutResponse, error) {
	return &pb.PutResponse{}, s.put()
}

func serveStub(t *testing.T, put func() error) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterKVServer(srv, &stubKV{put: put})
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func TestClientLeavesANodeThatKnowsNoLeader(t *testing.T) {
	lost := serveStub(t, func() error {
		st, _ := status.New(codes.FailedPrecondition, "not leader").WithDetails(&pb.NotLeader{})
		return st.Err()
	})
	leader := serveStub(t, func() error { return nil })
	cl, err := NewClient([]string{lost, leader}, ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cl.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("put = %v, want the client to move on to the second endpoint", err)
	}
}
