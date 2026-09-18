package kv

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/arifisme/keystone/proto"
)

// Client talks to any node and follows leader hints. Every write carries
// the session's next sequence number, and a retry reuses it, so a command
// that was applied but whose reply was lost is not applied again.
type Client struct {
	endpoints []string
	attempt   time.Duration
	group     uint64

	mu       sync.Mutex
	conns    map[string]*grpc.ClientConn
	leader   string
	next     int
	clientID uint64
	seq      uint64
}

type ClientOptions struct {
	// AttemptTimeout bounds a single RPC before the client tries elsewhere.
	AttemptTimeout time.Duration
	// Group pins every request to one Raft group. Zero lets the node
	// route by key.
	Group uint64
}

var ErrNoEndpoints = errors.New("kv: no endpoints")

// NewClient prepares a client with no session. Only Do is usable on it;
// routers forward requests that carry the caller's session this way.
func NewClient(endpoints []string, opts ClientOptions) (*Client, error) {
	if len(endpoints) == 0 {
		return nil, ErrNoEndpoints
	}
	if opts.AttemptTimeout <= 0 {
		opts.AttemptTimeout = 2 * time.Second
	}
	return &Client{endpoints: endpoints, attempt: opts.AttemptTimeout, group: opts.Group, conns: map[string]*grpc.ClientConn{}}, nil
}

// Dial registers a session with the cluster. It retries until ctx ends.
func Dial(ctx context.Context, endpoints []string, opts ClientOptions) (*Client, error) {
	c, err := NewClient(endpoints, opts)
	if err != nil {
		return nil, err
	}
	var id uint64
	err = c.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		res, err := cli.RegisterClient(ctx, &pb.RegisterRequest{Group: c.group})
		if err != nil {
			return err
		}
		id = res.ClientId
		return nil
	})
	if err != nil {
		c.Close()
		return nil, err
	}
	c.clientID = id
	return c, nil
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		conn.Close()
	}
	c.conns = map[string]*grpc.ClientConn{}
}

func (c *Client) session() *pb.Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	return &pb.Session{ClientId: c.clientID, Seq: c.seq}
}

func (c *Client) Put(ctx context.Context, key, value []byte) error {
	sess := c.session()
	return c.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		_, err := cli.Put(ctx, &pb.PutRequest{Session: sess, Key: key, Value: value, Group: c.group})
		return err
	})
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
	sess := c.session()
	return c.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		_, err := cli.Delete(ctx, &pb.DeleteRequest{Session: sess, Key: key, Group: c.group})
		return err
	})
}

// Cas sets key to value if its current value equals expected; a nil
// expected requires the key to be absent. It reports whether the swap
// happened and the value found.
func (c *Client) Cas(ctx context.Context, key, expected, value []byte) (swapped bool, current []byte, found bool, err error) {
	sess := c.session()
	err = c.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		res, err := cli.Cas(ctx, &pb.CasRequest{Session: sess, Key: key, Expected: expected, Value: value, Group: c.group})
		if err != nil {
			return err
		}
		swapped, current, found = res.Success, res.Current, res.Found
		return nil
	})
	return
}

func (c *Client) Get(ctx context.Context, key []byte, mode pb.ReadMode) (value []byte, found bool, err error) {
	sess := c.session()
	err = c.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		res, err := cli.Get(ctx, &pb.GetRequest{Session: sess, Key: key, Mode: mode, Group: c.group})
		if err != nil {
			return err
		}
		value, found = res.Value, res.Found
		return nil
	})
	return
}

func (c *Client) Scan(ctx context.Context, start, end []byte, limit int, mode pb.ReadMode) (kvs []*pb.KeyValue, err error) {
	sess := c.session()
	err = c.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
		res, err := cli.Scan(ctx, &pb.ScanRequest{Session: sess, Start: start, End: end, Limit: uint32(limit), Mode: mode, Group: c.group})
		if err != nil {
			return err
		}
		kvs = res.Kvs
		return nil
	})
	return
}

// Do runs op against the presumed leader, switching on NotLeader hints
// and connection failures with capped exponential backoff. Routers use it
// to forward requests they built themselves.
func (c *Client) Do(ctx context.Context, op func(context.Context, pb.KVClient) error) error {
	backoff := 20 * time.Millisecond
	for {
		addr := c.target()
		cli, err := c.client(addr)
		if err == nil {
			actx, cancel := context.WithTimeout(ctx, c.attempt)
			err = op(actx, cli)
			cancel()
			if err == nil {
				return nil
			}
		}
		hinted := c.handleFailure(addr, err)
		if hinted == errFatal {
			return err
		}
		if ctx.Err() != nil {
			return err
		}
		delay := backoff
		if hinted == nil {
			delay = time.Duration(rand.Int63n(int64(backoff)/2 + 1))
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(delay):
		}
		if backoff *= 2; backoff > time.Second {
			backoff = time.Second
		}
	}
}

var errFatal = errors.New("fatal")

// handleFailure updates the leader guess. It returns nil when a hint
// named the leader, errFatal when the error is not worth retrying.
func (c *Client) handleFailure(addr string, err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := status.FromError(err)
	if !ok {
		c.leader = ""
		return err
	}
	switch st.Code() {
	case codes.FailedPrecondition:
		for _, d := range st.Details() {
			if nl, ok := d.(*pb.NotLeader); ok && nl.LeaderAddr != "" && nl.LeaderAddr != addr {
				c.leader = nl.LeaderAddr
				return nil
			}
		}
		c.leader = ""
		return err
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		c.leader = ""
		c.next++
		return err
	}
	return errFatal
}

func (c *Client) target() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != "" {
		return c.leader
	}
	return c.endpoints[c.next%len(c.endpoints)]
}

func (c *Client) client(addr string) (pb.KVClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	conn, ok := c.conns[addr]
	if !ok {
		var err error
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		c.conns[addr] = conn
	}
	return pb.NewKVClient(conn), nil
}
