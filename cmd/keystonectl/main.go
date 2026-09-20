// Command keystonectl is a command-line client for a Keystone cluster.
//
//	keystonectl [-endpoints a,b,c] [-mode readindex|log] get KEY
//	keystonectl put KEY VALUE
//	keystonectl delete KEY
//	keystonectl cas KEY EXPECTED VALUE      (EXPECTED "-" means absent)
//	keystonectl [-limit N] scan [START [END]]
//	keystonectl shards                       (sharded clusters: print the shard map)
//	keystonectl move SHARD GROUP             (sharded clusters: move a shard)
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/arifisme/keystone/kv"
	"github.com/arifisme/keystone/proto"
)

func main() {
	endpoints := flag.String("endpoints", "127.0.0.1:7001", "comma-separated node addresses")
	mode := flag.String("mode", "readindex", "read mode: readindex or log")
	limit := flag.Int("limit", 0, "scan: maximum keys (0 = all)")
	timeout := flag.Duration("timeout", 10*time.Second, "overall deadline")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
	}

	readMode := pb.ReadMode_READ_INDEX
	switch *mode {
	case "readindex":
	case "log":
		readMode = pb.ReadMode_LOG
	default:
		fail("unknown -mode %q", *mode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	c, err := kv.Dial(ctx, strings.Split(*endpoints, ","), kv.ClientOptions{})
	if err != nil {
		fail("connect: %v", err)
	}
	defer c.Close()

	switch cmd, rest := args[0], args[1:]; cmd {
	case "get":
		need(rest, 1)
		v, found, err := c.Get(ctx, []byte(rest[0]), readMode)
		check(err)
		if !found {
			fail("not found")
		}
		fmt.Println(string(v))
	case "put":
		need(rest, 2)
		check(c.Put(ctx, []byte(rest[0]), []byte(rest[1])))
	case "delete":
		need(rest, 1)
		check(c.Delete(ctx, []byte(rest[0])))
	case "cas":
		need(rest, 3)
		var expected []byte
		if rest[1] != "-" {
			expected = []byte(rest[1])
		}
		swapped, current, found, err := c.Cas(ctx, []byte(rest[0]), expected, []byte(rest[2]))
		check(err)
		if !swapped {
			if found {
				fail("cas failed: current value is %q", current)
			}
			fail("cas failed: key absent")
		}
	case "scan":
		var start, end []byte
		if len(rest) > 0 {
			start = []byte(rest[0])
		}
		if len(rest) > 1 {
			end = []byte(rest[1])
		}
		kvs, err := c.Scan(ctx, start, end, *limit, readMode)
		check(err)
		for _, kv := range kvs {
			fmt.Printf("%s\t%s\n", kv.Key, kv.Value)
		}
	case "shards":
		var m *pb.ShardMap
		check(c.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
			var err error
			m, err = cli.GetShardMap(ctx, &pb.ShardMapRequest{})
			return err
		}))
		for i, g := range m.Groups {
			line := fmt.Sprintf("shard %2d  group %d", i, g)
			if i < len(m.Moving) && m.Moving[i] != 0 {
				line += fmt.Sprintf("  moving to %d", m.Moving[i])
			}
			fmt.Println(line)
		}
	case "move":
		need(rest, 2)
		shard, err := strconv.Atoi(rest[0])
		check(err)
		group, err := strconv.ParseUint(rest[1], 10, 64)
		check(err)
		check(c.Do(ctx, func(ctx context.Context, cli pb.KVClient) error {
			_, err := cli.MoveShard(ctx, &pb.MoveShardRequest{Shard: uint32(shard), Group: group})
			return err
		}))
	default:
		usage()
	}
}

func need(args []string, n int) {
	if len(args) != n {
		usage()
	}
}

func check(err error) {
	if err != nil {
		fail("%v", err)
	}
}

func fail(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: keystonectl [flags] get KEY | put KEY VALUE | delete KEY | cas KEY EXPECTED VALUE | scan [START [END]] | shards | move SHARD GROUP")
	os.Exit(2)
}
