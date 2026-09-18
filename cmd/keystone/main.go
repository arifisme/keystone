// Command keystone runs one node of a Keystone cluster.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/arifisme/keystone/kv"
	"github.com/arifisme/keystone/raft"
	"github.com/arifisme/keystone/shard"
)

func main() {
	id := flag.Uint64("id", 0, "node id, must appear in -peers")
	peers := flag.String("peers", "", "comma-separated id=host:port for every node")
	groups := flag.String("groups", "", "raft groups as group:id,id,...;group:... (default: one group of every peer); group 1 is the meta group when sharded")
	dir := flag.String("data", "", "data directory")
	addr := flag.String("addr", "", "listen address (default: this node's -peers entry)")
	metrics := flag.String("metrics", "", "address for /metrics and /debug/pprof (off when empty)")
	flag.Parse()

	peerMap, err := parsePeers(*peers)
	if err != nil {
		log.Fatal(err)
	}
	if *id == 0 || peerMap[raft.NodeID(*id)] == "" {
		log.Fatal("-id must name an entry in -peers")
	}
	if *dir == "" {
		log.Fatal("-data is required")
	}
	if *addr == "" {
		*addr = peerMap[raft.NodeID(*id)]
	}
	groupMap, err := parseGroups(*groups, peerMap)
	if err != nil {
		log.Fatal(err)
	}
	n, err := kv.Open(kv.NodeOptions{ID: raft.NodeID(*id), Peers: peerMap, Groups: groupMap, Dir: *dir, Addr: *addr, MetricsAddr: *metrics})
	if err != nil {
		log.Fatal(err)
	}
	var router *shard.Router
	if len(groupMap) > 1 {
		router, err = shard.NewRouter(n, groupMap)
		if err != nil {
			log.Fatal(err)
		}
		n.Serve(router)
	} else {
		n.Serve(n.Server())
	}
	log.Printf("node %d listening on %s, groups %v", *id, *addr, n.Groups())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	if router != nil {
		router.Close()
	}
	if err := n.Stop(); err != nil {
		log.Fatal(err)
	}
}

func parseGroups(s string, peers map[raft.NodeID]string) (map[uint64][]raft.NodeID, error) {
	out := map[uint64][]raft.NodeID{}
	if s == "" {
		for id := range peers {
			out[1] = append(out[1], id)
		}
		return out, nil
	}
	for _, part := range strings.Split(s, ";") {
		gStr, members, ok := strings.Cut(part, ":")
		if !ok {
			return nil, fmt.Errorf("group %q is not group:id,id", part)
		}
		g, err := strconv.ParseUint(gStr, 10, 64)
		if err != nil || g == 0 {
			return nil, fmt.Errorf("group %q has a bad id", part)
		}
		for _, m := range strings.Split(members, ",") {
			id, err := strconv.ParseUint(m, 10, 64)
			if err != nil || peers[raft.NodeID(id)] == "" {
				return nil, fmt.Errorf("group %d member %q is not in -peers", g, m)
			}
			out[g] = append(out[g], raft.NodeID(id))
		}
	}
	return out, nil
}

func parsePeers(s string) (map[raft.NodeID]string, error) {
	out := map[raft.NodeID]string{}
	for _, part := range strings.Split(s, ",") {
		if part == "" {
			continue
		}
		idStr, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("peer %q is not id=addr", part)
		}
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("peer %q has a bad id", part)
		}
		out[raft.NodeID(id)] = addr
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("-peers is required")
	}
	return out, nil
}
