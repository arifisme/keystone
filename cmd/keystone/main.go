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
)

func main() {
	id := flag.Uint64("id", 0, "node id, must appear in -peers")
	peers := flag.String("peers", "", "comma-separated id=host:port for every node")
	dir := flag.String("data", "", "data directory")
	addr := flag.String("addr", "", "listen address (default: this node's -peers entry)")
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
	var members []raft.NodeID
	for id := range peerMap {
		members = append(members, id)
	}
	n, err := kv.Open(kv.NodeOptions{ID: raft.NodeID(*id), Peers: peerMap, Groups: map[uint64][]raft.NodeID{1: members}, Dir: *dir, Addr: *addr})
	if err != nil {
		log.Fatal(err)
	}
	n.Serve(n.Server())
	log.Printf("node %d listening on %s", *id, *addr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	if err := n.Stop(); err != nil {
		log.Fatal(err)
	}
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
