package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// Nodes persist to disk and are killed and restarted at random while
// clients keep proposing. Every proposal that was acknowledged must be
// applied, in order, on every node once the cluster settles.
func TestCommittedEntriesSurviveRandomCrashRestarts(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	c := newClusterWith(t, 3, true)
	defer c.stop()
	rng := rand.New(rand.NewSource(c.seed))
	var acked [][]byte
	next := 0
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		l := c.leader()
		if l == nil {
			time.Sleep(time.Millisecond)
			continue
		}
		var batch []*Proposal
		for i := 0; i < 5; i++ {
			batch = append(batch, l.Propose([]byte(fmt.Sprintf("op%d", next))))
			next++
		}
		for _, p := range batch {
			select {
			case <-p.Done():
				if p.Err == nil {
					acked = append(acked, p.data)
				}
			case <-time.After(2 * electionTimeout()):
			}
		}
		if rng.Intn(4) == 0 {
			victim := c.ids[rng.Intn(len(c.ids))]
			if _, alive := c.nodes[victim]; alive {
				c.kill(victim)
				time.Sleep(time.Duration(rng.Intn(20)) * time.Millisecond)
				c.start(victim)
			}
		}
	}
	for _, id := range c.ids {
		if _, alive := c.nodes[id]; !alive {
			c.start(id)
		}
	}
	if len(acked) == 0 {
		t.Fatalf("nothing was acknowledged (seed %d)", c.seed)
	}
	for _, id := range c.ids {
		got := c.waitApplied(id, len(acked), 10*time.Second)
		gi := 0
		for _, want := range acked {
			for gi < len(got) && !bytes.Equal(got[gi].Data, want) {
				gi++
			}
			if gi == len(got) {
				t.Fatalf("node %d lost acknowledged entry %q (seed %d)", id, want, c.seed)
			}
			gi++
		}
	}
	t.Logf("%d acknowledged of %d proposed, seed %d", len(acked), next, c.seed)
}
