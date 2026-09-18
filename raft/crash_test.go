package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// Nodes persist to disk and are killed and restarted at random while a
// client keeps proposing against whichever node is leader. Every proposal
// that was acknowledged must be applied, in order, on every node once the
// cluster settles.
func TestCommittedEntriesSurviveRandomCrashRestarts(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	c := newClusterWith(t, 3, true)
	defer c.stop()
	rng := rand.New(rand.NewSource(c.seed))

	var mu sync.Mutex
	var acked [][]byte
	proposed := 0
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			mu.Lock()
			l := c.leader()
			mu.Unlock()
			if l == nil {
				time.Sleep(time.Millisecond)
				continue
			}
			p := l.Propose([]byte(fmt.Sprintf("op%d", i)))
			select {
			case <-p.Done():
				if p.Err == nil {
					acked = append(acked, p.data)
				}
			case <-time.After(2 * electionTimeout()):
			}
			proposed++
		}
	}()

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(time.Duration(10+rng.Intn(40)) * time.Millisecond)
		victim := c.ids[rng.Intn(len(c.ids))]
		mu.Lock()
		if _, alive := c.nodes[victim]; alive {
			c.kill(victim)
		}
		mu.Unlock()
		time.Sleep(time.Duration(rng.Intn(30)) * time.Millisecond)
		mu.Lock()
		c.start(victim)
		mu.Unlock()
	}
	close(stop)
	wg.Wait()

	if len(acked) == 0 {
		t.Fatalf("nothing was acknowledged (seed %d)", c.seed)
	}
	c.waitSettled(15 * time.Second)
	for _, id := range c.ids {
		got := c.sms[id].entries()
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
	t.Logf("%d acknowledged of %d proposed, seed %d", len(acked), proposed, c.seed)
}
