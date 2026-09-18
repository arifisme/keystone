package sim

import (
	"flag"
	"fmt"
	"math/rand"
	"testing"
)

var (
	seeds = flag.Int("seeds", 500, "number of seeds to run")
	seed  = flag.Int64("seed", 0, "replay a single seed")
	chaos = flag.Int("chaos", 2, "chaos level 0-3")
)

func runSeed(t *testing.T, seed int64, chaos int) Stats {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	return runConfig(t, Config{Seed: seed, Chaos: chaos, Nodes: 3 + 2*rng.Intn(2)})
}

func runConfig(t *testing.T, cfg Config) Stats {
	t.Helper()
	var stats Stats
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("%v", r)
			}
		}()
		stats, err = New(cfg).Run()
	}()
	if err != nil {
		t.Fatal(err)
	}
	return stats
}

func TestSim(t *testing.T) {
	if *seed != 0 {
		st := runSeed(t, *seed, *chaos)
		t.Logf("seed %d: %+v", *seed, st)
		return
	}
	for i := 1; i <= *seeds; i++ {
		i := i
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			runSeed(t, int64(i), *chaos)
		})
	}
}

// Three groups of three: the meta group issues sessions and two shard
// groups own the key space, so every client alternates between groups and
// sessions must carry across them.
func TestSimMultiShard(t *testing.T) {
	if *seed != 0 {
		st := runConfig(t, Config{Seed: *seed, Chaos: *chaos, Groups: 3, Nodes: 3, Keys: 8})
		t.Logf("seed %d: %+v", *seed, st)
		return
	}
	for i := 1; i <= *seeds/5; i++ {
		i := i
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			runConfig(t, Config{Seed: int64(i), Chaos: *chaos, Groups: 3, Nodes: 3, Keys: 8})
		})
	}
}

func TestSimIsDeterministic(t *testing.T) {
	a := runSeed(t, 42, 3)
	b := runSeed(t, 42, 3)
	if a != b {
		t.Fatalf("same seed, different runs:\n%+v\n%+v", a, b)
	}
}
