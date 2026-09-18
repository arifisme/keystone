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
	cfg := Config{Seed: seed, Chaos: chaos, Nodes: 3 + 2*rng.Intn(2)}
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

func TestSimIsDeterministic(t *testing.T) {
	a := runSeed(t, 42, 3)
	b := runSeed(t, 42, 3)
	if a != b {
		t.Fatalf("same seed, different runs:\n%+v\n%+v", a, b)
	}
}
