package storage

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// The crash test runs a workload in a child process that acknowledges each
// write on stdout only after Put or Delete has returned. The parent kills
// the child with SIGKILL at a random point, reopens the same directory and
// checks that every acknowledged write survived. The workload is a pure
// function of (seed, op index), so parent and child agree on it without
// sharing state. One directory is reused across iterations so flushes and
// compactions accumulate real history.

const crashChildEnv = "KEYSTONE_CRASH_CHILD"

type crashOp struct {
	key    []byte
	value  []byte
	delete bool
}

func crashWorkloadOp(seed int64, i int) crashOp {
	rng := rand.New(rand.NewSource(seed*1_000_003 + int64(i)))
	op := crashOp{key: []byte(fmt.Sprintf("k%04d", rng.Intn(1000)))}
	if rng.Intn(10) == 0 {
		op.delete = true
		return op
	}
	op.value = make([]byte, 50+rng.Intn(400))
	rng.Read(op.value)
	copy(op.value, fmt.Sprintf("op%d-", i))
	return op
}

func crashOptions() Options {
	return Options{MemtableSize: 64 << 10, BlockSize: 1024, Sync: SyncAlways}
}

func TestCrashChild(t *testing.T) {
	if os.Getenv(crashChildEnv) == "" {
		t.Skip("child process entry point")
	}
	dir := os.Getenv("KEYSTONE_CRASH_DIR")
	seed, _ := strconv.ParseInt(os.Getenv("KEYSTONE_CRASH_SEED"), 10, 64)
	start, _ := strconv.Atoi(os.Getenv("KEYSTONE_CRASH_START"))
	d, err := Open(dir, crashOptions())
	if err != nil {
		fmt.Fprintf(os.Stderr, "child open: %v\n", err)
		os.Exit(2)
	}
	for i := start; ; i++ {
		op := crashWorkloadOp(seed, i)
		if op.delete {
			err = d.Delete(op.key)
		} else {
			err = d.Put(op.key, op.value)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "child op %d: %v\n", i, err)
			os.Exit(2)
		}
		fmt.Fprintf(os.Stdout, "%d\n", i)
	}
}

func TestCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("crash harness is slow")
	}
	iterations := 200
	if s := os.Getenv("KEYSTONE_CRASH_ITERS"); s != "" {
		iterations, _ = strconv.Atoi(s)
	}
	seed := time.Now().UnixNano()
	if s := os.Getenv("KEYSTONE_CRASH_SEED"); s != "" {
		seed, _ = strconv.ParseInt(s, 10, 64)
	}
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewSource(seed))
	dir := t.TempDir()
	oracle := map[string][]byte{}
	next := 0

	for iter := 0; iter < iterations; iter++ {
		acked, inflight := runCrashChild(t, dir, seed, next, rng)
		if acked < next {
			t.Fatalf("seed %d iteration %d: child regressed to %d before %d", seed, iter, acked, next)
		}
		for i := next; i < acked; i++ {
			applyCrashOp(oracle, crashWorkloadOp(seed, i))
		}
		next = acked

		d, err := Open(dir, crashOptions())
		if err != nil {
			t.Fatalf("seed %d iteration %d: reopen after kill: %v", seed, iter, err)
		}
		for k, want := range oracle {
			got, err := d.Get([]byte(k))
			if inflight != nil && bytes.Equal(inflight.key, []byte(k)) {
				continue
			}
			if err != nil || !bytes.Equal(got, want) {
				d.Close()
				t.Fatalf("seed %d iteration %d: acknowledged %q = %q, %v; want %q", seed, iter, k, got, err, want)
			}
		}
		if inflight != nil {
			landed := inflightLanded(d, oracle, inflight)
			if landed {
				applyCrashOp(oracle, *inflight)
				next++
			}
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("%d iterations, %d ops, %d live keys", iterations, next, len(oracle))
}

func applyCrashOp(oracle map[string][]byte, op crashOp) {
	if op.delete {
		delete(oracle, string(op.key))
	} else {
		oracle[string(op.key)] = op.value
	}
}

// inflightLanded decides whether the one unacknowledged op reached disk,
// which is allowed either way.
func inflightLanded(d DB, oracle map[string][]byte, op *crashOp) bool {
	got, err := d.Get(op.key)
	prev, had := oracle[string(op.key)]
	if op.delete {
		return err == ErrNotFound && had
	}
	if err == nil && bytes.Equal(got, op.value) {
		return true
	}
	if had && err == nil && bytes.Equal(got, prev) || !had && err == ErrNotFound {
		return false
	}
	panic(fmt.Sprintf("key %q holds %q (%v), neither previous %q nor in-flight value", op.key, got, err, prev))
}

// runCrashChild returns the index one past the last acknowledged op and
// the op that may have been in flight when the child died.
func runCrashChild(t *testing.T, dir string, seed int64, start int, rng *rand.Rand) (int, *crashOp) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$")
	cmd.Env = append(os.Environ(),
		crashChildEnv+"=1",
		"KEYSTONE_CRASH_DIR="+dir,
		"KEYSTONE_CRASH_SEED="+strconv.FormatInt(seed, 10),
		"KEYSTONE_CRASH_START="+strconv.Itoa(start),
	)
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	killAfter := rng.Intn(150)
	acks := make(chan int, 1024)
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			n, err := strconv.Atoi(sc.Text())
			if err != nil {
				break
			}
			acks <- n
		}
		close(acks)
	}()
	last := start - 1
	timer := time.After(time.Duration(rng.Intn(200)) * time.Millisecond)
	count := 0
loop:
	for {
		select {
		case n, ok := <-acks:
			if !ok {
				break loop
			}
			last = n
			count++
			if count >= killAfter && rng.Intn(2) == 0 {
				break loop
			}
		case <-timer:
			break loop
		}
	}
	cmd.Process.Signal(syscall.SIGKILL)
	// Acks already in the pipe when the child died still count.
	for n := range acks {
		last = n
	}
	cmd.Wait()
	op := crashWorkloadOp(seed, last+1)
	return last + 1, &op
}
