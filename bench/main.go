// Command bench measures a Keystone cluster started inside the process.
//
//	bench run -nodes 5 -op put -value 1024 -clients 32 -duration 10s -out results/put.json
//	bench failover -nodes 5 -out results/failover.json
//	bench all -out results          # the matrix behind the README
//	bench chart -in results -out results
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"sync"
	"time"

	"github.com/arifisme/keystone/proto"
)

type Result struct {
	Name       string        `json:"name"`
	Nodes      int           `json:"nodes"`
	Op         string        `json:"op"`
	Read       string        `json:"read,omitempty"`
	ValueSize  int           `json:"value_size"`
	Clients    int           `json:"clients"`
	Batched    bool          `json:"batched"`
	Duration   time.Duration `json:"duration"`
	Ops        int           `json:"ops"`
	Errors     int           `json:"errors"`
	Throughput float64       `json:"throughput"`
	P50        time.Duration `json:"p50"`
	P99        time.Duration `json:"p99"`
	P999       time.Duration `json:"p999"`
}

type Failover struct {
	Nodes        int             `json:"nodes"`
	Clients      int             `json:"clients"`
	KilledAt     time.Duration   `json:"killed_at"`
	NewLeaderAt  time.Duration   `json:"new_leader_at"`
	Unavailable  time.Duration   `json:"unavailable"`
	Errors       int             `json:"errors"`
	Ops          int             `json:"ops"`
	Timeline     []int           `json:"timeline"`
	Bucket       time.Duration   `json:"bucket"`
	ElectionTick time.Duration   `json:"election_timeout"`
	Successes    []time.Duration `json:"-"`
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "run":
		cmdRun(os.Args[2:])
	case "failover":
		cmdFailover(os.Args[2:])
	case "all":
		cmdAll(os.Args[2:])
	case "chart":
		cmdChart(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bench run|failover|all|chart [flags]")
	os.Exit(2)
}

type runOptions struct {
	nodes    int
	op       string
	read     string
	value    int
	clients  int
	duration time.Duration
	batched  bool
	dir      string
}

func (o runOptions) name() string {
	batch := "batched"
	if !o.batched {
		batch = "unbatched"
	}
	if o.op == "get" {
		return fmt.Sprintf("get-%s-n%d-v%d", o.read, o.nodes, o.value)
	}
	return fmt.Sprintf("put-%s-n%d-v%d", batch, o.nodes, o.value)
}

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	o := runOptions{}
	fs.IntVar(&o.nodes, "nodes", 3, "cluster size")
	fs.StringVar(&o.op, "op", "put", "put or get")
	fs.StringVar(&o.read, "read", "readindex", "get: readindex or log")
	fs.IntVar(&o.value, "value", 1024, "value size in bytes")
	fs.IntVar(&o.clients, "clients", 64, "concurrent clients")
	fs.DurationVar(&o.duration, "duration", 10*time.Second, "measurement length")
	fs.BoolVar(&o.batched, "batched", true, "let concurrent proposals share a log append")
	fs.StringVar(&o.dir, "dir", "", "data directory (default: temporary)")
	out := fs.String("out", "", "write JSON result here")
	cpuprofile := fs.String("cpuprofile", "", "write a CPU profile here")
	fs.Parse(args)
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fail(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	res := runWorkload(o)
	report(res, *out)
}

func report(v interface{}, out string) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
	if out != "" {
		if err := os.WriteFile(out, b, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func runWorkload(o runOptions) Result {
	c := startCluster(o.nodes, o.batched, o.dir)
	defer c.stop()
	c.waitLeader()
	value := make([]byte, o.value)
	rand.New(rand.NewSource(1)).Read(value)
	const keys = 100000
	if o.op == "get" {
		cl := c.dial()
		for i := 0; i < 10000; i++ {
			if err := cl.Put(context.Background(), benchKey(i), value); err != nil {
				fmt.Fprintln(os.Stderr, "preload:", err)
				os.Exit(1)
			}
		}
		cl.Close()
	}
	mode := pb.ReadMode_READ_INDEX
	if o.read == "log" {
		mode = pb.ReadMode_LOG
	}

	var wg sync.WaitGroup
	samples := make([][]time.Duration, o.clients)
	errs := make([]int, o.clients)
	stop := time.Now().Add(o.duration)
	start := time.Now()
	for i := 0; i < o.clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cl := c.dial()
			defer cl.Close()
			rng := rand.New(rand.NewSource(int64(i)))
			ctx := context.Background()
			for time.Now().Before(stop) {
				k := rng.Intn(keys)
				if o.op == "get" {
					k = rng.Intn(10000)
				}
				t := time.Now()
				var err error
				if o.op == "get" {
					_, _, err = cl.Get(ctx, benchKey(k), mode)
				} else {
					err = cl.Put(ctx, benchKey(k), value)
				}
				if err != nil {
					errs[i]++
					continue
				}
				samples[i] = append(samples[i], time.Since(t))
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	var all []time.Duration
	total := 0
	for i := range samples {
		all = append(all, samples[i]...)
		total += errs[i]
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	res := Result{Name: o.name(), Nodes: o.nodes, Op: o.op, ValueSize: o.value, Clients: o.clients, Batched: o.batched, Duration: elapsed, Ops: len(all), Errors: total}
	if o.op == "get" {
		res.Read = o.read
	}
	if len(all) > 0 {
		res.Throughput = float64(len(all)) / elapsed.Seconds()
		res.P50 = all[len(all)/2]
		res.P99 = all[len(all)*99/100]
		res.P999 = all[len(all)*999/1000]
	}
	return res
}

func benchKey(i int) []byte {
	return []byte(fmt.Sprintf("key%08d", i))
}

func cmdFailover(args []string) {
	fs := flag.NewFlagSet("failover", flag.ExitOnError)
	nodes := fs.Int("nodes", 5, "cluster size")
	clients := fs.Int("clients", 64, "concurrent clients")
	duration := fs.Duration("duration", 15*time.Second, "total run")
	killAt := fs.Duration("kill", 5*time.Second, "when to kill the leader")
	dir := fs.String("dir", "", "data directory (default: temporary)")
	out := fs.String("out", "", "write JSON result here")
	fs.Parse(args)
	report(runFailover(*nodes, *clients, *duration, *killAt, *dir), *out)
}

// runFailover keeps clients writing, kills the leader at killAt, and
// reports the longest stretch without a successful write between the
// kill and the new leader settling in.
func runFailover(nodes, clients int, duration, killAt time.Duration, dir string) Failover {
	c := startCluster(nodes, true, dir)
	defer c.stop()
	c.waitLeader()
	value := make([]byte, 1024)
	var mu sync.Mutex
	var successes []time.Duration
	errors := 0
	start := time.Now()
	stop := start.Add(duration)
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cl := c.dial()
			defer cl.Close()
			ctx := context.Background()
			rng := rand.New(rand.NewSource(int64(i)))
			for time.Now().Before(stop) {
				err := cl.Put(ctx, benchKey(rng.Intn(100000)), value)
				mu.Lock()
				if err != nil {
					errors++
				} else {
					successes = append(successes, time.Since(start))
				}
				mu.Unlock()
			}
		}(i)
	}
	time.Sleep(killAt)
	old := c.leader()
	c.kill(old)
	killed := time.Since(start)
	var newLeader time.Duration
	for {
		if l := c.leader(); l != 0 {
			newLeader = time.Since(start)
			break
		}
		time.Sleep(time.Millisecond)
	}
	wg.Wait()

	sort.Slice(successes, func(i, j int) bool { return successes[i] < successes[j] })
	f := Failover{Nodes: nodes, Clients: clients, KilledAt: killed, NewLeaderAt: newLeader, Errors: errors, Ops: len(successes), Bucket: 100 * time.Millisecond, ElectionTick: c.electionTimeout()}
	// The gap opens before Stop returns, since the old leader finishes
	// what it had in flight, so look for the longest gap in a window
	// around the kill rather than at the timestamp itself.
	for i := 1; i < len(successes); i++ {
		if successes[i] < killed-time.Second || successes[i-1] > newLeader+2*time.Second {
			continue
		}
		if gap := successes[i] - successes[i-1]; gap > f.Unavailable {
			f.Unavailable = gap
		}
	}
	buckets := int(duration/f.Bucket) + 1
	f.Timeline = make([]int, buckets)
	for _, t := range successes {
		f.Timeline[int(t/f.Bucket)]++
	}
	return f
}

func cmdAll(args []string) {
	fs := flag.NewFlagSet("all", flag.ExitOnError)
	out := fs.String("out", "bench/results", "results directory")
	duration := fs.Duration("duration", 10*time.Second, "per run")
	clients := fs.Int("clients", 64, "concurrent clients")
	dir := fs.String("dir", "", "data directory (default: temporary)")
	fs.Parse(args)
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var runs []runOptions
	for _, nodes := range []int{3, 5} {
		for _, value := range []int{100, 1024, 10240} {
			runs = append(runs, runOptions{nodes: nodes, op: "put", value: value, batched: true})
		}
		runs = append(runs, runOptions{nodes: nodes, op: "put", value: 1024, batched: false})
		for _, read := range []string{"readindex", "log"} {
			runs = append(runs, runOptions{nodes: nodes, op: "get", read: read, value: 1024, batched: true})
		}
	}
	for _, o := range runs {
		o.clients, o.duration, o.dir = *clients, *duration, *dir
		fmt.Fprintf(os.Stderr, "== %s\n", o.name())
		res := runWorkload(o)
		report(res, filepath.Join(*out, o.name()+".json"))
	}
	fmt.Fprintln(os.Stderr, "== failover")
	report(runFailover(5, *clients, 15*time.Second, 5*time.Second, *dir), filepath.Join(*out, "failover.json"))
}
