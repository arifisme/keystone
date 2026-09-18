# Keystone

A distributed, persistent, linearizable key-value store written from
scratch in Go: Raft consensus on top of a custom LSM-tree storage engine,
with deterministic simulation testing and linearizability checking.

```
                 keystonectl / client library
                              |
                              v gRPC
   +------------------------------------------------------------+
   |  kv        Get / Put / Delete / CAS / Scan                  |
   |            client sessions (exactly-once), ReadIndex        |
   +------------------------------------------------------------+
   |  raft      election, replication, snapshots                 |
   +------------------------------------------------------------+
   |  storage   WAL -> memtable -> SSTables -> compaction        |
   +------------------------------------------------------------+
```

See [DESIGN.md](DESIGN.md) for how it is put together.

## Status

Storage engine, Raft, the key-value service, the deterministic simulator
and sharding across Raft groups are complete. Benchmarks, metrics and
container packaging are next.

## Testing

The interesting tests are the ones that run a whole cluster inside one
goroutine with nothing real underneath it.

**Simulation.** `sim` replaces the network with a priority queue of
messages keyed by simulated delivery time, the clock with a counter that
advances only when the loop pops an event, and the Raft log with an
in-memory journal whose unsynced tail is rolled back when a node crashes.
Every choice comes from one seeded random source, so a seed reproduces a
run exactly, and a failing run prints its seed and the last two hundred
events.

**Fault injection.** Each run draws its own message drop and duplicate
rates, delay range and clock jitter, then schedules symmetric and
asymmetric partitions with random membership and duration, node crashes
with restarts, leader-targeted kills, and torn tails that keep a random
prefix of a step's writes and none of its messages. Snapshots are forced
every forty entries and sent in 128-byte chunks, so most runs install a
few of them across partitions.

**Invariants**, checked after every event: at most one leader per term,
terms never decrease on a node, an index that any node has committed holds
the same entry on every node that commits it, every state machine applies
the same entry at every index, and a restarted node still holds everything
it had reported committed.

**Linearizability.** Three simulated clients issue gets, puts, deletes and
compare-and-swaps over a handful of keys through the real state machine,
half of the reads through ReadIndex and half through the log, retrying
with the same sequence number exactly as the real client does. The
history goes to [Porcupine](https://github.com/anishathalye/porcupine)
with a per-key register model after every run.

`TestSimMultiShard` runs the same machinery over three groups of three
nodes, with the meta group issuing sessions and two groups owning the key
space, so clients cross groups and sessions must carry between them.

```
go test -race ./sim                          # 500 seeds, chaos level 2, what CI runs
go test ./sim -seeds=10000 -chaos=3          # about 40 s on 16 cores
go test ./sim -seed=52 -chaos=2 -v           # replay one seed
```

The nightly workflow runs 50,000 seeds at the highest chaos level and
5,000 iterations of the storage crash harness. [BUGS.md](BUGS.md) lists
what the simulator has found so far.

**Crash injection.** `storage/crash_test.go` runs a workload in a child
process that acknowledges each write only after it returned, kills it
with SIGKILL at a random moment, reopens the directory and checks that
every acknowledged write is there. Two hundred iterations run in CI.

## Storage engine benchmarks

Single process, AMD Ryzen 7 7700X, ext4 on WSL2, Go 1.27. `fsync` means
every `Put` waits for `fsync`; `nosync` syncs on a 10 ms timer.

```
go test -run '^$' -bench 'BenchmarkPut$|BenchmarkPutParallel|BenchmarkGet' -benchtime=2s ./storage
go test -run '^$' -bench BenchmarkRecovery -benchtime=1x ./storage
```

| Workload                 | ops/s   | p50      | p99      |
|--------------------------|--------:|---------:|---------:|
| Put seq 100 B, fsync     | 617     | 1.52 ms  | 3.14 ms  |
| Put seq 100 B, nosync    | 309,518 | 1.4 µs   | 6.2 µs   |
| Put rand 100 B, fsync    | 539     | 1.59 ms  | 3.77 ms  |
| Put rand 100 B, nosync   | 168,453 | 3.4 µs   | 11.4 µs  |
| Put seq 1 KB, fsync      | 575     | 1.61 ms  | 3.28 ms  |
| Put seq 1 KB, nosync     | 99,126  | 2.3 µs   | 18.3 µs  |
| Put rand 1 KB, fsync     | 389     | 1.65 ms  | 31.3 ms  |
| Put rand 1 KB, nosync    | 78,197  | 4.2 µs   | 23.2 µs  |
| Put seq 10 KB, fsync     | 450     | 1.67 ms  | 16.2 ms  |
| Put seq 10 KB, nosync    | 11,652  | 11.0 µs  | 77.3 µs  |
| Put rand 10 KB, fsync    | 442     | 1.81 ms  | 9.85 ms  |
| Put rand 10 KB, nosync   | 9,474   | 10.6 µs  | 61.6 µs  |
| Get seq 100 B            | 443,651 | 1.9 µs   | 7.9 µs   |
| Get rand 100 B           | 369,571 | 2.2 µs   | 9.6 µs   |
| Get seq 1 KB             | 323,811 | 2.4 µs   | 12.4 µs  |
| Get rand 1 KB            | 292,806 | 2.7 µs   | 12.4 µs  |
| Get seq 10 KB            | 109,969 | 5.9 µs   | 91.0 µs  |
| Get rand 10 KB           | 103,834 | 6.4 µs   | 92.5 µs  |

Sixteen concurrent writers with `fsync` reach 3,585 puts/s at 100 B and
2,730 puts/s at 10 KB: group commit folds their writes into one `fsync`
each round, so throughput is bounded by fsync latency times the number of
rounds, not the number of writes.

Recovering a database whose WAL holds 1 GB of unflushed 1 KB writes takes
1.6 s.

Gets are served from 100,000 keys loaded into settled SSTables; the hot
set fits in the page cache, so this measures the read path, not the disk.
