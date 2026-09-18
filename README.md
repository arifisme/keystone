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

The storage engine is complete. Raft, the key-value service, the
simulator and sharding follow in that order.

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
