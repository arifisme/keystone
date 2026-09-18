# Design

Keystone is a linearizable key-value store. Clients talk gRPC to any node;
writes are replicated by Raft and applied to a log-structured merge-tree
engine on every replica.

```
                 keystonectl / client library
                              |
                              v gRPC
   +------------------------------------------------------------+
   |  kv        Get / Put / Delete / CAS / Scan                  |
   |            client sessions (exactly-once), ReadIndex        |
   +------------------------------------------------------------+
   |  raft      election, replication, snapshots                 |
   |            Transport   Clock   LogStore   StateMachine      |
   +------------------------------------------------------------+
   |  storage   WAL -> memtable -> SSTables -> compaction        |
   +------------------------------------------------------------+
                              |
                              v
                     files on one disk
```

`shard/` sits above `kv/`: it maps key hashes to independent Raft groups
and forwards requests. It is described in its own section once built.

## Packages

**storage** knows nothing about the network. It owns a write-ahead log,
an in-memory skip list, an SSTable format, a manifest of live tables, and a
size-tiered compactor. It is also the durable log behind Raft: the log store
reuses the WAL segment and checksum code.

**raft** knows nothing about keys or values. Entries are opaque bytes. The
node is a synchronous state machine: `Step` consumes one message, `Tick`
advances one logical tick, and every side effect leaves through one of four
interfaces. A goroutine wrapper drives it in production; the simulator drives
it directly.

**kv** is the state machine that Raft applies entries to, plus the gRPC
surface. It encodes commands, keeps the per-client dedup table, and serves
reads either through the log or through ReadIndex.

**sim** runs a whole cluster in one goroutine with a seeded random source.
Time, delivery, disk durability and crashes are all decisions of the seed,
so any failing run replays exactly.

## Interface boundaries

Everything that touches the outside world goes through one of these, so the
simulator can replace all of them:

| Interface      | Production                     | Simulation                        |
|----------------|--------------------------------|-----------------------------------|
| `Transport`    | gRPC streams between peers     | priority queue of in-flight msgs  |
| `Clock`        | `time.Now`, `time.After`       | logical time, stepped by the loop |
| `LogStore`     | WAL segments + state file      | in-memory with modeled durability |
| `StateMachine` | kv engine over `storage`       | same code, in-memory `storage`    |

Randomness is a single seeded `*rand.Rand` handed in at construction.

## Non-goals

- Distributed transactions across shards. Each key lives in one Raft group
  and operations never span groups.
- Dynamic membership through joint consensus. Group membership is fixed at
  startup; rebalancing moves whole shards between fixed groups instead.
