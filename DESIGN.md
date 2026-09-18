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

## Storage engine

Writes go to the WAL, then to the memtable. When the memtable is full it is
frozen, its WAL segment is rotated, and a background goroutine writes it out
as an SSTable. A second goroutine merges SSTables. Reads consult the
memtable, then frozen memtables, then SSTables newest first.

### Write-ahead log

The log is a sequence of segment files, `%08d.wal`. Each record is

```
length   uint32   payload bytes
crc      uint32   CRC-32C of payload
payload
```

A DB record is one batch: `seq uint64 | count uint32 | (kind uint8 |
key | value)*`, where the i-th op takes sequence `seq+i`. The sync policy is
either `SyncAlways`, where `Put` returns after `fsync`, or `SyncInterval`,
where a goroutine syncs on a timer and a power loss may drop the last
interval. Under either policy a process crash loses nothing that `Put`
returned for, because the write reaches the kernel before it returns.

Concurrent writers are batched: the writer at the head of the queue
appends every queued batch in one `write` and one `fsync`, then wakes the
rest. This is what keeps `SyncAlways` usable under load.

On open, every segment is replayed. A torn record at the end of the newest
segment is truncated away; damage anywhere else refuses to open, because a
gap in the middle of the history would be silent data loss.

A memtable maps one-to-one to a WAL segment: rotating one rotates the
other. After a memtable is flushed, `MANIFEST` records the next segment as
the oldest still needed, and older segments are deleted.

### Memtable

The memtable is a skip list with one writer and lock-free readers. Writes
are already serialized by the commit queue, so the writer needs no lock;
readers follow `atomic.Pointer` links. A node is fully built before its
predecessors publish it, and nothing is ever unlinked or modified, so a
reader can never observe a half-made node. This was chosen over a mutex
because point reads and scans should not contend with the commit path.

Every key carries an 8-byte trailer, `sequence << 8 | kind`, and the
comparator orders user key ascending then sequence descending. The newest
version of a key therefore sorts first, and a reader that seeks to
`(key, snapshot sequence)` lands on the newest version it may see.

### SSTable

```
[data block][crc32c]*      sorted internal keys
[index block][crc32c]      last key of each data block -> (offset, size)
[bloom filter][crc32c]     over user keys, 10 bits per key, 7 probes
[footer]                   40 bytes
```

Data and index blocks share one format:

```
entry*      shared uvarint | unshared uvarint | vlen uvarint |
            key[shared:] | value
restarts    uint32 offset of every 16th entry
nrestarts   uint32
```

Keys are prefix-compressed against the previous entry; every 16th entry
restarts with the full key, so a seek binary-searches the restart offsets
and then scans at most 16 entries.

The footer is

```
index offset  uint64   bloom offset  uint64
index size    uint64   bloom size    uint64
version       uint32   magic         uint32 (0x4b455953)
```

The bloom filter uses double hashing: probe `i` tests bit `h1 + i*h2`
where `h1`, `h2` are the halves of a 64-bit MurmurHash of the user key.
A point read checks the filter before touching any block.

### Reads and versions

A `version` is an immutable list of the memtable, frozen memtables, and
SSTables. Readers take a reference on the current version and on the
sequence number at that moment. A scan merges every source through a
binary heap, skips versions newer than its sequence, keeps the first
version of each key, and hides tombstones. Because a version pins its
tables by reference count, a compaction can unlink a file while a scan is
still reading it; the file goes away when the last reference drops.

### Compaction

Compaction is size-tiered. Tables are kept in age order, newest first.
Whenever four or more adjacent tables are within a factor of two in size
they are merged into one; if the count still exceeds ten, the cheapest
adjacent window of four is merged regardless of size. Only adjacent
tables are ever merged, because point reads stop at the first table that
holds the key: merging around a table would let an older version overtake
a newer one.

A merge keeps the newest version of each key. That is safe because every
reader of the output has a sequence bound at least as high as anything in
it, so the older versions could not have been visible to it anyway.
Tombstones are dropped only when the merge includes the oldest table,
since only then is there no older table that could still hold a value.

### MANIFEST

`MANIFEST` lists the live tables, the next file number, the highest
sequence in any table, and the oldest WAL segment still needed. It is
rewritten in full to `MANIFEST.tmp`, synced, and renamed over the old one
on every flush and compaction. Recovery reads it, deletes any `.sst` it
does not mention, and replays WAL segments from the recorded one onward.

## Non-goals

- Distributed transactions across shards. Each key lives in one Raft group
  and operations never span groups.
- Dynamic membership through joint consensus. Group membership is fixed at
  startup; rebalancing moves whole shards between fixed groups instead.
