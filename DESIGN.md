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
and forwards requests.

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
| `LogStore`     | WAL segments + snapshot file   | in-memory with modeled durability |
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
gap in the middle of the history would be silent data loss. The newest
segment is synced once it has been replayed: after a process crash its
tail may be in the page cache only, and the segment started next turns it
into an older one, which is not allowed a torn tail.

A write that fails, on a full disk for instance, can leave part of a
record in the file. Anything appended behind it would be acknowledged and
then cut off by the next replay, so after a failed write the log refuses
every further append and rotation. The next open truncates the tail.

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

## Raft

`raft.Raft` is the protocol: a struct whose only entry points are `Step`
for one incoming message, `Tick` for one unit of time, `Propose`, and
`ReadIndex`. It never blocks and never starts a goroutine. Every effect is
a `LogStore` write or a `Transport.Send`. `raft.Node` wraps it with the
state machine, proposal handles and, in production, one goroutine that
selects over the transport, the clock and queued proposals. The simulator
calls `Step`, `Tick` and `Flush` itself.

### Persistence

The node answers a vote or an append only after the state it implies is
durable. `DiskStore` writes `currentTerm` and `votedFor` as records in the
same WAL segments as the entries, so one `fsync` covers both, and keeps
the live tail of the log in memory so nothing reads the WAL until the next
open. An entry record whose index is at or below the last one truncates
the log; that is the only truncation mechanism, which keeps recovery a
straight replay. A snapshot is a file named by its index; the WAL record
pointing at it is written afterwards, so a crash between the two leaves a
file the next open discards.

### Election and replication

Election timeouts are drawn per term from `[T, 2T)` ticks. A follower that
has heard from a leader refuses to vote in that term, so one slow node
cannot unseat a working leader by timing out early.

Replication is the paper's AppendEntries with two additions. When a
follower rejects, it reports the term of its conflicting entry and the
first index of that term; the leader jumps back over the whole term in one
step instead of decrementing. And a probe below the follower's commit
index is answered without inspection, because committed entries are the
same everywhere.

A leader ships a batch to every follower that is caught up before its
own write returns, so the follower disks work while the leader's does
rather than after it, and it moves each follower's next index as batches
go out instead of waiting for acknowledgements, so consecutive batches
pipeline. Both are safe for the same reason: the leader counts its own
replica toward a majority only after its write is durable, and a batch
that was lost or never made durable on the leader is repaired when the
next heartbeat probes past it and the follower rejects.

A batch is cut at 256 entries or 1 MiB of payload, whichever comes first.
The byte cap is there for the transport: gRPC refuses a message over
4 MiB, and a follower working through a backlog of large values would
otherwise be sent the same undeliverable batch for ever. An entry larger
than the cap travels alone.

The commit rule is the one from Raft §5.4.2: the leader advances the
commit index only to an entry of its own term, even if an older entry has
been replicated to every node. An old-term entry on a majority can still
be overwritten by a leader that never saw it; only an own-term entry
proves the leader's log is the one that survives. A new leader appends a
no-op so this can happen without waiting for a client.

### Reads

Two paths, chosen per request. A log read is a command like any other:
it costs a round of replication and an `fsync`, and is trivially
linearizable because it is ordered in the log. A ReadIndex read records
the commit index, sends one heartbeat round, and once a majority answers
serves from local state as soon as that index is applied; it skips the
disk and the log but still pays one network round trip, and the leader
must have committed an entry of its own term first or its commit index
may be stale. A confirmed read index stays valid after losing leadership,
since it names a point in the log that cannot change.

The heartbeat of a read round is an append message, and a follower
behind the leader's compaction point would get a snapshot chunk instead,
whose reply carries no read id. Such a follower is sent an empty append
at the compaction point for the round. It rejects it, and a rejection
confirms the leader as well as an acceptance does, so a follower that is
still catching up counts toward the majority a read needs.

### Snapshots

When the applied log exceeds a threshold the node asks the state machine
for a snapshot and compacts the log behind it. The key-value state
machine's snapshot is an SSTable produced by the engine's `Export`: one
sequential pass that keeps the newest version of every key and drops
tombstones. Restoring installs that file as the only live table, which is
a WAL rotation and a manifest write rather than a re-insert of every key.

A follower too far behind receives the snapshot in chunks with one in
flight at a time. Each chunk is acknowledged with the offset expected
next; a chunk that cannot be placed restarts the transfer, and the
leader's heartbeat resends whatever is outstanding, so a lost chunk costs
one tick.

## Key-value service

Commands are protobuf messages in the log. Each carries a client id and a
sequence number; the state machine keeps the last sequence and cached
result per client, returns the cached result for a repeat, and writes the
session update in the same batch as the command's effects. The client
library allocates a sequence number once per operation and reuses it on
every retry, so a write whose reply was lost is applied exactly once.
Sessions never expire; that is listed under limitations.

A request may hold 1 MiB of keys and values, and a larger one is refused
with `InvalidArgument` before it reaches the log. Without a limit the
only bound is gRPC's 4 MiB per message, and that one is measured at the
wrong place: a request a few bytes under it is accepted, grows by its
Raft headers, and becomes an entry the leader holds but can send to
nobody. etcd draws the same line at 1.5 MiB for the same reason.

Everything lives in one engine under a one-byte prefix: user keys under
`k`, sessions under `s`, and the applied index under `m`. The applied
index is written in every batch, which is what lets a restart resume from
the state machine's own durable position instead of replaying the log
from the last snapshot.

Servers answer a request they cannot serve with `FailedPrecondition` and
a `NotLeader` detail naming the leader's id and address. The client
switches to that address and retries at once; a connection failure, or an
answer from a node that knows no leader, moves it to the next endpoint
with capped exponential backoff.

## Sharding

A node hosts a replica of every Raft group it is a member of, each with
its own engine, log and state machine, and one gRPC listener. Messages
and requests carry a group id; the transport demultiplexes incoming
streams by it.

The key space is cut into sixteen hash ranges. The shard map, an array of
sixteen group ids, lives under a reserved key in group 1, the meta group,
which is an ordinary key-value group that serves no user keys. The first
router to find no map writes the default round-robin assignment with a
compare-and-swap, so concurrent routers agree on it.

Every node runs a router as its client-facing service. A request that
names a group is served by the local replica. One that names none is
hashed, mapped to a group and forwarded to that group's leader through
the same client library `keystonectl` uses, so leader hints and backoff
are shared code. The caller's session travels with the request: sessions
are issued by the meta group so their ids are unique, and a group starts
tracking a session the first time it sees it. A scan is sent to every
group and merged; each group answers from its own consistent point, so a
scan across shards is not one snapshot.

### Moving a shard

`keystonectl move SHARD GROUP` is a stop-the-shard move run by whichever
router receives it:

1. The meta group's map is updated with a compare-and-swap to say the
   shard is moving, so a move of the same shard to another group is
   refused.
2. A freeze command goes through the source group's log. From then on the
   source answers writes to that shard with "moving"; reads still work.
3. The router pages through the source group, keeps the keys that hash
   to the shard, and imports them into the destination through its log in
   chunks. A page and a chunk end at a thousand keys or at 1 MiB, the
   request limit, whichever comes first, so neither can outgrow a gRPC
   message however large the values are. The last chunk opens the shard
   there.
4. A purge command through the source group's log deletes the keys and
   marks the shard gone. The source refuses reads and writes for it from
   then on, until the shard is one day imported back, so a router still
   holding the old map gets nothing from it.
5. The map entry is flipped with another compare-and-swap against the
   bytes written in step 1.

The purge comes before the flip because a frozen source still answers
reads. Flipping first leaves a moment, and after a mover's crash up to a
second, in which a router with the old map reads the source's copy while
the destination already takes writes: a stale read, in a store whose
point is that there are none. The price is that reads of the shard
stall, like its writes, from the purge to the flip. The copy in the
source is given up only once the destination has all of it through its
own log. This is the order of the MIT 6.5840 sharding lab: freeze,
install, delete, then publish the new configuration.

A move that is interrupted, by the router dying or the caller's deadline
passing, leaves the shard frozen and the map marked. Running the same
move again finishes it: a map that already names the move skips step 1,
and every other step can be taken twice. Freezing and purging are
idempotent, a freeze leaves a shard that is already gone alone, and a
chunk imported again carries the same frozen values.
The one thing that must not happen is a chunk landing after the import
has finished, because clients write to the destination from then on. So
a group refuses an import for a shard it holds, and the refusal tells the
mover that an earlier run completed the copy. There is no way to abandon
a move halfway; the MIT 6.5840 sharding lab, which this follows, fences
stale requests with a configuration number instead, which would also
cover a request delayed across two whole moves of the same shard.

Routers refresh the map every second and immediately when a group
answers "moving"; they retry the request against the new group a few
times and otherwise report Unavailable, which the client library retries
with backoff. So during a move, writes to that shard stall, its reads
stall for the last step, and every other shard is untouched.

Each group keeps its own session table. A write to the moving shard whose
reply was lost, retried after the flip, reaches a destination that has
not seen that sequence number and applies it a second time; the window is
the move itself. Session state does not travel with the shard.

## Simulation

`sim` runs a whole cluster in one goroutine. Every source of
nondeterminism is replaced: a priority queue of events keyed by simulated
time stands in for the network, the clock advances only when the loop
pops an event, the log store is a journal in memory whose tail is rolled
back on a crash, and the state machine runs on the in-memory engine.
The same seed always produces the same run, so a failure is a seed. The
section on testing in the README describes the fault schedule and what is
checked.

## Non-goals

- Distributed transactions across shards. Each key lives in one Raft group
  and operations never span groups; a scan across groups is not atomic.
- Dynamic membership through joint consensus. Group membership is fixed at
  startup.
