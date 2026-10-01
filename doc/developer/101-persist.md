# Persist 101

## Semantics
Persist is a durable store of time-varying collections.

Each time-varying collection is called a "shard".

For the time window between a shard's **"since"** (lower bound) and its **"upper"** (upper bound), we know the history of the collection and can query its contents.
- Before the lower bound, we no longer remember the history of the collection.
- At or beyond the upper, the collection's state is still being decided.

We can only write (append) to a collection _at or beyond_ the upper. We cannot alter history before that point.

Individual records within a shard (as recorded at a given point in history) are _(key, value, multiplicity)_.

## Architecture

**Blob Storage:** Each shard's contents are written to (immutable) blobs.

**Consensus:** All coordination and bookkeeping happens in the consensus database. For each shard:
- _Trace:_ The since, the upper, and all the blobs with our data.
- _Readers:_ What each reader is looking at, so we don't delete it before they're done.
- _Writers:_ Each writer's last write, for idempotent retries.

**Clients:** There is no Persist "server". All clients interact directly with blob storage and consensus.

## Data Layout in Blob Storage
At its core, a Persist shard is just an append-only log, where writers append batches of blobs
and readers sift through the blobs to find what they need.

Here is what those blobs look like:

### Layout: Shard > Batch > Run > Part
Within a shard, records are organized into:
- _Batches._ Together, they cover the entire `[Since, Frontier)` time range without overlapping. Each timestamp within the range belongs to a single batch.
- _Runs._ Each batch is made up of one or more runs. Runs are an artifact of how batches are written, not a semantic separation of records. i.e. Two runs can cover exactly the same time and key ranges.
- _Parts._ Each run is made up of one or more parts. Each _part_ of a _run_ is responsible for a range of keys.
  Within a part, records are typically sorted by key, and within a run, parts are sorted by their key ranges (which do not overlap).
	- _Parts are internally sorted by key only if they've been through compaction. For large shards, most parts have been through compaction._

#### Data Layout in a Part File
Part files are Parquet, where the columns are:
- `t` - timestamp
- `d` - diff
- `k_s` - Arrow-encoded Parquet "key", which is either:
	- `ok` - struct with one field per relation column
	- `err` - binary-encoded `DataflowError`
- `v_s` - unused

Our Parquet writer is simple:
- only one row group
- no statistics (Predicate push-down chooses which part files to read based on stats we write into shard state--outside the part files themselves.)

_Note: There is also a legacy format with columns `k, v, t, d` and a migration format with `k, v, t, d, k_s, v_s`._

#### Interlude on Future Work: Predicate Push-Down _within_ a Part
Today, we always read and decode an entire part file.
If we stop doing that, maybe we can improve predicate push-down and even support point (key + time) lookups.

Claude's diagram of Parquet chunks and pages within our one row group:
```
File
└── Row group            (a horizontal slice of rows, all columns)
    ├── Column chunk "t" (all of t's values for those rows)
    │   ├── Page         (~1 MB of t values)
    │   ├── Page
    │   └── Page
    ├── Column chunk "d"
    │   └── Page ...
    └── Column chunk "k_s"
        └── Page ..
```

Steps to doing better:
- Split the file into multiple row groups.
- Enable `Chunk` or `Page` statistics, which gives us a page index (in the parquet footer) with stats (e.g. column min/max/nulls) for those segmentations of the part file.
- Add "Range Read" to our Blob Store interface to read from specific offsets within a file. Then, only read the row groups or pages whose stats fit the predicate.

## Writers
### First, a bare-minimum introduction to the consensus database

In order to append to a shard, a writer must perform an atomic `compare_and_append` operation:
1. By comparing sequence numbers, does the actual shard state match the latest shard state the writer has seen?
2. Durably append the writer's change to the shard's state (with the next sequence number).
    - Example state change: Append a batch of blobs for the latest timestamp range.

Any client (writers, readers, others) that needs to catch up on the latest shard state performs these operations:
1. `head`, to get an outline of the latest shard state, including a pointer to the latest state "rollup" blob.
2. `scan`, to read from the most recent state rollup through all the subsequent state changes.

_Shard state is recorded as a sequence of incremental updates (stored inline in the consensus database) interspersed with periodic full-state rollups (stored in blobs),
and each client builds its own view of shard state by applying the updates to a rollup._

### Materialized View sink
Multiple replicas can write to the same shard, including replicas running different versions of the code.
- We can't assume all writers agree on the collection's contents.
	- e.g. After a bugfix, newer replicas will disagree with older replicas.
- A writer can't assume the previous batches in Persist were written by writers it agrees with.

Therefore, if a writer wants the shard to match its own view of the collection, it must _read the shard_, calculate the diff, and commit the diff.
We call this behavior _self-correction_ because the winning writer erases any accumulated mistakes from the collection.

_On each replica:_

1. One worker chooses the time range for a batch.
    - It also _selects which worker_ is responsible for `compare_and_append`ing the batch.
2. All workers write their own parts for the batch.
3. The _selected worker_ `compare_and_append`s all the workers' runs.

### Source exports' Persist sinks
For each of a _source's_ (e.g. database) _exports_ (e.g. table), we run a `persist_sink` dataflow to record that export's collection of records.

_On each replica:_ 
1. One worker chooses the time range for a batch.
2. All workers write their own parts for the batch.
   - _N.B._ Because sources don't guarantee key ordering, each part can span the entire key range. Therefore, each part is its own run.
   - _Fun fact:_ Workers write their parts as single-timestamp batches, which the leader worker consolidates into one Persist batch, typically spanning multiple timestamps.
3. One worker `compare_and_append`s the batch with each worker's run(s).
   - _Multiple Replicas:_ Kafka and load-generator sources run on multiple replicas, which compete for a successful `compare_and_append`.
   Postgres, MySQL, and SQL Server only run on a single replica.

_Fun fact x2:_ The source exports' Persist sink implementation is derived from the materialized view sink, with the self-correction step removed.
### Tables, `txn-wal`
In each environment, the storage controller batches updates from all tables (user tables and system tables) into a group commit, timestamped by the timestamp oracle, and appends it to a `txn-wal` shard.

For every table with data changes, the storage controller then copies from the group commit in `txn-wal` into the table's shard (and deletes that table's commit from `txn-wal` to mark it as "done").

If we didn't group-commit all tables to `txn-wal`:
- We couldn't support multi-table transactions.
- We'd still tick each table separately. i.e. Even if a table didn't change, we'd still need to append to its shard every tick. Now, only `txn-wal` needs to tick.

#### Migrating system tables during 0dt upgrades
The new generation starts out read-only. The new storage controller follows `txn-wal` but cannot write to it.

For any system tables with schema changes that cannot be migrated in place, the new coordinator creates new table shards and writes their IDs to the migration shard.
The new storage controller writes the catalog state to the new table shards directly, without passing through `txn-wal` first.

On promotion, the new generation restarts, no longer in read-only mode:
- The new catalog performs its migration, with the new table shard IDs from the migration shard.
- The new storage controller takes over `txn-wal` and registers the new table shards there.
- The coordinator reads system tables from their shards and, via `txn-wal`, replaces their rows with the current state from the catalog.

_Note:_ Docs say the new generation's behavior is a hack, and it should be unified with the `txn-wal` approach.

### More Writers

These other components write to Persist but are not drivers of its design:
- Sinks: only record progress
- COPY FROM: uses the table path
- Webhook sources
- Storage-controller collections
- Catalog
- Expression cache
- Builtin schema migration shard
- Dropped-shard cleanup

## Readers
Every reader is registered in shard state, and the shard's since is the minimum of all the readers' sinces.

**Leased readers** (`ReadHandle`) read data. Each one has:
- A _since hold_: the shard's since can't advance past it, so the reader can still read at any time at or after it.
- A _seqno hold_: the oldest version of shard state the reader is still reading from. GC won't delete a blob that this version of state references, even after compaction replaces it.
  - Every part a reader returns from a snapshot or listen carries a lease on the seqno it came from. The reader's seqno hold is its oldest outstanding lease.

A leased reader stays registered only as long as it heartbeats:
- A background task heartbeats every quarter of the lease (`persist_reader_lease_duration`: 15 minutes by default, 30 minutes in cloud production).
- Each heartbeat writes a new version of shard state, which also moves the reader's holds forward.
- Dropping the handle expires the reader.

**Critical readers** (`SinceHandle`) only hold back the since. They can't read data, and they don't have a seqno hold.
- They have no lease, so they never expire. If a critical reader is registered and then forgotten, the shard's since is stuck forever.
- Each since downgrade must present the reader's current opaque token, and can swap in a new one. A stale process with an old token can't move the since.
  - e.g. The storage controller has a critical reader for each collection (`CONTROLLER_CRITICAL_SINCE`) and puts environmentd's epoch in the token, so an older environmentd can't move the since.
- _Note:_ A read-only storage controller (during 0dt) holds sinces with leased readers instead.

### Losing a Lease
Whenever a client writes a new version of shard state, it also expires every leased reader that hasn't heartbeated within its lease.
- The reader's seqno hold is gone right away, so GC can delete blobs the reader was about to fetch.
- _N.B._ Expiring a reader doesn't recompute the shard's since. That waits until some reader downgrades its since.

The reader finds out from its next heartbeat (`Leased reader ... was expired due to inactivity. Did the machine go to sleep?`) or from a fetch that finds its blob gone (`could not fetch batch part ...: reader ... has been expired out of state`).
What happens next depends on who's reading: a direct `ReadHandle` fetch panics, a compute dataflow halts the process, and storage restarts the dataflow.

The usual cause is a process too starved (CPU, swap, memory) to run its heartbeat task for a whole lease.
Expiry is also routine during 0dt: once the old generation shuts down, its readers stop heartbeating and get expired.

### Reading a Snapshot
- Get all _batches_ for the shard, up to the _time_ we want to read.
- In parallel, read all the _parts_ for all the _runs_ in those batches.

In the persist source (`shard_source`), one worker plans the read, and every worker fetches:
1. The planning worker opens a leased reader and waits for the shard's upper to pass the `as_of`.
   - If the `as_of` is before the shard's since, the read fails ("cannot serve requested as_of").
2. It leases the current seqno and reads the batch list from that version of state.
3. It skips each part whose stats rule it out (predicate push-down), and sends each remaining part to a random worker.
   - Random, not round-robin, so alternating large and small parts don't pile up on half the workers.
   - It keeps each part's lease until a worker reports the part fetched.
4. Each worker fetches its parts from blob storage, then:
   - Drops updates after the `as_of`. (The listen after the snapshot emits them.)
   - Advances the remaining updates' times to the `as_of`.
   - Applies the dataflow's map/filter/project (MFP).

After the snapshot, the source listens for new batches and reads each one the same way.
- _N.B._ Compaction can merge a batch the listen already read into a new one, so the listen drops updates from before its current frontier.

The output is mostly unconsolidated. Matching updates from different parts aren't combined, so downstream operators (e.g. arrangements) consolidate.
- _Note:_ `ReadHandle::snapshot_and_fetch` (used by e.g. txn-wal) consolidates on a single reader, with the same streaming merge that compaction uses.

### Interlude on Future Work: Consolidate on Read
Compacted runs are sorted by key, so a reader could merge runs as it reads them and emit consolidated data.
`snapshot_and_fetch` already does this, but only on one reader. The persist source sends parts to workers at random, so matching updates rarely meet.

What consolidated reads would buy us:
- Lower memory peaks during hydration. Today, arrangements first take in the shard's unconsolidated history. (database-issues#7077: ~3.6 GiB peak vs. ~200 MiB steady state.)
- Upsert rehydration work proportional to live keys, not history.
- _Monotonic_ snapshots (no retractions), so one-off `SELECT`s can use cheaper monotonic operators and `LIMIT` queries can return rows early.

Two levels (design doc PR #18244, still open, and DB-96):
- _Best effort:_ Sort parts by key lower bound before handing them out, so each worker gets a contiguous key range. Fetch and decode several parts at once.
- _Full consolidation:_ Give the source a finer timestamp that tracks progress through the key range, in a nested scope, so a consolidate operator can emit each key range as soon as it's done.
  - The hard part: the source's memory backpressure can't see the consolidation buffer, and the two can deadlock.

## Consensus
Consensus keeps one log per shard. Entry _N_ in the log is the diff that produced version _N_ of the shard's state. (_N_ is the version's sequence number, or **seqno**.)
- CockroachDB in cloud, usually Postgres in self-managed. Both use one table: `consensus (shard, sequence_number, data)`.
- Operations: `compare_and_set` (append entry _N+1_ only if the latest entry is still _N_), `head`, `scan`, `truncate` (delete entries before _N_), and `list_keys`.

Every change to a shard's state takes the same steps:
1. Apply the change to the latest state this process knows (seqno _N_).
2. Diff the new state against the old one, field by field.
3. `compare_and_set` the diff at seqno _N+1_.
4. If another client already wrote _N+1_, fetch the newer diffs and start over.

Each state change also does some upkeep:
- Expires readers and writers whose leases have run out.
- About every 128 seqnos (`persist_rollup_threshold`), asks this client to write a **rollup**: the full state, written to blob storage, then linked into state with another `compare_and_set`. Every diff carries the key of the latest rollup.
- Asks this client to run GC once enough old versions have piled up.

A process keeps one copy of each shard's state, shared by all its handles to that shard. To catch up, it scans for the diffs after its seqno. A process with no copy starts from the latest rollup.

_Note:_ Small writes (under 4 KiB) are stored inline in shard state, so their data lives in consensus. Once a shard holds 1 MiB of inline data, writers have to write to blob storage instead. Compaction moves inline data out to blob storage.

_Fun fact:_ Readers mostly don't poll consensus. After a successful `compare_and_set`, the client pushes its diff to a pub-sub server in environmentd, which forwards it to every process subscribed to that shard. Polling is the fallback.

**Every operation goes through the same `compare_and_set`.** Appends, reader heartbeats and since downgrades, registering and expiring readers and writers, compaction results, rollups, GC, and finalizing dropped shards all write the next entry in the same log (PER-38).
- Consensus writes grow with all activity on the shard, not just with data written. e.g. Every reader heartbeat writes a new version of state.
- Unrelated work contends. e.g. A reader heartbeat can take seqno _N+1_ before an append does, and the append has to retry.
- A shard's state changes one version at a time, so a bigger consensus database doesn't make a busy shard faster.

### Shard State

Claude's diagram of shard state:
```
State
├── shard_id, seqno, walltime_ms, hostname
└── collections: StateCollections
    ├── version                      state format version (0dt compatibility)
    ├── trace: Trace                 the batch list: since, upper, spine
    │   └── HollowBatch              desc (lower, upper, since), len, run_splits, run_meta
    │       └── RunPart
    │           ├── Single(BatchPart)
    │           │   ├── Hollow       blob key, size, key_lower, stats, schema_id
    │           │   └── Inline       the updates themselves, stored in state
    │           └── Many(HollowRunRef)   pointer to a blob that lists more parts
    ├── leased_readers   id → since, seqno, last heartbeat, lease duration
    ├── critical_readers id → since, opaque token
    ├── writers          id → last heartbeat, last write token, last write upper
    ├── schemas          schema id → encoded key/val schemas
    ├── rollups          seqno → blob key of that rollup
    ├── active_rollup, active_gc, last_gc_req
```

## Compaction
Compaction replaces several batches with one batch that holds the same data but is cheaper to read. One pass does both kinds:
- _Physical compaction:_ Merge small batches into a bigger one, sorted by key, in parts of up to ~128 MiB (`persist_blob_target_size`). This keeps the number of batches logarithmic in the number of updates, and moves inline data out of consensus.
- _Logical compaction:_ Advance each update's time to the since, then add up the diffs of identical (key, value, time) updates and drop the ones that sum to zero.
  - e.g. With the since at 22, an insert at 20 and its retraction at 21 both move to 22 and cancel out.

Writers trigger compaction after writing:
1. A `compare_and_append` adds the new batch to the shard's _spine_ (its batches, grouped into levels by size, forked from differential's `Spine`). The spine may answer with a request to merge some batches.
2. The writer runs the merge in the background. The write itself doesn't wait.
3. Compaction streams the input runs through a merge, writes the output to blob storage, and swaps it into state for the inputs with one `compare_and_set`.
   - If the inputs are no longer in state (e.g. another writer compacted them first), compaction deletes its output instead.
   - The replaced inputs stay in blob storage until GC deletes them.

So compaction runs wherever a shard's appends get committed: in the replica process that won the `compare_and_append` for MVs and sources, and in environmentd for tables. Processes in read-only mode (0dt) don't compact.

Limits, per process:
- Skips small merges (fewer than 8 batches, 8 parts, and 1024 updates).
- Runs up to 5 compactions at once and queues 20 more. Requests beyond that are dropped, and their batches get merged later as part of a bigger merge.
- Holds one part per input run in memory, within `persist_compaction_memory_bound_bytes` (1 GiB). If there are too many runs for that, it merges them in chunks.

### Incremental Compaction
Without it, a compaction writes all of its output, then commits it to state in one `compare_and_set` at the end. If the process restarts first, all the work is lost (and the output blobs leak).

With `persist_enable_incremental_compaction`, each chunk gets committed as soon as it's written, so a restart loses at most one chunk.
- Off by default. Partly rolled out in cloud (PER-84).

### Slow Readers Hold Back Compaction for Everyone
The since is the minimum of every reader's since, and compaction can only advance times to the since. So the slowest reader (e.g. a rehydrating source, a lagging replica, or a long-lived `AS OF` read) limits logical compaction for every reader (PER-42).
- Physical compaction still happens. Batches still merge, but updates that would cancel don't.
- Each shard has one physical layout, so a fast reader can't get a more compacted copy.
- A slow leased reader's seqno hold also stops GC from deleting the replaced inputs.

### Interlude on Future Work: Per-Reader Physical Views
Blobs never change, so readers could see different physical layouts of the same data without copying it (PER-42).
Fast readers would follow the compacted batches, and a slow reader would keep the old batches it still needs.
GC would delete a blob once no reader's view uses it.

## Garbage Collection
GC cleans up two kinds of garbage:
- _State GC:_ Old versions of shard state: diffs in consensus, and rollups in blob storage.
- _Blob GC:_ Batch parts that are no longer in shard state, usually because compaction replaced them.

GC can only clean up to the shard's **seqno since**: the oldest seqno that any leased reader still holds. (Critical readers don't hold seqnos.) Versions of state before that are no longer needed, and neither are blobs that were removed from state before that.
- So a leased reader that holds an old seqno for a long time (e.g. a slow snapshot) keeps old diffs in consensus and old blobs in blob storage.

**When:** A state change asks its client to run GC once enough versions have piled up since the last GC. Writers get the job first, and any client gets it if the shard has no writers. GC runs in the background.

**How:**
1. Scan the shard's diffs from consensus, and replay them starting from the oldest rollup.
2. Collect every blob that a diff removed from state.
3. For each rollup up to the seqno since: delete the collected blobs from before that rollup, then truncate consensus up to it.
4. Remove the old rollups from state with one `compare_and_set`.

Consensus is truncated after each rollup, so a GC that dies part-way resumes where it left off.

_Note:_ GC runs in cloud. The flag that's off in cloud is `persist_batch_delete_enabled`, which only affects `Batch::delete`: cleaning up a batch that never made it into state (e.g. after losing a `compare_and_append` race). In cloud, those blobs leak.

### Dropped Shards
Dropped shards get _finalized_:
1. Dropping a collection adds its shard to `unfinalized_shards` in the catalog.
2. A background task in environmentd advances the shard's upper and since to `[]` (the empty frontier), then calls `finalize_shard`.
3. `finalize_shard` turns the shard into a **tombstone**: it removes every reader and writer, replaces each batch with an empty one, and runs GC to delete the blobs.
4. Then the shard is removed from `unfinalized_shards`.

### Leaked Blobs
GC only deletes a blob after it sees a diff remove that blob from state. It never lists blob storage. So once state no longer knows about a blob, nothing deletes it:
- Parts written but never linked into state. e.g. A writer crashes before its `compare_and_append`, or a batch loses a `compare_and_append` race in cloud.
- Dropped shards whose blobs were never deleted.

At one self-managed customer, five dropped shards left behind over 200 TB:
- Four shards were tombstoned and removed from the catalog, but their blobs were never deleted. Why is still open (PER-86).
- The fifth shard's blobs were uploaded but never linked into state.

There's no leaked-blob reaper yet (PER-87). `persistcli inspect` can find unreferenced blobs, but nothing deletes them.
