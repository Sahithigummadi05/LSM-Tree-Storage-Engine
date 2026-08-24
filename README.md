# LSM Storage Engine

A log-structured merge-tree key-value store in Java 21 — write-ahead logging, immutable SSTables,
compaction (full and size-tiered), bloom filters, and range scans. This is the design underneath
RocksDB, LevelDB, Cassandra, and HBase, built from scratch to understand the trade-offs rather than
inherit them.

```java
try (var store = new LsmStore(Path.of("/var/data"))) {
    store.put("user:42", "sahithi");
    store.getString("user:42");   // "sahithi"
    store.delete("user:42");
}
```

## Why an LSM tree instead of a B-tree

A B-tree updates data **in place**: to change a key you locate its page and rewrite it. That's a
random seek per write, and random I/O is the slowest thing a storage device does.

An LSM tree never overwrites anything. A write goes to an in-memory table plus a sequential append
to a log. When memory fills, the whole table is flushed as a new immutable sorted file. **Every
write is sequential** — dramatically faster on both spinning disks and SSDs.

Nothing is free, and the cost is worth naming precisely:

| | B-tree | LSM tree |
|---|---|---|
| Writes | random seeks, slower | sequential, fast |
| Reads | one lookup | may check several files |
| Space | mostly compact | old versions linger until compaction |
| Deletes | remove in place | tombstone, reclaimed later |

The whole design is a three-way tension between **write throughput**, **read latency**, and **space
amplification**. Improving one costs another. Compaction is the dial: it merges files and discards
shadowed data, buying read speed and space at the cost of write bandwidth.

## How a read works

Newest to oldest — memtable first, then SSTables in reverse age order, stopping at the first hit.

**Order here is correctness, not optimisation.** An older SSTable legitimately holds a previous
value for the same key, so consulting it first would return stale data. A tombstone found on the
way ends the search and reports absence, which is what stops a delete being undone by an older file
further down.

Bloom filters short-circuit most of this. Before touching a file, a small in-memory bit array
answers "could this key be in here?" — and if not, the file is skipped entirely.

**The two bloom filter error modes are not equally dangerous:**

- A **false positive** wastes a file read and still returns the right answer. Merely slow.
- A **false negative** would make the engine skip the file holding the key and report it missing —
  silent data loss on data that was durably written.

So the no-false-negatives property is tested exhaustively against 10,000 string keys and 25,000
random binary keys, while the false-positive rate is only checked for being in a sane range.

## Durability: verified by actually killing the process

Almost every storage bug that matters hides in the crash path, and none of it is reachable by
calling `close()` politely at the end of a test. A store that never flushes still passes a
happy-path test, because the data is in memory where `get()` can find it.

So `CrashDurabilityTest` **spawns a real child JVM**, writes records, and calls `Runtime.halt()` —
which terminates immediately, running no shutdown hooks, flushing no buffers, closing no files.
It's the closest thing to `kill -9` available from inside Java. A fresh store is then opened over
the same directory to see what actually survived.

It covers: every acknowledged write surviving a hard kill; recovery with data split between
SSTables and the log; a **torn record** at the log tail being discarded rather than parsed as
garbage; a **corrupt checksum** stopping recovery instead of loading bad data; and an **unfinished
SSTable** from a crash mid-flush being deleted rather than opened.

### What this harness cannot prove

Running the same test with **fsync disabled**, all 300 records still survived. That isn't a bug —
killing a JVM doesn't clear the operating system's page cache, so the bytes had already left the
process and the OS still held them.

The honest consequence: **these tests prove recovery against process death** (`kill -9`, OOM kill,
JVM crash) — the common failure. They do **not** prove the fsync path, because the scenario fsync
defends against, power loss or kernel panic where the page cache evaporates, cannot be simulated
from inside a process. Verifying that needs real hardware losing power or a fault-injecting
filesystem. This is stated plainly here rather than left for a reader to assume the test covers
more than it can.

## Range scans

```java
var keys = store.scanKeys("user:100", "user:200");   // sorted, deletions excluded
```

This is the capability that justifies keeping everything sorted, and a major reason to pick an LSM
tree over a hash-indexed store: because the memtable and every SSTable are already in key order, a
range query is a **k-way merge of sorted streams** — nothing is sorted at query time, and no data
outside the range is read. A hash index cannot answer the same question without scanning
everything.

`MergingScanner` runs a heap-based merge in O(n log k) for n records across k sources. It is not a
plain sorted union, because the same key legitimately appears in several sources with different
values — so each source carries a **recency rank**, and when several offer the same key the newest
wins and the rest are discarded.

That detail is easy to get wrong in a way that hides: point lookups walk sources newest-first and
return the first hit, so recency is handled by the loop itself and `get()` stays correct. Only
scans have to resolve it explicitly. A bug there produces stale values in range results while every
`get()` test still passes — so scans are verified against a `TreeMap` reference model across five
seeded randomized workloads, checking full scans, twenty random bounded windows each, and the
values, not just the key set.

Tombstones are consumed by the merge rather than returned: a deleted key must not appear, and its
tombstone still has to shadow older versions from older sources — so it wins the merge, suppresses
them, and is then dropped.

## Compaction strategy, and what it costs

Full compaction is simple but rewrites the entire dataset every run, so its cost scales with total
data no matter how little changed. **Size-tiered** compaction merges only tables of comparable size
(within 4×): freshly-flushed small tables merge cheaply and often, while a large table is rewritten
only once enough other large tables exist to join it — the approach Cassandra and RocksDB's tiered
mode use.

Measured on a **growing** dataset (mostly new keys, 20% overwrites), bytes physically written:

| Rounds | Full compaction | Size-tiered | Saved |
|---:|---:|---:|---:|
| 10 | 563,708 | 374,166 | 33.6% |
| 20 | 1,958,290 | 877,382 | 55.2% |
| 40 | 7,291,280 | 2,258,302 | 69.0% |
| 80 | 28,113,100 | 5,984,604 | **78.7%** |

The percentages matter less than the shape: full compaction grows ~4× per doubling of work
(quadratic — it rewrites everything, and "everything" keeps getting bigger), while size-tiered grows
~2.6× (near-linear). The advantage widens with scale, which is the entire reason the strategy
exists.

**Two findings worth being explicit about, because the tests produced them rather than confirming
what I expected:**

*A bug the correctness tests could not see.* The first size-tiering implementation checked only
that a candidate table was not too large for its bucket. That let a small, freshly-flushed table
join a bucket anchored by a huge one — quietly turning every compaction back into a full rewrite.
Every correctness test still passed, because the results were right; the byte counter showed the
two strategies writing **identical** totals, which is what exposed it. The similarity test now has
to hold in both directions.

*A claim that was wrong until the workload was.* An earlier version measured a flat ~17% saving
that slightly *shrank* with scale, and the test correctly failed the "advantage grows" assertion.
The cause was the benchmark, not the algorithm: it reused a fixed 500-key space, so the dataset
never grew and both strategies cost the same per round. Size-tiering's advantage is specifically
about datasets that grow — so the workload was fixed to grow, and the claim then held.

## Measured behaviour

`mvn test -Dtest=BenchmarkTest`. Ranges are across repeated runs on one containerized dev box.

| Metric | Measured |
|---|---|
| Write throughput, fsync **off** | 286,000 – 347,000 ops/sec |
| Write throughput, fsync **on** | 4,800 – 5,200 ops/sec |
| **Cost of durability** | **~59–67× throughput** |
| Read latency, key present | 5.3 – 7.7 µs |
| Read latency, key absent | **0.6 – 1.1 µs** |
| Write amplification | **2.28×** (deterministic) |

Three things worth reading off that table:

**Durability is expensive, and now quantified.** Forcing every write to physical storage costs
roughly 60× throughput. That is the entire reason databases expose fsync as a tunable, and why
"how much data can you afford to lose?" is a design question rather than a technical one.

**Absent keys are ~7× faster than present ones** — the opposite of the naive expectation, and
exactly what bloom filters buy. A miss answers from a bit array in memory; a hit has to go read
the data.

**2.28× write amplification** means every logical byte stored caused 2.28 bytes to be physically
written, because compaction rewrites data already on disk. Unlike the timings this is
byte-for-byte identical every run, since it depends on the compaction schedule rather than the
machine.

## Architecture

```
   put(k,v) ──► write-ahead log (append + fsync) ──► memtable (sorted, in memory)
                                                          │  full?
                                                          ▼
                                                   SSTable-3.db   ← newest
                                                   SSTable-2.db
                                                   SSTable-1.db   ← oldest
                                                          │  too many files?
                                                          ▼
                                                    compaction ──► one merged file

   get(k) ──► memtable ──► SSTable-3 ──► SSTable-2 ──► SSTable-1
                              ▲ bloom filter skips files that cannot contain k
                              first hit wins; a tombstone means "absent"
```

| Component | Role |
|---|---|
| `wal/WriteAheadLog` | Append + fsync before acknowledging; CRC32 per record; replay stops at a torn tail |
| `memtable/Memtable` | Sorted in-memory buffer (`ConcurrentSkipListMap`, unsigned key order) |
| `sstable/SSTable` | Immutable sorted file: data, sparse index, bloom filter, footer with magic |
| `bloom/BloomFilter` | Double-hashed bit array sized from expected entries and target FP rate |
| `LsmStore` | Read path, flush, compaction (full and size-tiered), crash recovery |
| `MergingScanner` | Heap-based k-way merge for range scans, newest-version-wins |

**A few decisions worth calling out:**

- **The index is sparse** — one entry per 64 records. A dense index would be as large as the keys
  themselves and defeat the point of keeping it resident. A lookup binary-searches to the nearest
  anchor then scans at most 64 records: a bounded I/O cost to keep the index small.
- **SSTables are written to a temp name and atomically renamed**, after an fsync. A crash mid-write
  can therefore never leave a partial file under a real name.
- **A footer with a magic number is written last.** No valid footer means the file was never
  finished, so recovery deletes it — safe, because its records are still in the log.
- **Keys compare unsigned** (`Arrays.compareUnsigned`). Java's `byte` is signed, so a naive
  comparison sorts `0x80` before `0x00` and silently breaks binary search over the index.
- **Tombstones are only dropped when every table is being merged.** A tombstone shadows older
  values; discarding it while an unmerged older table still holds that key would resurrect deleted
  data. Because compaction here always consumes all tables, nothing older can survive to
  contradict it — a partial compaction scheme would have to keep them.

## Testing

```bash
mvn test     # 52 tests
```

Beyond the crash and bloom suites, `LsmStoreTest` includes **differential testing against a
reference model**: 5,000 randomized mixed put/delete operations run against both the engine and a
plain `HashMap`, with the two compared throughout and at the end. That's what catches interactions
between flush, compaction, tombstones, and recovery that no hand-written case would think to
combine — and it's re-verified after closing and reopening the store.

Explicitly covered because they are the classic LSM failure modes:

- A deleted key **staying deleted** after compaction (the resurrection bug)
- A tombstone in a newer table shadowing a value in an older one
- Compaction keeping the newest of five versions of a key
- Compaction actually **reclaiming space**, not just merging files
- Binary keys with high bytes (`0x80`, `0xFF`) sorting and retrieving correctly

## Limitations

Stated rather than left to be discovered:

- **Two compaction strategies, neither levelled.** Full and size-tiered are both implemented;
  RocksDB's levelled mode bounds write amplification further at the cost of more read work.
- **Compaction is synchronous.** It runs on the calling thread and blocks writes. Production
  engines compact in the background.
- **SSTable data is held in memory once opened.** Fine at these sizes, wrong for datasets larger
  than RAM, which would need `mmap` or paged block reads.
- **Single writer.** `put`/`get` are synchronized on the store; there is no MVCC or snapshot
  isolation.
- **Scans materialise each source.** `scan()` reads whole SSTables into lists before merging,
  which is fine at these sizes but should stream blocks for datasets larger than memory.
- **fsync durability is unverified** — see the crash-testing section above.
