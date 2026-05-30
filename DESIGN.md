# thinDB — Design

A single-node, columnar analytics database with a tight, fast core and deliberately small surface area. Inspired by StarRocks/Doris in storage shape, but stripped of every multi-node, optimizer, and ecosystem concern that doesn't earn its keep on one machine.

This document is the working spec. v1 is effectively complete as of 2026-05-18 — joins (every algorithm), range/opaque predicates, skew auto-routing, upserts, WAL, type coercion, and a substantial scalar/aggregate function set have all landed. Section 14 tracks what's been deferred to v2 and v3.

Decisions are presented as decisions, not options. Rationale is included where it isn't obvious; alternatives we considered and rejected are listed in `## Alternatives considered`.

---

## 1. Goals & Non-goals

### Goals
- **Single-node, embedded library** for analytical workloads on one machine.
- **Raw speed** via columnar layout, vectorized execution, SIMD where it helps.
- **Simple, predictable execution.** Queries run in the order you wrote them. No runtime optimizer.
- **Strict schema.** `CREATE TABLE` with fixed types, `NOT NULL` by default. Typing is enforced at compile time wherever possible.
- **Append-only storage** with tombstoning and background compaction.
- **Thin everywhere.** Each subsystem should be the minimum that's correct and fast — nothing more.

### Non-goals (v1)
- Distribution, replication, sharding.
- A SQL parser/dialect (v2).
- A user-facing network server or wire protocol — though an in-process `Connection` exists (`thindb.local`) and TCP transport stubs are wired (v2 ships the parser; v3 ships MySQL wire-compat).
- Client APIs for non-Zig languages (v2).
- Subqueries, CTEs, window functions (v2). **Joins are in v1** (hash / sort-merge / nested-loop / range-sweep, with auto routing and skew detection).
- Transactions or multi-statement atomicity (not currently planned).
- Timezone-aware datetimes (v2 — `TIMESTAMPTZ`).
- Cost-based query planning or statistics-driven optimization (rejected for the "thin" ethos).
- Implicit string ↔ number coercion (matches DuckDB/StarRocks; users call `to_int` / `to_string`).

### Shipped beyond the original v1 plan

Items the early spec listed as v2 that landed in v1:

- **Joins** — hash, sort-merge, nested-loop, range-sweep, with `.auto` routing via manifest stats and Misra-Gries skew detection that re-routes hash → SMJ in-place.
- **Upserts** — `Table.upsert()`; inserts on unique tables transparently apply last-writer-wins resolution (StarRocks-style).
- **Crash durability** — WAL with leader-follower group commit.
- **Implicit type coercion** in scalar functions (DuckDB-style cost-ranked overload selection).

Items genuinely deferred to v2/v3 are listed in `## 14`.

---

## 2. Architecture overview

thinDB is a Zig library. A process embeds it via `@import("thindb")`, opens a `Database` pointed at a directory on disk, and operates on it through a typed API. There is no separate server, no daemon, no IPC.

Three internal subsystems:

```
┌────────────────────────────────────────────────────────────┐
│                   public API (src/api/)                     │
│   Database · Table · Query builder · .pipe() composition    │
└────────────┬───────────────────────────────┬───────────────┘
             │                               │
       ┌─────▼──────┐                  ┌─────▼──────┐
       │ Write Path │                  │ Read Path  │
       │ (engine/)  │                  │  (exec/)   │
       │            │                  │            │
       │ memtable   │                  │ manifest   │
       │ flush      │                  │ snapshot   │
       │ compaction │                  │ operators  │
       │ deletes    │                  │ cache      │
       └─────┬──────┘                  └─────┬──────┘
             │                               │
             └───────────────┬───────────────┘
                             │
                       ┌─────▼──────┐
                       │   Storage  │
                       │ (storage/) │
                       │            │
                       │ segments   │
                       │ manifest   │
                       │ tombstones │
                       │ encodings  │
                       └────────────┘
```

A **single writer thread** processes a command queue (`Insert`, `Delete`, `Flush`, `Compact`, `Alter`). Reads run concurrently on caller threads and see a manifest-snapshot view consistent at the moment the query started.

---

## 3. Data model

### 3.1 Types

| Category | Type | Backing |
|---|---|---|
| Integer | `TINYINT` | i8 |
| | `SMALLINT` | i16 |
| | `INT` | i32 |
| | `BIGINT` | i64 |
| | `LARGEINT` | i128 |
| Float | `FLOAT` | f32 |
| | `DOUBLE` | f64 |
| Decimal | `DECIMAL(p, s)` | i64 (`p ≤ 18`) or i128 (`p ≤ 38`). `p` is total digits, `s` is digits after the decimal point. |
| String | `CHAR(N)` | Fixed-width bytes |
| | `VARCHAR(N)` | Variable-width bytes, bounded |
| | `STRING` | Variable-width bytes, unbounded (effective limit: 64 MB / value) |
| Temporal | `DATE` | i32 days since 1970-01-01 UTC |
| | `DATETIME` | i64 microseconds since 1970-01-01 UTC. No timezone awareness — applications convert at boundaries. |
| Boolean | `BOOLEAN` | u8 (0/1) |

Explicitly **out of scope for v1**: `JSON`, `ARRAY`, `MAP`, `STRUCT`, `BITMAP`, `HLL`, `PERCENTILE`, `TIMESTAMPTZ`.

### 3.2 Schema and order key

Every table requires an **order key** at creation. The order key is one or more columns by which rows in every segment are physically sorted. It is the engine's only mechanism for:

- Range pruning at scan time (min/max per segment and per row group)
- Uniqueness enforcement (when `unique = true`)
- Efficient compaction (sorted merge of segments)

The order key may be marked `unique = true` or `unique = false` (default).

Columns are **NOT NULL by default**. To allow nulls, mark explicitly:

```zig
.{ .name = "note", .type = .string, .nullable = true }
```

### 3.3 Null representation

`NOT NULL` columns carry no null metadata. Nullable columns carry a **1-bit-per-row null bitmap**, co-located with the column data in each row group. Standard Arrow/Parquet layout.

### 3.4 Decimal arithmetic

Following DuckDB semantics: precise, predictable, errors on overflow at row-level.

**Aggregates promote the accumulator type**:

| Input column type | `SUM` result |
|---|---|
| Any integer type | `LARGEINT` (i128). Errors at i128 overflow. |
| `FLOAT`, `DOUBLE` | `DOUBLE` |
| `DECIMAL(p, s)` | `DECIMAL(38, s)`. Errors at the i128 ceiling. |

`MIN`/`MAX` return the input type. `AVG` returns `DOUBLE`. `COUNT` returns `BIGINT`.

**Row-level arithmetic** stays in the column's declared type. Overflow is an error (`thindb.Error.ArithmeticOverflow`). To get wider math, the user explicitly casts upstream.

**Decimal precision/scale propagation**:

| Operation | Result type |
|---|---|
| `DECIMAL(p1, s1) + DECIMAL(p2, s2)` | `DECIMAL(max(p1-s1, p2-s2) + max(s1, s2) + 1, max(s1, s2))` |
| `DECIMAL(p1, s1) - DECIMAL(p2, s2)` | same as `+` |
| `DECIMAL(p1, s1) * DECIMAL(p2, s2)` | `DECIMAL(p1 + p2, s1 + s2)` |
| `DECIMAL(p1, s1) / DECIMAL(p2, s2)` | `DECIMAL(p1 + s2 + 4, s1 + 4)` |

Result precisions exceeding 38 are clamped to 38, with overflow → error rather than truncation. Mixed decimal/integer arithmetic promotes the integer to decimal first.

---

## 4. On-disk format

### 4.1 Directory layout

```
<data_dir>/
  <table_name>/
    manifest                       ← table-level manifest (atomically updated)
    schema.json                    ← static schema (immutable post-create)
    segments/
      <seg_id>.dat                 ← immutable segment file
      <seg_id>.tomb                ← tombstones for that segment (sparse, append-only)
    __alter_<ts>_<table_name>/     ← shadow directory used by ALTER TABLE (transient)
```

`<seg_id>` is a monotonically increasing u64. `.tomb` files are absent until the first delete that hits that segment.

### 4.2 Manifest

A small file listing the active segments for a table. On every change (flush, compaction, delete-of-an-entire-segment), the writer:

1. Constructs the new manifest in memory.
2. Writes `manifest.new`.
3. `rename` over `manifest` (atomic on both POSIX and Windows when same-volume).

Readers open `manifest` once at query start and read only segments it lists. Segments not in the manifest are invisible to that query, even if their files exist on disk.

Manifest contents (binary, little-endian):

```
magic              u32   "tDBM"
version            u16
schema_fingerprint u64   identifies schema rev — must match schema.json
segment_count      u32
[ per segment ]
  segment_id       u64
  row_count        u64
  min_key, max_key — variable-width, typed per order-key columns
  tomb_present     u8    (0 or 1)
```

### 4.3 Segment file

Each `.dat` file is a self-describing columnar container. Rows within a segment are physically sorted by the order key. A segment is partitioned into **row groups** (default **64K rows per group**, configurable per database).

```
┌─ Header ─────────────────────────────────────────┐
│ magic "tDBS", version, schema fingerprint,        │
│ segment id, total row count, row group count      │
├─ Row group 1 ─────────────────────────────────────┤
│ ┌─ Column 0 block ─┐                              │
│ │ encoding, compression, null-bitmap? (if nullable),│
│ │ min, max, data                                 │ │
│ └──────────────────┘                              │
│ ┌─ Column 1 block ─┐ … one per column             │
├─ Row group 2 ─────────────────────────────────────┤
│ …                                                 │
├─ Footer ──────────────────────────────────────────┤
│ row group offsets, per-row-group per-column min/max│
│ checksums, footer length, magic "tDBS"            │
└───────────────────────────────────────────────────┘
```

Footer is read first (via the trailing length + magic). Row group offsets in the footer let scans skip to the relevant byte ranges without parsing the whole file.

### 4.4 Column block encodings

Encoding is chosen per row group at flush time based on the column's data characteristics. The block header records which encoding was used; scanners handle each one.

| Encoding | When chosen | Data layout |
|---|---|---|
| **Plain** | Fixed-width numeric types, fallback for strings | Raw values back-to-back |
| **RLE** (run-length) | Repetitive low-cardinality data | `(value, run_length)` pairs |
| **Dictionary** | Strings with < 128 distinct values in the block | Dictionary + integer indexes |
| **Frame-of-reference** | Integer columns where `max - min` is small | Min value + bit-packed deltas |
| **Fixed-width** | `CHAR(N)` always | N bytes per row |
| **Offsets + bytes** | `VARCHAR(N)`/`STRING` when dictionary is not chosen | `u32` offsets + flat byte buffer (Arrow-style) |

After encoding, each column block is **zstd-compressed** as a final pass. Both the encoding and the zstd-compressed size are recorded in the block header.

### 4.5 Tombstone files

For each segment that has any deleted rows, a sibling `<seg_id>.tomb` file exists. Format:

```
magic "tDBT", version, count, [u32 row_offset]...
```

Row offsets are the 0-indexed positions within the segment (across all row groups). The file is append-only — new deletes append more offsets. At scan time the file is read, sorted/deduped in memory, and converted to a bitset that is ANDed into the filter step.

When a segment is compacted away, its `.tomb` file is deleted alongside the `.dat`.

---

## 5. Write path

### 5.1 Memtable

Each table has an in-memory memtable that buffers writes between flushes. Internal layout mirrors a segment: **column-oriented**, each column a growing `ArrayList`. Rows accumulate in insertion order.

For tables with `unique = true` on the order key, the memtable additionally holds a hash map (`order_key_value → row_index`) for O(1) duplicate detection at insert time.

### 5.2 Inserts

Two API surfaces, same memtable underneath:

- **Row-oriented (primary, ergonomic)**: caller passes a slice of row structs. Engine transposes into column buffers in a single O(n) pass per column. Negligible cost relative to the rest of insert work.
- **Columnar (bulk path)**: caller passes pre-built column arrays. Engine appends directly. No transposition.

Insert sequence (per batch):

1. Sort the incoming batch by order key.
2. Detect intra-batch duplicates (unique tables only) — error if found.
3. Check the memtable's hash index for cross-batch duplicates within the in-memory buffer (unique tables only).
4. For unique tables, check existing segments via per-segment `[min_key, max_key]` range — most segments are skipped without reading; overlapping segments are probed via binary search on the row-group min/max ladder.
5. If clean, append columns to the memtable. For unique tables, update the hash index.
6. If a duplicate was found at any step, return `thindb.Error.UniqueKeyViolation` and the entire batch is rejected. No partial inserts.

### 5.3 Flush triggers

The memtable becomes a new segment when **any** trigger fires:

| Trigger | Default |
|---|---|
| Memtable column data exceeds size | 64 MB |
| Memtable row count exceeds | 1,000,000 |
| Memtable has been non-empty for ≥ time, and exceeds min size | 5 seconds + 1,000 rows / 1 MB |
| Manual `db.flush(table)` call | — |

The min-size guard on the time trigger prevents pathologically tiny segments on low-volume tables.

### 5.4 Flush procedure

1. Atomically detach the current memtable (becomes immutable from the engine's perspective). Allocate a fresh empty memtable for new writes — they continue uninterrupted.
2. Compute a sort permutation from the order-key column on the detached memtable.
3. Apply the permutation to each column (one allocation per column, vectorized memcpy).
4. Open a new `<seg_id>.dat` file. Stream out row groups (64K rows each):
   a. For each column, choose an encoding based on cardinality / range statistics over that row group.
   b. Encode, then zstd-compress.
   c. Write the block, accumulating offsets + min/max for the footer.
5. Write the footer.
6. `fsync` the segment file (optional in v1; non-durable mode skips this).
7. Update the manifest: read current, append the new segment, write `manifest.new`, `rename` over `manifest`.
8. Discard the detached memtable.

### 5.5 Deletes

DELETE is **predicate-based** (Model B): users may delete by any condition the filter operator can evaluate.

```zig
try orders.delete(.{ .col = "status", .op = .eq, .val = .{ .string = "cancelled" } });
```

Execution:

1. Take a manifest snapshot.
2. For each segment in the snapshot:
   a. Scan its row groups, evaluating the predicate.
   b. For each matching row, record its in-segment offset.
   c. Append all matched offsets to `<seg_id>.tomb` (creating the file if it didn't exist).
3. The memtable is also scanned. Matching rows are removed in place (the memtable hasn't been flushed yet, so true removal is fine — for unique tables, also remove the hash index entry).

A delete that runs concurrently with reads is invisible to them — readers see the manifest snapshot taken at their query start, including the tomb file state at that moment. New deletes append to the tomb file; readers using an older snapshot just see fewer tombstoned rows than the live state.

There is **no UPDATE**. To replace a row: delete + insert.

---

## 6. Read path

### 6.1 Snapshot isolation

Every query begins by reading the manifest once. That snapshot is fixed for the lifetime of the query: which segments exist, which `.tomb` files apply, and at what size. Subsequent writes do not affect the in-flight query.

### 6.2 Operator pipeline

Reads execute as a chain of **vectorized operators**, each producing **batches** of up to **1024 rows** at a time. Each operator implements:

```zig
pub fn next(self: *Self) ?Batch          // null → end of stream
pub fn schema(self: *Self) Schema
pub fn deinit(self: *Self) void
```

**Built-in operators (v1)**:

| Operator | Purpose |
|---|---|
| `Scan` | Read row groups from a table; prunes via manifest stats; applies tombstones via bitset |
| `Filter` | Evaluates predicate; produces a bitmap-selected batch |
| `Project` | Selects / renames / excludes columns |
| `Compute` | Derived columns via scalar functions (with implicit coercion) |
| `Aggregate` | Hash-group + standard aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) plus statistical (`STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP`), `COUNT_DISTINCT`, `PERCENTILE_CONT`, `GROUP_CONCAT` |
| `Sort` | Materializes, sorts by key columns, streams sorted batches |
| `Limit` | Stops after N rows |
| `Join` (hash) | Build smaller side, probe with the larger; compound keys via order-preserving byte encoding |
| `SortMergeJoin` | Materialize + sort both sides, streaming merge; merge-only fast-path when manifest stats prove pre-sorted |
| `NestedLoopJoin` | Cartesian eval with per-pair predicates; used for pure-range, opaque-callback, or no-equi-keys joins |
| `RangeSweepJoin` | Single inequality `a OP b` between two side-sorted columns — cursor-style merge, ~2× faster than NLJ on pure-range shapes |
| `Sink` | Terminal — collects results or yields batches to caller |

Join routing (`.algorithm = .auto`): opaque predicate → NLJ; pure single-range shape → range_sweep; both sides sorted on the join keys (per manifest stats) → SMJ; otherwise hash. Hash join's build phase runs Misra-Gries sampling — under heavy skew it transfers ownership of the built columns to an SMJ at execute time.

**Memtable scan**: every Scan also reads from the (potentially non-empty) memtable of the table. Memtable rows are processed identically to segment rows. This gives read-your-writes consistency.

### 6.3 Execution model

The pipeline runs in pull mode (Volcano-style): `Sink.next()` pulls from upstream, which pulls from its upstream, etc. Each operator's `next()` returns a `Batch` — a small struct holding column slices for the rows currently in flight.

Hot kernels (predicate evaluation, aggregation accumulators, arithmetic) use `@Vector(N, T)` for SIMD. Vector width is platform-dependent; code is written generically and the compiler chooses.

### 6.4 Caching

Three-tier caching, only one of which we explicitly manage:

| Tier | What | Storage |
|---|---|---|
| 1. Always resident | Manifest, segment footers, schemas | In-memory, loaded on open / on manifest update |
| 2. LRU-bounded | Decoded row group column blocks | In-memory, keyed by `(segment_id, row_group_id, column_id)` |
| 3. Free | Raw segment bytes | OS page cache |

The LRU cache is bounded by a configurable size (default **2 GB**, set at `Database.open`). Eviction is strict LRU. **Cache entries are never invalidated** — segments are immutable, so cached decoded data is correct forever. When a segment is garbage-collected after compaction, its cache entries become unreachable from new queries and age out naturally.

---

## 7. Compaction

### 7.1 Triggers

Compaction runs on the writer thread when **any**:

- The table has more than 8 small segments.
- The oldest small segment is older than 15 minutes.
- Explicit `db.compact(table)` call.

A "small" segment is one below ~256 MB of compressed data. Large segments are only re-compacted when enough smaller neighbors accumulate.

### 7.2 Strategy

Tiered. Compaction picks a set of adjacent (by `segment_id`) small segments, k-way merges them on the order key, and writes a single new segment. Tombstoned rows from the input segments are dropped (not carried into the output).

Steps:

1. Pick the input segment set.
2. Open scanners for each, loading their `.tomb` files into bitsets.
3. K-way merge by order key, skipping tombstoned rows.
4. Stream into a new `<seg_id>.dat` file with the same encoding logic as flush.
5. Update the manifest: remove the inputs, add the output.
6. Schedule the input `.dat` and `.tomb` files for deletion. Files are deleted after a grace period (default 30 seconds) to let in-flight readers finish.

### 7.3 Concurrency

Compaction holds the writer queue (no concurrent inserts/deletes/flushes during a compaction). Reads continue unaffected via manifest snapshots.

---

## 8. Concurrency

- **Per-table mutex** serializes memtable + WAL mutations. Multiple writer threads may call `insert`/`upsert`/`delete`/`flush` concurrently; they line up at the mutex one record at a time. Each `Table` has its own mutex, so writes to different tables run in parallel.
- **Many reader threads.** Reads acquire a manifest snapshot at query start; no locks held during execution.
- **Manifest update is atomic** via `rename`. Readers always see either the pre- or post-state, never partial.
- **Memtable snapshot isolation.** Scans pin a refcounted snapshot of the memtable at start; concurrent writers see a fresh memtable. Long readers and active writers never block each other.

The atomic-rename semantics of `manifest` are load-bearing. On Windows, `MoveFileEx` with `MOVEFILE_REPLACE_EXISTING` provides the same atomicity for same-volume renames.

### 8.0 Memtable snapshot isolation

The memtable is heap-allocated and reference-counted:

- The `Table` holds one reference. A scan that captures the memtable holds another via `Memtable.acquire`.
- `flush`, `delete`, `upsert` (when they would mutate existing rows) all do **retire-replace**: allocate a new empty memtable, atomically swap the table's `memtable` pointer, mark the old one retired, release the table's reference. The old memtable's columns are never mutated again, so any reader iterating it is safe.
- When a reader releases its reference and refcount drops to zero, the retired memtable's buffers are freed.

The remaining hazard is a writer extending the **active** memtable while a reader is iterating it — an `ArrayList` append that triggers realloc would invalidate the reader's pointer. We close this with a small twist: `scan()` captures the memtable under the table mutex, and **if the captured memtable has rows, it forces a retire-replace right there**. The scan's snapshot becomes a frozen, retired memtable that no writer will ever touch again. The active memtable becomes empty; writers append to it without ever endangering this scan. Single overhead: one empty `Memtable.create` per scan against a non-empty memtable.

This is structurally similar to MVCC by full-snapshot — each long reader pins one retired memtable in memory until it finishes. Memory bound:

```
(1 active + N retired) × auto_flush_bytes
```

where N is the number of concurrent long readers. Bounded by workload, freed automatically when readers complete.

### 8.1 WAL group commit

When `sync_mode = .per_flush` AND `wal_enabled = true`, each `insert`/`delete` is durable on return. The naive implementation — fsync the WAL inside the table mutex — would serialize every writer through one fsync. Instead, we use leader-follower group commit:

1. Under the table mutex: mutate memtable, append WAL bytes, capture the cumulative `write_offset`. Release the table mutex.
2. Outside the table mutex: call `WalWriter.awaitDurable(target_offset)`.
3. In `awaitDurable`: if another fsync is in-flight (`in_progress == true`), park on a condition variable. Otherwise become leader.
4. **Adaptive coalescing pause** (key to amortization):
   - Leader spins for `coalesce_probe_ns` (20 µs) regardless of contention. Single-writer cost: ~20 µs added latency.
   - If `waiters` grew during the probe, the leader restarts the dwell clock and keeps spinning, up to `coalesce_max_ns` (200 µs total).
   - Otherwise fsync immediately.
5. Snap `write_offset`, call `file.sync()`, then broadcast. Followers wake; those whose target is now covered return immediately; others retry as the next leader.

The probe is unconditional because the "is anyone arriving" signal only appears *during* a pause — checking before the leader pauses would always see `waiters == 1` (just the leader itself, which has just incremented the counter on entry to `awaitDurable`).

Bench numbers (8 OS threads, `sync_mode=.per_flush`, tight insert loop, Windows / NVMe):

| Threads | Wall clock | fsyncs | inserts/fsync |
|--------:|----------:|-------:|--------------:|
| 1 | 67 ms | 250 | 1.0 |
| 2 | 77 ms | 256 | 1.95 |
| 4 | 87 ms | 270 | 3.70 |
| 8 | 127 ms | 370 | 5.41 |

Throughput scales sub-linearly with thread count (each fsync is now amortized over multiple writers), single-writer pays ~3% latency overhead vs. the no-pause baseline.

Truncate (called at end of flush) coordinates with `awaitDurable`: it drains the current leader, then bumps `synced_offset` to the pre-truncate `write_offset` so any pending waiters from before the truncate become no-ops (their data is now in a segment, not the WAL).

---

## 9. API

The public Zig API. v1 has no other client surface.

### 9.1 Open/close a database

```zig
const thindb = @import("thindb");

var db = try thindb.Database.open(allocator, .{
    .path = "C:/data/mydb",
    .cache_size_bytes = 2 * 1024 * 1024 * 1024,
    .flush_interval_secs = 5,
});
defer db.close();
```

### 9.2 Create/alter/drop tables

```zig
try db.createTable("orders", &.{
    .{ .name = "id",        .type = .bigint },
    .{ .name = "user_id",   .type = .bigint },
    .{ .name = "total",     .type = .{ .decimal = .{ .precision = 18, .scale = 2 } } },
    .{ .name = "status",    .type = .{ .varchar = 32 } },
    .{ .name = "placed_at", .type = .datetime },
    .{ .name = "note",      .type = .string, .nullable = true },
}, .{
    .order_key = &.{"id"},
    .unique = true,
});

try db.alterTable("orders", &.{
    .{ .add    = .{ .name = "discount", .type = ..., .default = .{ .decimal = 0 } } },
    .{ .drop   = "legacy_status" },
    .{ .rename = .{ .from = "total", .to = "amount" } },
    .{ .change_type = .{ .name = "user_id", .new_type = .bigint } },
});

try db.renameTable("orders", "orders_v2");
try db.dropTable("orders_v2");
```

`ALTER TABLE` is implemented as orchestrated copy-and-swap: create shadow, stream rows through projection, atomic directory rename. Writes are paused during the copy; reads see the old version until swap, the new version after.

### 9.3 Inserts

```zig
const orders = try db.table("orders");

// Row-oriented (primary surface)
try orders.insert(&.{
    .{ .id = 1, .user_id = 10, .total = .{...}, .status = "paid",    .placed_at = ..., .note = null },
    .{ .id = 2, .user_id = 11, .total = .{...}, .status = "paid",    .placed_at = ..., .note = "rush" },
    .{ .id = 3, .user_id = 10, .total = .{...}, .status = "pending", .placed_at = ..., .note = null },
});

// Columnar (bulk path)
try orders.insertColumns(.{
    .id        = &id_arr,
    .user_id   = &user_arr,
    .total     = &total_arr,
    .status    = &status_arr,
    .placed_at = &placed_at_arr,
    .note      = &note_arr,
});
```

### 9.4 Deletes

```zig
try orders.delete(.{ .col = "status", .op = .eq, .val = .{ .string = "cancelled" } });
```

Predicate-based; scans all segments and the memtable, emits tombstones for matches. See §5.5.

### 9.5 Queries

Each builder method returns a `Query` value carrying its output schema as a comptime type parameter. Queries are lazy — no work happens until `.next()` or `.collect()`.

```zig
var q = orders.scan()
    .filter(.{ .col = "total", .op = .gt, .val = .{ .decimal = ... } })
    .project(&.{ "id", "total", "placed_at" })
    .order_by(&.{ .{ .col = "placed_at", .desc = true } })
    .limit(100);
defer q.deinit();

while (try q.next()) |batch| {
    const ids   = batch.column(i64, "id");
    const totals = batch.column(Decimal, "total");
    // ...
}
```

`batch.column(T, "name")` is a comptime check: typos or stale column references are compile errors.

### 9.6 Composition: `.pipe()`

`Query` values are themselves sources. Variables hold intermediate stages; functions over `Source` are reusable transforms; `.pipe()` glues them.

```zig
fn last7Days(source: anytype) @TypeOf(source.filter(undefined)) {
    return source.filter(.{ .col = "placed_at", .op = .gt, .val = .{ .datetime = now() - 7*day_us } });
}

fn topUsersByRevenue(source: anytype) Source(.{ .user_id = .bigint, .revenue = .decimal }) {
    return source
        .group_by(&.{"user_id"})
        .aggregate(&.{ .{ .col = "total", .op = .sum, .as = "revenue" } })
        .order_by(&.{ .{ .col = "revenue", .desc = true } });
}

const top = orders.scan()
    .pipe(last7Days)
    .pipe(topUsersByRevenue)
    .limit(10);
```

`.pipe()` also accepts a placeholder-rooted chain:

```zig
const recent_paid = thindb.placeholder(OrdersSchema)
    .filter(.{ .col = "status", .op = .eq, .val = .{ .string = "paid" } })
    .filter(.{ .col = "placed_at", .op = .gt, .val = .{ .datetime = since } });

const q = orders.scan().pipe(recent_paid).limit(10);
```

Both forms are fully typed at comptime. `.pipe(f)` is zero-cost — Zig inlines it to `f(source)`.

### 9.7 Streaming vs materializing

- `.next()` — pull one batch at a time. No materialization. Default for forward-only consumption.
- `.collect(allocator)` — run the query to completion, materialize into an in-memory `Table` that is itself a `Source`. Useful when the result is small and you want to fork or re-query it.

### 9.8 Errors

All fallible API calls return a Zig error union. The public error surface is split between `thindb.Error` (API/catalog) and `thindb.exec.Error` (query execution):

**API-level (`src/api/api.zig`):**
```
SchemaMismatch, UnsupportedUniqueKeyType, UpsertRequiresUniqueKey,
TableNotFound, TableAlreadyExists, ColumnNotFound,
ColumnAlreadyExists, UnsupportedAlterOp,
FunctionAlreadyExists, FunctionInvalidDefinition,
```

**Execution-level (`src/exec/exec.zig`):**
```
ColumnNotFound, TypeMismatch, PredicateTypeMismatch,
UnsupportedOperatorForType,
SortNoKeys,
AggregateNoSpecs, AggregateColumnRequired,
AggregateUnsupportedType, AggregateInvalidParam,
ArithmeticOverflow,
ComputeNoColumns, ComputeNameCollision, ComputeUnsupportedExpr,
ComputeNoSuchOverload, ComputeTooManyArgs,
JoinUnsupportedType, JoinEmptyOnClause, JoinKeyTypeMismatch,
JoinColumnNameCollision,
MemoryBudgetExceeded, WindowUnsupported,
```

Plus standard Zig errors (`OutOfMemory`, IO errors via `std.Io`, etc.) propagated unchanged.

No transactions, no rollback. Each top-level API call either fully succeeds or fully fails with no side effect (e.g., a failed `insert` leaves the memtable untouched; a failed `ALTER` leaves the shadow table to be cleaned up but the original is intact).

---

## 10. Configuration

`Database.open` takes a `Config` struct. No config files in v1.

| Field | Default | Notes |
|---|---|---|
| `path` | (required) | Directory on disk. Created if missing. |
| `cache_size_bytes` | 2 GB | LRU bound for the decoded row group cache. |
| `flush_interval_secs` | 5 | Time-based flush trigger. |
| `min_time_flush_rows` | 1,000 | Guard against tiny flushes. |
| `min_time_flush_bytes` | 1 MB | Guard against tiny flushes. |
| `row_group_size` | 65,536 | Rows per row group in a segment. |
| `max_columns` | 1,024 | Per table. |
| `max_string_bytes` | 64 MB | Per string value. |
| `compaction_threshold_segments` | 8 | Trigger compaction when small segments exceed. |
| `compaction_threshold_secs` | 900 | Or when oldest small segment is older than this. |
| `gc_grace_secs` | 30 | Delay before deleting compacted-away files. |
| `durable_writes` | false | Reserved for v2 — currently does nothing. |

---

## 11. Limits

| Limit | v1 value |
|---|---|
| Max columns per table | 1,024 |
| Max precision for DECIMAL | 38 |
| Max string value | 64 MB |
| Max segments per table | 2^32 |
| Max rows per segment | 2^32 |
| Row group size | 65,536 (configurable per database) |

---

## 12. Alternatives considered (and rejected)

- **Rust or C++ for the engine.** Rejected: user preference. Zig also has stronger SIMD ergonomics than Rust for vectorized kernels and a cleaner C ABI for future bindings.
- **Go for the engine.** Rejected: GC pauses during scans, no first-class SIMD, awkward FFI for future client libraries.
- **Row-oriented storage as a co-equal option.** Rejected: doubles the engine surface for an OLTP workload that isn't the target.
- **Parquet for the segment format.** Rejected: significant external dependency surface and ABI complexity for benefits we don't need (cross-engine interop is a non-goal in v1).
- **Postgres wire protocol for the future server.** Rejected in favor of MySQL wire-compat (v3, task #139). MySQL has more BI-tool / ORM ecosystem in the StarRocks-adjacent space we sit in, and our scalar-function naming already aligns with MySQL via the parity work.
- **In-place column updates / inline tombstones inside segment files.** Rejected: breaks immutability, which is the foundation of lock-free concurrent reads.
- **Strict schema as a perf concern.** Rejected based on review: strict schema is actually faster than dynamic, not slower. No tradeoff.
- **Runtime query optimizer.** Rejected: explicitly out of scope. Query execution order is what the user wrote. Pre-execution rewrites are allowed (constant folding, predicate normalization) but no plan-cost-based reordering.

---

## 13. Build & layout

### 13.1 Repo layout

```
src/
  api/                          public Database, Table, Query builder, Connection
  engine/                       writer thread, memtable, flush, compaction, alter
  exec/                         operators (scan, filter, project, compute, sort, limit,
                                aggregate, joins, nlj, smj, range_sweep, cast, cell_io, skew)
  storage/                      segment reader/writer, manifest, encodings, compression, tombstones
  ir/                           operator-tree IR + serialization (foundation for v2 SQL parser)
  net/                          in-process Connection + TCP transport stubs
  cache/                        LRU row-group cache
  util/                         allocator helpers, small primitives
tests/
  integration/                  end-to-end scenarios
  integration_client/           Connection-mediated query surface
bench/
  main.zig                      entry + dispatch
  join_bench.zig                join algorithm benchmarks
  durability_bench.zig          WAL / sync mode benchmarks
  compact_bench.zig             compaction scenarios
  tcp_bench.zig                 transport overhead vs in-process
  harness.zig                   shared timer + report helpers
build.zig
DESIGN.md
CLAUDE.md
README.md
```

### 13.2 Build & test

```
zig build              # debug build
zig build test         # runs all `test` blocks
zig build -Doptimize=ReleaseFast
zig build bench        # runs benchmarks
```

Target Zig version: 0.16.

---

## 14. Roadmap — v2 and beyond

### Shipped in v1 (originally planned for later)

| Feature | Notes |
|---|---|
| **Joins** | hash / SMJ / NLJ / range_sweep. `.auto` routing via manifest stats + Misra-Gries skew detection that re-routes hash → SMJ in-place when one key dominates the build side. |
| **Range / opaque predicates** | Single inequality `a OP b`, multi-range (BETWEEN), `extra_predicate` post-join filter, opaque callback via NLJ. Skew detection + auto-route on top. |
| **Upserts** | StarRocks-style last-writer-wins on tables with `unique = true`. Insert auto-resolves; `Table.upsert()` is the self-documenting alias. |
| **Crash durability** | WAL with leader-follower group commit (§8.1). `wal_enabled = true` + `sync_mode = .per_flush`. |
| **Implicit type coercion** | DuckDB/StarRocks-style: numeric widening, int → float/double, bool → ints, date → datetime. Exact-match overload selection takes the fast path; coercion is cost-ranked when no exact overload exists. |
| **Statistical / set-oriented aggregates** | `STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP`, `COUNT_DISTINCT`, `PERCENTILE_CONT`, `GROUP_CONCAT`. |
| **In-process Connection** | `thindb.local(...)` returns a Connection that mediates queries — same surface a future remote-mode Connection will expose. |

### v2 — next major band (user-facing query surface)

The biggest piece is a **compiled query-plan tree** as IR — most of v2 builds on it.

| Feature | Notes |
|---|---|
| Multi-source pipelines / CTEs | Compile builder calls into an explicit plan tree before exec. Foundation for everything else in v2. |
| SQL parser + execution | MySQL/StarRocks dialect; parser emits the same IR as the builder. |
| Database / namespace system | 2-level (catalog.schema.table) per Postgres/Iceberg/BI-tool convention. Each level a directory under the Database root. |
| Temp tables + per-connection sessions | Sessions own a temp-table overlay isolated from other connections. Per-session timezone, isolation knobs later. |
| EXPLAIN plan output | Render the plan tree as text (and later JSON). Cheap once the plan-tree IR exists. |
| Zig UDFs | Trusted in-process scalar and aggregate functions registered on the catalog. Scalar UDFs participate in existing overload/coercion resolution; aggregate UDFs use a generic state-backed path. |
| Window functions | `ROW_NUMBER`, `RANK`, `LAG`/`LEAD`, framed aggregates (`OVER PARTITION BY … ORDER BY … ROWS BETWEEN …`). Likely a new `Window` operator. |
| Column defaults + auto-increment | New column metadata (`default`, `auto_increment`, future: `on_update`). Memtable insert resolves defaults / picks next ID when the row omits the field. |
| `TIMESTAMPTZ` | New type alongside `DATETIME`; existing columns unaffected. |
| Non-Zig client libraries | Each library builds the operator-tree IR locally and sends it over the wire protocol. |
| SIMD optimization pass | Audit hot paths for `@Vector(N, T)` opportunities (cast kernels, filter, aggregate accumulators, join key compare). |

### v3 — later (server / parallelism / extensibility)

| Feature | Notes |
|---|---|
| MySQL wire-protocol compatibility | Listener that speaks the MySQL client/server protocol so any mysql/MariaDB client connects. Replaces the "design our own protocol" path. |
| Table-valued UDFs | TVFs act as pipeline operators; invokable from SQL once the parser ships. |
| Auto-partitioned parallel execution | Split safely-partitionable queries into N parallel sub-queries; partial graph splits where safe. Single-threaded today; revisit after the plan-tree IR. |
| Partition key on tables | Per-key-value or hash-bucket physical partitioning. Natural parallelism axis for the auto-parallel work above. |
| Schema evolution via in-place changes | Order-key changes, column reorder. v1's copy-and-swap covers most needs. |

### Explicitly deferred — revisit much later

| Feature | Notes |
|---|---|
| External sort / spillable operators | Memory accountant exists; spill-to-disk for Sort and Aggregate when over budget. Today they throw `MemoryBudgetExceeded`. |
| Property-based tests | Random-input invariants (round-trip, join-algorithm equivalence, aggregate split-invariance). |

### Not planned

| Feature | Notes |
|---|---|
| Transactions / multi-statement atomicity | Would require coordinating manifest updates across commands. |
| Replication / multi-node | Explicitly out of scope. |
| Cost-based optimizer / statistics-driven plans | The "thin" ethos rejects this. Pre-execution rewrites (constant folding, predicate normalization) are fine; plan-cost reordering is not. |
| Implicit string ↔ number coercion | Footgun-prone (MySQL behavior); explicit `to_int` / `to_string` instead (Postgres/DuckDB/StarRocks consensus). |

---

## 15. Client/server (v2 trajectory)

Going forward, **all user queries flow through a `Connection`**. Existing `Database` / `Table` / `Query` types remain — they are the *server's* internals (and what tests use directly). The user-facing API is:

```zig
var conn = try thindb.local(allocator, io, data_dir, .{});  // in-process
// or, future:
// var conn = try thindb.connect(io, "tcp://host:5432");    // remote
defer conn.close();

var q = conn.scan("orders").limit(10);   // builds operator IR
defer q.deinit();
while (try q.next()) |batch| { ... }
```

The Connection abstracts a **transport**:
- **In-process** (today): client and server in the same address space. The client encodes operator IR into bytes; the server-side dispatcher decodes and runs against the in-process `Database`. Exercises the wire path for tests with no socket overhead. (Walking skeleton currently passes `Batch` values directly across the boundary; batch wire-encoding lands with the TCP transport.)
- **TCP** (later): same `Connection` API, bytes flow over a socket.

### 15.1 Operator IR

A single binary tree describes a query: tagged tree, each operator carries its upstream encoded immediately after the operator's payload. Format defined in `src/ir/ir.zig`. Versioned header (`tDBQ` magic + `u16` version) so future tag additions are forward-compatible.

Walking-skeleton scope today: `Scan(table_name)` and `Limit(n)`. Roadmap:
- `Where(predicate)` / `Filter` — alias for `where` at the canonical name
- `Select(columns)` — whitelist projection
- `Exclude(columns)` — drop columns; downstream cannot reference them
- `OrderBy(specs)`, `GroupBy(keys, aggs)`
- `Pipe(fn)` — compose a sub-pipeline (`fn(ClientQuery) → ClientQuery`)
- (post-server) `PipeUdf(name)` — invoke a server-registered UDF; see §17

### 15.2 User-defined functions

Current scope: embedded applications register trusted in-process Zig UDFs on the catalog through `Database.registerScalarUdf` / `Database.registerAggregateUdf`. UDF definitions are process-local configuration, not persisted catalog objects. Scalar UDFs receive vectorized `ColumnView` inputs and append into a `ColumnStore`; aggregate UDFs declare a state size/alignment plus `init`, `update_one`, optional batch/combine hooks, `finalize`, and optional `destroy`. Bad UDF code is trusted native code and can crash the process.

SQL references registered scalar UDFs as ordinary calls (`SELECT my_fn(col) ...`). Registered aggregate names are recognized during parse/analyze and run through a generic state-backed aggregate operator; built-in aggregates keep the specialized hash/radix/streaming paths. UDAFs currently support regular grouped and global aggregation; table, window, SQL-defined, dynamic-library, WASM, Python, and JS UDFs remain deferred.

Endgame: clients in many languages (Rust, Zig, C, JS, TS, Python, Go) author UDFs and register them with the server. The server holds a UDF registry; queries reference UDFs by name via SQL or `.pipeUdf("name")`. Two runtime tiers behind a common adapter interface:

| Tier | Runtime | Languages | Speed | Sandbox |
|---|---|---|---|---|
| **Native** | `dlopen` + C ABI | C, Zig, Rust, Go (`-buildmode=c-shared`), ... | Full native | None — trusted operator only |
| **WASM** | wasmtime sandbox | C, Zig, Rust, AssemblyScript, others compiling to WASM | ~10–30% slower than native | Yes — multi-tenant safe |
| **Scripting** (eventual) | QuickJS / MicroPython | JS, TS (transpiled), Python | 30–100× slower than native | Yes (engine-provided) |

The wire-level UDF contract is a single C header (Arrow-style flat `Batch` struct + `OutputBuilder` accessors). Each supported language ships an idiomatic helper crate that wraps the raw struct. Embedded `.pipe(&op)` was considered as a stepping stone and dropped — the multi-language registry is the canonical UDF path; embedded users hit the same surface via the in-process Connection transport.

---

## 16. References

- StarRocks columnar storage and compaction model (background influence — not used as a code source)
- DuckDB decimal & overflow semantics (modeled after for arithmetic rules)
- Apache Arrow column block layout (informed encoding choices)
- LSM-tree compaction tiering (informed the tiered-compaction strategy)
