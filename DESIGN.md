# thinDB — Design

A single-node, columnar analytics database with a tight, fast core and deliberately small surface area. Inspired by StarRocks/Doris in storage shape, but stripped of every multi-node, optimizer, and ecosystem concern that doesn't earn its keep on one machine.

This document describes the current single-server engine. It includes an embedded API, SQL compilation, MySQL/PostgreSQL/native listeners, joins, CTEs, windows, UDFs, and parallel analytical execution. The execution pipeline is documented in [docs/simple_query_pipeline.md](docs/simple_query_pipeline.md).

## 1. Goals & Non-goals

- Run analytical workloads efficiently on one server, with an embeddable core and optional network frontends.
- Keep columnar execution, memory ownership, error handling, and persistence transitions explicit.
- Select general physical techniques from query shape and available metadata before execution. This work adds no query-specific recognizers, join-order optimizer, or group-key width policy.
- Support strict schemas, immutable segments, tombstones, WAL recovery, and background compaction.
- Keep distribution and replication outside the core. Ordinary SQL transaction verbs do not provide rollback or isolation; the supported staged-write XA protocol is described in section 8.2.

---

## 2. Architecture overview

thinDB is a Zig library with an optional standalone server. A process embeds it via `@import("thindb")` and opens a `Catalog` or the `Database.open` convenience wrapper. SQL frontends parse into IR, compile physical operators, and execute against the same storage core.

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

Per-table mutexes serialize writes. Reads capture segment and memtable snapshots, then execute on caller and worker threads. Background flush and compaction use the same ownership and publication rules as foreground operations.

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

### 3.4 Arithmetic

Integer arithmetic matches StarRocks (4.0.10): result types widen one level
and BIGINT wraps silently. Decimal arithmetic follows DuckDB: precise, and an
error on overflow at row level.

**Integer operators** (both operands integers; BOOLEAN counts as the narrowest):

| Operation | Result type | Overflow |
|---|---|---|
| `a + b`, `a - b`, `a * b` | common type widened one level: TINYINT→SMALLINT, SMALLINT→INT, INT→BIGINT, BIGINT→BIGINT | BIGINT wraps (two's complement): `BIGINT_MAX + 1 = BIGINT_MIN`, `BIGINT_MAX * 2 = -2` |
| `-a` | `a`'s type widened one level (parsed as `0 - a`) | `-BIGINT_MIN = BIGINT_MIN` |
| `a DIV b`, `a % b` | common type | `INT_MIN DIV -1 = INT_MIN`, `BIGINT_MIN DIV -1 = BIGINT_MIN`, `x % -1 = 0`; a zero divisor gives NULL |
| `ABS(a)` | SMALLINT→INT, INT→BIGINT, BIGINT→BIGINT | `ABS(BIGINT_MIN) = BIGINT_MIN` |
| `a / b` | `DOUBLE` (or `DECIMAL`) | IEEE; `/ 0` is ±inf |

An integer literal in integer arithmetic takes the narrowest type that holds
it, so `SELECT 2147483647 + 1` is INT + TINYINT → BIGINT `2147483648`, and
`smallint_col + 1` is INT. The result type is decided once, in
`scalar_fn.intArithResultType`; kernels convert both operands to it and use
wrapping ops. LARGEINT operands stay LARGEINT. A widened result passed to a
function's narrower integer parameter narrows back, as in StarRocks (see
implicit type coercion).

Known difference: StarRocks returns LARGEINT for `ABS(BIGINT)`, so
`ABS(BIGINT_MIN)` is `9223372036854775808` there. thinDB keeps BIGINT, which
wraps to `BIGINT_MIN`.

**Aggregates**:

| Input column type | `SUM` result |
|---|---|
| Any integer type up to `BIGINT` | `BIGINT`. Wraps on overflow, like the operators. |
| `LARGEINT` | `LARGEINT` |
| `FLOAT`, `DOUBLE` | `DOUBLE` |
| `DECIMAL(p, s)` | `DECIMAL(38, s)`. Errors at the i128 ceiling. |

Every SUM path (generic, V2 handlers, radix, region, SMA metadata, affine
reduction) accumulates exactly in i128 and truncates to BIGINT at emit. That
equals the wrapped sum, because truncation commutes with addition mod 2^64.
ORDER BY and HAVING on a SUM see the emitted, wrapped value. `AVG` divides the
exact sum and returns `DOUBLE`. `MIN`/`MAX` return the input type. `COUNT`
returns `BIGINT`.

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

The manifest selects the active immutable segments. Flush and compaction build a candidate without changing the published in-memory list. They finish the referenced output files, atomically replace `manifest` via `manifest.tmp`, then install the new in-memory state. Failed publication retains the old input ownership. TRUNCATE follows the same rule: publish an empty manifest and WAL checkpoint before replacing the memtable and reclaiming old files; segment IDs remain monotonic while deferred deletion is possible. Late deletes found during compaction reconciliation are written to the output tombstone before that output is selected.

Manifest v11 has a 56-byte header, including a 16-byte WAL generation and the covered physical byte offset. This checkpoint makes a published flush recoverable even if subsequent WAL replacement fails. WAL v2 has a 32-byte header with its generation. Readers also accept manifest v10 and WAL v1. Older binaries cannot read newly written formats; downgrade testing must use an untouched snapshot.

The exact binary layouts live in [src/storage/manifest.zig](src/storage/manifest.zig) and [src/engine/wal.zig](src/engine/wal.zig). Segment entries include row/byte counts, leading-key statistics, per-column statistics, and cardinality sketches.

Durable mode syncs output files before publishing references, and syncs affected parent directories on POSIX before reclaiming the old WAL or inputs. Windows performs file sync and same-volume replacement; the directory-sync helper is unsupported there. `sync_mode = .none` remains the default and does not promise power-loss durability.

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
│ row group offsets, per-row-group per-column stats │
│ (min/max/sum/null_count), checksums, footer       │
│ length, magic "tDBS"                              │
└───────────────────────────────────────────────────┘
```

Footer is read first (via the trailing length + magic). Row group offsets in the footer let scans skip to the relevant byte ranges without parsing the whole file.

Per-row-group per-column stats are small materialized aggregates: min/max (zone maps, per-type i128 encoding), an exact `null_count`, and a per-type `sum` slot (integer sum; f64 sum for floats; for strings, the blank-excluded min prefix used by ORDER BY pruning). A bare global `SUM` / `AVG` / `COUNT(col)` / `MIN` / `MAX` over a tombstone-free table answers from these without touching data; any tombstone or unflushed memtable row makes the stats-dependent lane fall back to the scan path.

### 4.4 Column block encodings

Encoding is chosen per row group at flush time based on the column's data characteristics. The block header records which encoding was used; scanners handle each one.

| Encoding | When chosen | Data layout |
|---|---|---|
| **Plain** | Fixed-width numeric types, fallback for strings | Raw values back-to-back |
| **RLE** (run-length) | Repetitive low-cardinality data | `(value, run_length)` pairs |
| **Dictionary** | Strings with < 128 distinct values in the block | Dictionary + integer indexes |
| **Frame-of-reference** | Integer columns where `max - min` is small | Min value + bit-packed deltas |
| **FSST** | High-NDV strings dict declines, when it saves ≥ 12.5% | Block-local symbol table + per-row compressed slices (random access preserved; stays compressed in cache, decoded only at materialization) |
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
3. The memtable is also scanned. Matching rows go into the WAL as a `replace` record that retracts them and inserts nothing, then are removed (the memtable hasn't been flushed yet, so true removal is fine).

The WAL records the rows a DELETE removed, never its predicate. Replay therefore has no predicate evaluator that could drift from the live one: it removes exactly what the live DELETE removed. Older binaries logged the predicate (`delete` / `delete_expr` records). Replay still reads those from a log such a binary left behind, but nothing writes them.

A delete that runs concurrently with reads is invisible to them — readers see the manifest snapshot taken at their query start, including the tomb file state at that moment. New deletes append to the tomb file; readers using an older snapshot just see fewer tombstoned rows than the live state.

UPDATE is delete + insert in batches: the memtable's matching rows form one batch, and each matching row group of a segment forms another. Each batch goes into the WAL as one `replace` record before it is applied. The record carries the batch's deletes (the retracted memtable rows, or segment offsets) together with the replacement rows. Replay retracts memtable rows, appends the replacements and merges the offsets into the `.tomb` files. On a plain table a retracted row removes one equal row. On a unique table it removes every row with its key, since until the post-replay upsert pass the recovered memtable still holds the versions that later inserts superseded. A crash can therefore leave an UPDATE applied up to some batch. It never keeps a delete without its replacement, and never keeps both versions of a row. Segment offsets reach the `.tomb` file once per segment. They also reach it before any flush retires the WAL, since an auto-flush can fire mid-UPDATE.

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

Scan pruning resolves column references with the same rules as row evaluation.
Alias, projection, and compute wrappers map hints back to unchanged source
columns. Limits and windows stop hints; aggregates only forward group-key hints
when their output is uncapped. A hint must not change the rows used to calculate
a window, an aggregate value, or a limited result.

The staged SQL compiler also derives join scans' required columns from
their ancestors within the current query block. This keeps unused payloads out
of lookup scans and hash builds even when a sibling CTE contains a wildcard.
It retains join keys, filters, expressions, grouping and ordering inputs through
filter/compute wrappers. It declines at wildcards or column-scope boundaries it
cannot resolve. This pre-execution column projection leaves join order and SQL
results intact.

The parser decides which join input each `JOIN ... ON` column belongs to. A
qualified column goes to the input its qualifier names. An unqualified one goes
to the single input whose output has that name, as MySQL resolves it. Base-table
columns come from the session's catalog through the parse context, derived
tables and CTEs from their projection, and table functions from their declared
output. A name that both inputs have is an error (`SqlOnColumnAmbiguous`, MySQL
1052). If an input's columns can't be listed (a file scan, or a parse without
the catalog), unqualified names on that join don't resolve.

Before preparing a fused hash join, a pure filter over an existing materialized
stage may check that stage for a surviving probe row. An empty probe skips the
lookup builds through a chain of non-FULL joins. The check reuses stage buffers,
retains parallel probing for nonempty inputs, and does not add a materialization
boundary or choose a different join order or algorithm.

Parallel grouped aggregation initially reserves at most one 8,192-row batch's
worth of groups per bucket and allocates its state slab only when rows arrive.
This keeps small tables' setup allocations out of the workers' allocation
traffic while bounding speculative memory for large hints. When a table grows,
it forecasts capacity from the observed number of distinct composite keys per
source row, counting weighted run partials by their original row counts. The
forecast has 25% headroom and is capped by the existing statistical reservation
hint; actual insertions can always grow beyond either estimate. This changes
memory reservation only, preserving operator order, grouping, and aggregate
semantics. Reused workspaces retain their allocated capacity and reset the
observed row counters. Developer profiles report actual groups, hash bytes,
state capacity/bytes, growth count, and worker-summed allocation time.

Group accumulator storage grows in pages of 8,192 records. A small first page
can grow up to that bound; subsequent growth adds pages without moving existing
records. Key packing and record widths are unchanged. State pages reserve for
the next batch rather than copying a large slab to match a speculative forecast;
the hash table retains its bounded forecast. Published staging buffers transfer
ownership to the group queue, and replacement buffers are acquired only when
another append needs them. Numeric count-ranked top-N compares count and key
before constructing a full candidate record.

Grouped programs carrying variable-length aggregate inputs give ready grouping
work priority over staging and scanning. Staging yields after one chunk batch
so consumers can release payloads before producers allocate more. Numeric-only
programs retain their existing scheduling policy. The shared raw/group recycle
pool for variable-length inputs retains at most 2 GiB of slab, reference and
payload capacity, further capped at one eighth of the query and shared memory
budgets. Surplus idle buffers are freed; live buffers and aggregate state remain
subject to the ordinary allocation budget. These programs use reclaimable
workspace allocation even when the diagnostic arena-workspace option is set.

Count-only grouped programs use immediate updates while a bucket's live state
is below 2 MiB. Larger live states prefetch existing accumulator records and hold
up to sixteen pending count increments per worker's batch in stack storage. This
physical-kernel choice uses actual live bytes and preserves the query plan. The
batch reserves state capacity before probing, so these record addresses remain
valid until every pending increment is applied. New groups initialize their
counts immediately. A separate kernel keeps deferred-buffer branches and live
registers out of the ordinary update loop. Other aggregate programs retain their
existing update order; pending counts are drained before batch completion can
release work credits.

Consumers that explicitly support encoded input may consume pinned raw or RLE
blocks during a scan callback. The callback completes before pins are released,
so ordinary batch lifetimes do not change. Integer global reductions fold RLE
values and lengths directly and borrow raw views. Count-only grouped pipelines
can merge non-null integer key runs without expanding their logical rows first.
Derived inputs, filters, unsupported encodings, and tombstoned segments retain
the ordinary scan path. Results are computed on each execution; this does not
cache aggregates or change the stored representation.

Block-pruned top-N workers borrow raw probe columns, retaining pins through
predicate evaluation and candidate selection. Only retained candidate keys are
copied; output payloads are fetched after selection. Non-viewable encodings use
owned decoding, and each worker reuses its view and predicate-mask buffers.

Parallel grouping collects its final candidates only after every scan producer
has closed and all published staged rows have been aggregated. A row stays
counted as unfinished while queued, being partitioned, held in a partial bucket
buffer, or being aggregated. Weighted run partials count once in this work
counter; their weights still determine aggregate values. Empty queue snapshots
cannot establish completion during a hand-off. Failed folds retain their work
count until the query aborts, and aborted workers skip candidate collection.

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

### 6.5 Keyed pipeline regions

SQL blocks declared with `WITH KEYED BY (...)` can compile a supported CTE
subtree into one region: partition once at entry, execute the operator chain
within each shard, and publish its output as an ordinary materialized stage.
The compiler verifies the key contract at each candidate boundary. It may
select an inner CTE below a join, window, alias, or primary TVF input; the
outer operations then execute through the staged engine. The declaration
requests regional execution wherever compatible; a block with no valid
region uses ordinary execution. Incompatible window partitions, coarser
groups, and global sort/limit boundaries can feed a later region through
staged ingress. Earlier compatible CTEs can independently form regions within
that ingress, preserving the user's operator order. Invalid SQL still raises
its ordinary error. Stage provenance and region traces distinguish actual
engagement from fallback.

SQL windows use the ordinary window evaluator on complete shard-local
partitions. The compiler resolves partition, order, argument, and output
names once, and verifies that every window partition retains the routed key.
Different specs share sorting when their partition/order keys match; frame
and NULL behavior remains per call. Worker instances borrow input columns
and retain output buffers. Ranking, distribution, offset, value, and aggregate
window functions therefore share semantics with ordinary SQL rather than
requiring separate regional implementations.

Frame evaluation distinguishes physical rows (`ROWS`), peers (`RANGE`), and
peer groups (`GROUPS`). Bounded `RANGE` currently accepts one numeric order
column; decimal offsets respect the order column's scale. `FIRST_VALUE`,
`LAST_VALUE`, and `NTH_VALUE` honor the actual frame, including empty frames
and `IGNORE NULLS`. Temporal range offsets and `EXCLUDE` remain unsupported.

Entry expressions may replace SQL-visible input names while retaining their
original inputs under distinct physical slots. Projection expansion uses the
same rules as ordinary SQL. Cached runs reconstruct the same entry recipe
against fresh snapshots.

Ordinary `UNION ALL` can feed a region without table functions. Its branches
and entry projections/filters compile through the staged SQL compiler,
preserving positional column naming, type widening, duplicates, and shared
or explicitly materialized CTEs. The resulting stream is partitioned once;
supported downstream operations execute within the region. Union ingress
currently uses one scatter worker, with normal parallel execution available
inside its SQL branches. When there are fewer input streams than workers,
bucket sort buffers are reserved after ingress and the independent sorts run
in the parallel shard phase. This preserves arrival-order tie breaks without
concurrent allocation from the input worker's arena.
Every cached run rebuilds the source against fresh
snapshots. Grouping exactly by the current range keys is supported as one
aggregate group per range, including NULL keys and all-NULL values.

`UNION ALL` branches that reference the same CTE input can instead remain
inside one region. The compiler retains the shared frame, executes each
branch's filters, projections, expressions, compatible windows and joins,
then concatenates their outputs within each declared-key partition. Nested
unions use the same rule. Frame snapshots borrow column buffers; each
branch owns its result buffers until the union consumes them. Filters and
joins retain empty range positions so branches can be aligned without
another exchange. Positional names, casts and NULLability use the ordinary
union's column planner, and left-before-right order preserves window ties.

Fusion requires value-identical partition keys in corresponding output
positions and an exclusively consumed shared input. Forced materialization,
externally shared CTEs, key replacements, incompatible windows, branch
aggregation/table functions and joins depending on the retained input keep
ordinary staging. A failed fusion attempt retries the existing staged-ingress
path at the same boundary. Cached programs recheck both the source versions
and the branch-sharing recipe before reuse. These are structural SQL rules;
no query text, table name or UDF name selects the optimization.

Branch collection checks join kind, range conditions and residual predicates
before preparing sources or lookup tables. A branch already filtered to false
also retains ordinary staging so its joins can be pruned. These structurally
unsupported forks do not execute speculative join inputs before fallback;
compatible regions above or below the fork remain available.

Failures that still require data-dependent proofs, such as duplicate lookup
keys, can reuse a bounded per-database rejection hint. Its fingerprint includes
the input/table versions, declared keys, CTE sharing, session/compile context
and immutable scalar kernel identity. Changed inputs retry; volatile calls,
unversionable sources and UDFs without an immutable execution contract do not
retain rejection hints. The hint stores no rows and only skips fusion; the
ordinary fallback always recompiles. Join-input compilation/execution errors
retain their error identity instead of becoming cached eligibility failures.

Regions whose program folds to only emission use the ordinary scan path,
avoiding an exchange and consolidation without shard-local work. Column
movement supports every stored payload type, including Boolean and UUID
values and their validity bits. Broadcast and co-partitioned join payloads
retain their right-side qualifier; they cannot overwrite a same-named left
column. Explicitly selected right join keys retain their own values and NULLs.
Cached lookup and emptiness proofs include the table's cache UID and
tombstone generation, so recreating a table or deleting only persisted rows
invalidates them even when memtable and manifest counters are unchanged.

Consecutive entry projections preserve their evaluation order: only the
lowest projection is absorbed into the scan entry; later projections and
their intervening computes execute in the region. An unordered, row-aligned
table function with an `.either` execution contract does not fix the initial
range granularity when a later operation requires a different partition.
The existing partition checks still determine whether each call can run
per range or over the complete shard.
Passthrough TVF outputs use their declared string-family type even when the
input uses another compatible string type. Borrowed views preserve the
original bytes and NULL bitmap without copying or changing input columns.
Frame-replacing TVFs retain routed-key provenance only under their existing
`ordered_output` contract: the call's partition columns must be present in
the output and preserve their values. The compiler binds the route to that
new physical output column. Unmarked kernels use ordinary execution.
Aggregation similarly carries provenance through an unchanged group key,
and remaps constant-column bookkeeping to the new frame. Windows and
co-partitioned joins check this physical identity, not a reused SQL alias;
computing a replacement key does not inherit it.
Entry-filter constants survive a replacing TVF only when its partition-value
contract preserves them, and survive aggregation only through group columns.
Reusing their names for changed outputs cannot keep an earlier literal join
shortcut.

For an unchanged declaration, the cache remembers which inner CTE boundary
compiled successfully. Repeated queries validate that boundary directly,
avoiding repeated evaluation of join inputs for unsupported outer candidates.
The declaration fingerprint covers the original subtree, source table
versions, and table-function identities before compilation's shared-IR
rewrites; the stored anchor hash identifies its cached program.
The declared keys, kernel identities, consumed table versions, and fresh scan
schemas are revalidated. Operations above the cached boundary compile
normally against current data.
For temporary tables held entirely in memory, up to 16,384 rows and 4 MiB,
cache validation fingerprints the complete read schema and ordered contents
under the table lock. Identical replacements can reuse the program across
sessions; changed values, NULLs, types, or ordering invalidate it. Larger or
spilled temporary tables disable this program cache instead of relying on
reusable allocation addresses as table identities.

Regional windows append results in the original row positions, so independent
window orders can coexist. Integer sums follow §3.4: `SUM` over an integer
up to BIGINT accumulates in i128 and returns BIGINT, wrapped; `SUM(LARGEINT)`
uses checked i128 accumulation and returns `LARGEINT`.
Consolidation keys retain all 64 integer bits plus a distinct NULL marker;
adjacent BIGINT values must never collapse into one partition.

See [REGION_PLAN.md](docs/plans/REGION_PLAN.md) and
[REGION_ELIGIBILITY_PLAN.md](docs/plans/REGION_ELIGIBILITY_PLAN.md) for the
runtime design, supported constructs, and remaining work.

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

Compaction builds output away from the table mutex. The commit phase reconciles concurrent deletes under the mutex, publishes output tombstones, and then publishes the candidate manifest. A separate compaction lock prevents overlapping compactions and excludes XA rollback from an in-flight merge.

---

## 8. Concurrency

- **Per-table mutex** serializes memtable + WAL mutations. Multiple writer threads may call `insert`/`upsert`/`delete`/`flush` concurrently; they line up at the mutex one record at a time. Each `Table` has its own mutex, so writes to different tables run in parallel.
- **Many reader threads.** Scans capture snapshots under the table mutex, then release it. Query lifetime leases prevent destructive catalog teardown and XA publication from invalidating borrowed table state.
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

Tombstones never get ahead of the log. A tombstone can hide a row whose replacement, from an UPDATE or a unique-key upsert, so far lives only in the WAL. So `Table.mergeTombstones` syncs the WAL before it writes a `.tomb` file whenever sync is on.

---

### 8.2 Ownership and XA staged writes

A Catalog obtains an exclusive OS lock on `.thindb.lock` before recovery or temporary-file cleanup and holds it through close. Schema table initialization is serialized so one name has one Table/WAL owner. Dropping names remain reserved until teardown finishes.

Normal statements and API scans/writes hold shared catalog leases. XA commit and destructive SQL DDL take an exclusive lease. Nested calls reuse their calling thread's lease; attempting to upgrade while that thread still owns a query returns `TableBusy`. Catalog close rejects new work and waits for live leases. Borrowed database/schema/table pointers are invalidated by drop or close; callers racing destructive DDL must hold a statement lease across lookup and use, as the wire handlers do. Asynchronous allocation cleanup retains a separate lifetime reference, keeping the catalog allocator and memory pool alive without holding up subsequent statements.

XA stores encoded write statements, their database, and their originating schema. PREPARE succeeds only after its bounded recovery record is atomically persisted. COMMIT keeps the branch recoverable while it validates targets, takes the exclusive visibility lease, snapshots manifest/WAL/tombstone metadata into `_xa/commit`, applies statements, and flushes each affected table. A durable completion marker decides recovery. Without it, startup restores the old metadata; with it, startup retains the committed data and removes the prepared record. Journal retirement uses a directory rename before cleanup so interrupted cleanup cannot turn a completed commit into rollback.

Statement errors trigger undo and leave the branch prepared for retry. If undo or the durable decision cannot be resolved, the catalog rejects further work with `RecoveryRequired`. Absent-branch COMMIT remains an idempotent success for the existing CDC integration. Prepared branches do not expire by default.

This is a staged-write protocol, not general SQL transactional isolation: reads inside ACTIVE do not see an uncommitted write set, and expressions are evaluated at commit. DDL is not part of the staged write set.

### 8.3 Query memory and cancellation

One thread-safe query resource context follows SQL physical operators and worker backends. Allocation wrappers charge requested live capacities, including variable-length payloads, hash state, sort permutations, and worker buffers, against per-query and shared limits. Estimates remain useful for planning, but do not enforce these limits. Rejected growth returns `MemoryBudgetExceeded`; there is no spill fallback yet.

Result buffers and metadata remain charged while owned. Retained region-pool capacity is charged when borrowed by a query and detached when returned to the separately capped pool. Asynchronous frees return reservations only when the corresponding storage is released. Allocator bookkeeping, allocator-internal rounding/freelists, parser/protocol buffers, database metadata/memtables, and the separate source cache are not an exact process-RSS ceiling.

Wire handlers reset their cancellation token at statement acceptance, before parsing/compilation. Compilation and eager subqueries share the token with execution. Scans, worker scheduling, sort partitions/passes, regional operations, and merge loops check it cooperatively. `QueryCancelled` unwinds ordinary resource ownership. Polling does not preempt a native UDF callback or an operating-system I/O call; this is cooperative cancellation, not a hard latency guarantee.

The server trips the same token when a MySQL-wire client disconnects mid-statement. Its connection reaper probes each connection's socket every 5 s, and cancels a statement whose only product is its result set (not a write, DDL or EXPLAIN) once the peer has closed. Writes run to completion, as in MySQL. The PostgreSQL wire does not arm this yet.

Each connection records what it is doing: its user and client address, current schema, command, and when that command began, plus up to 1 KiB of the running statement's text. `SHOW [FULL] PROCESSLIST` lists that record for every connection, so a runaway statement's id can be found and passed to `KILL`.

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
WalOrphaned, XaBranchTooLarge, XaInvalidXid,
DatabaseInUse, TableBusy, RecoveryRequired, DurabilityUncertain, DatabaseClosed,
```

`WalOrphaned`: a `wal` file sits inside the table's `segments/` directory. Replay only reads the log beside the manifest, so that file holds acknowledged rows a normal open would silently drop; the table refuses to open until an operator moves the log into place (same schema fingerprint) or aside.

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
MemoryBudgetExceeded, QueryCancelled, WindowUnsupported,
```

Plus standard Zig errors (`OutOfMemory`, IO errors via `std.Io`, etc.) propagated unchanged.

`ArithmeticOverflow` comes from decimal arithmetic and casts that leave the declared precision, and from `SUM(LARGEINT)` past the i128 range. Integer arithmetic and integer `SUM` up to BIGINT wrap instead of raising it (§3.4).

`DatabaseInUse` means another catalog owns the root's OS lock. `TableBusy` rejects an unsafe same-thread upgrade from a live query lease to destructive DDL. `DatabaseClosed` rejects new operations during close. `DurabilityUncertain` means a file replacement succeeded but parent-directory sync failed. The affected table/catalog is fenced at the persistence boundary, before releasing the mutation lock; queued writers recheck that state after acquiring the table lock. `RecoveryRequired` means that publication or an XA persistence/rollback outcome requires restart recovery; operations are rejected until reopening resolves the journal. XA admission rejects records exceeding its 64 MiB serialized recovery limit (`XaBranchTooLarge`) or invalid XIDs (`XaInvalidXid`, at most 1024 bytes).

Errors propagate to callers. Outside the XA commit protocol, an error is not a blanket guarantee that no effect occurred: durable publication can succeed before later cleanup fails. Retrying non-idempotent writes after an I/O error requires inspecting/recovering the state. Ordinary SQL BEGIN/COMMIT/ROLLBACK currently maintain protocol session status, not a multi-statement undo transaction.

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
| **Implicit type coercion** | DuckDB/StarRocks-style: numeric widening, int → float/double, bool → ints, date → datetime. Exact-match overload selection takes the fast path; coercion is cost-ranked when no exact overload exists. Only when no overload is reachable by widening does an integer argument narrow, saturating, to a narrower integer parameter. StarRocks casts function arguments the same way, so `date_add(d, n + 1)` still resolves although `n + 1` is BIGINT (§3.4). A string literal where a function takes a date or datetime is parsed once at plan time, including `CAST('…' AS DATE)`. A string column converts only by explicit `CAST`, which yields NULL for text that isn't a date. INSERT … SELECT parses text into a DATE/DATETIME column and rejects text that isn't a date. |
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
| ML/RL-driven query tuning | Learn per-query and per-data-shape settings for execution knobs such as scan tile size, chunk rows, route block rows, group bucket count, bucket granularity, flush thresholds, and scheduler/backlog thresholds. Start with offline benchmark traces and cardinality/runtime stats; later allow safe online exploration with guardrails for memory and tail latency. |
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
| General SQL transactions | Staged XA write commits exist (section 8.2); ordinary SQL transactional reads, rollback, and isolation remain unimplemented. |
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

Accepted MySQL sockets enable `TCP_NODELAY` on supported POSIX platforms to avoid
holding a short result tail behind a delayed ACK. Socket-option helpers remain
best effort. On Windows, Zig 0.16 exposes AFD handles without a public socket-option
setter, so these helpers currently leave the OS defaults in place.

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
