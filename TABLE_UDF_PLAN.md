# Table-Valued UDFs — design research

Goal: make complex procedural logic a first-class citizen of thinDB. A user
should be able to write a small Zig function, hand it to the server (source
or compiled), and call it in SQL as a table — so a 100-CTE stack collapses
into a handful of scaffolding CTEs plus one or two functions that do the
loops, carried state, and per-entity walks that SQL expresses so painfully.

Status: DESIGN RESEARCH (2026-07-05). Nothing here is implemented.

---

## 1. What exists in thinDB today

- **Scalar + aggregate UDFs** (`src/udf.zig`): trusted in-process Zig function
  pointers, vectorized signatures (`args: []const ColumnView`, out
  `*ColumnStore`), registered programmatically through the embedded API
  (`UdfRegistry.registerScalar/registerAggregate`). No SQL-level
  `CREATE FUNCTION`, no dynamic loading, embedded-host only. The silo can
  host UDAFs in its parallel fold (task #94).
- **`file_scan`** IR leaf: an existing "table-valued source" precedent — the
  compile/EXPLAIN/serialize plumbing for a leaf that produces rows from
  something that isn't a table already exists.
- **The SEPARABLE/window partition machinery**: sampling, key-range slicing,
  partition-grouped buffers, claiming pools, core-pinned workers, the strong
  concat-only contract. This is, almost verbatim, the runtime a
  partition-aware table UDF needs — built for a different feature.

## 2. Lay of the land — how other systems do this

| System | Mechanism | Ergonomics | Perf | Isolation | Lesson for us |
|---|---|---|---|---|---|
| Postgres | C extensions + SRF protocol; PL/pgSQL | C: painful (ABI, superuser install). PL/pgSQL: easy | C fast; PL/pgSQL row-at-a-time slow | none / none | Don't make users learn an extension ABI; don't invent an interpreted proc language |
| SQL Server | inline TVFs (parameterized views) vs multi-statement TVFs; CLR | inline TVF: great | inline TVF: great (plan-inlined); MSTVF: notoriously slow opaque blob | — | Two distinct things hide under "TVF": **SQL macros** (inline, cheap) and **procedural functions** (opaque). Offer both, keep them separate |
| DuckDB | C++ extension table functions (bind/init/execute chunk protocol) | writing an extension = real C++ project | excellent (vectorized) | none | The *chunk-protocol operator shape* is right; the packaging is too heavy for end users |
| SQLite | virtual tables, app-registered functions | in-process registration = trivial | fine | none | The embedded-registration ergonomics thinDB already copied for scalars |
| ClickHouse | executable UDFs (external process over stdin/stdout) | easy (any language) | serialization tax | process boundary | A cheap isolation trick worth remembering, not the primary path |
| Snowflake | **UDTFs with PARTITION BY / ORDER BY**: per-partition process() with carried state (JS/Python/Java) | very good | vectorized variant exists | sandboxed runtimes | **The model that fits our workload exactly** — "per customer, walk months in order, emit rows" is a partition-aware UDTF |
| BigQuery | JS UDFs (V8 sandbox), SQL table functions | good | meh | sandboxed | SQL table functions = parameterized views again |
| SingleStore / libSQL / Scylla / Redpanda | **WASM UDFs/TVFs** | compile any language to wasm32-wasi | near-native minus marshaling | real sandbox | The modern answer to "accept code from users safely" — the future safe tier |
| MonetDB | embedded Python over NumPy arrays | superb for data people | vectorized | none | Columnar-arrays-in / columnar-arrays-out is the right user-visible data shape |
| kdb+/q | the language *is* the query engine | — | — | — | Philosophical north star: loops and carried state as first-class, not bolted on |

Consensus takeaways:
1. **Nobody is happy with interpreted stored-procedure languages.** PL/pgSQL
   and T-SQL procs exist for compatibility, run row-at-a-time, and every
   vendor's modern answer is "embed a real language" (Python/JS/WASM).
   We should NOT design a proc DSL.
2. **Partition-aware UDTF is the shape that kills CTE stacks.** Snowflake's
   `initialize / process(partition) / end_partition` maps 1:1 onto
   "rollforward walk per customer".
3. **Inline SQL table functions (parameterized views) are a separate, cheap
   win** — pure macro expansion at compile time, no runtime at all.
4. **WASM is the only credible safe-code-submission story**; native code is
   always a trust decision, however it is packaged.

## 3. Design axes and recommendations

### 3.1 Input model — what the function sees

Three candidates:

- (a) **Row-stream** (SQLite/Postgres SRF): `next()`-driven. Maximum
  generality, worst ergonomics for carried state across an entity.
- (b) **Partition-aware** (Snowflake): the call site declares
  `PARTITION BY key ORDER BY cols`; the engine hands the function ONE
  partition at a time as fully-materialized column arrays, ordered; the
  function emits any number of output rows per partition. State lives in
  local variables across the partition loop — no window-function
  contortions, no 15-CTE state-carry chains.
- (c) **Whole-input materialized**: one call, everything in memory.

**Recommendation: (b) as the flagship, (c) as the degenerate case**
(a partition-less call = one partition). (b) is:
- exactly the wayroll shape (per-customer walk over ordered months);
- embarrassingly parallel — partitions are independent, so the existing
  partition-bucket / claiming-pool / core-lease machinery runs N partitions
  concurrently for free;
- memory-bounded by the largest partition, not the input (stream partitions
  through, materialize only within);
- the same strong contract as SEPARABLE BY (concat-only combine), so all
  the correctness rules of that feature carry over verbatim.

Streaming across partitions, materialized within a partition. No fully
streaming row protocol in v1.

### 3.2 The user-facing API shape

Columnar in / row-writer out, with comptime-declared schemas so the SDK does
all marshaling and the compiler catches type mismatches:

```zig
// user submits exactly this — nothing else
pub const input = .{
    .month = .date, .amount = .decimal64, .planId = .string,
};
pub const output = .{
    .month = .date, .upDown = .string, .amount = .decimal64, .lastAmount = .decimal64,
};

pub fn process(ctx: *tdb.Ctx, p: tdb.Partition(input), out: *tdb.Writer(output)) !void {
    var last: i64 = 0;
    for (0..p.len) |i| {
        const amt = p.col(.amount)[i];
        try out.row(.{
            .month = p.col(.month)[i],
            .upDown = if (amt > last) "up" else "down",
            .amount = amt,
            .lastAmount = last,
        });
        last = amt;
    }
}
```

- `tdb.Partition(input)` = typed column slices (`p.col(.amount)` is
  `[]const i64`) + partition key values + `p.len`. Zero-copy views into the
  engine's partition-grouped buffers.
- `tdb.Writer(output)` = typed row/column appenders writing straight into
  batch ColumnStores. Also a columnar bulk path (`out.rows(n)` returning
  mutable slices) for hot functions.
- `ctx` carries: arena allocator (auto-freed per partition — leaks
  impossible by construction), `ctx.shouldStop()` cancellation check,
  memory-accounted allocator for big scratch.
- Comptime: the SDK wrapper `@import`s the user file, reflects `input`/
  `output`, generates the ColumnView→slice marshaling and the vtable export.
  Wrong types are compile errors with `@compileError` messages, per house
  style.

SQL call site (Snowflake-flavored):

```sql
SELECT r.*
FROM TABLE(rollforward_walk(
       SELECT month, amount, planId FROM invoice_base WHERE projectId = 1000073
     ) PARTITION BY customerNumberLC ORDER BY month) r
```

Scalar args after the query arg for parameterization
(`rollforward_walk(<query>, '2026-07-01', TRUE)`).

### 3.3 Submission & compilation pipeline

Three tiers, built in this order:

- **Tier 0 — embedded registration** (exists for scalar/agg; extend to TVF):
  host process registers a `TableUdf` descriptor. Zero new machinery beyond
  the operator itself. This is how WE iterate on the operator before any
  loading exists.

- **Tier 1 — `CREATE FUNCTION ... LANGUAGE zig AS $$ <source> $$`**
  (server-side compile). The server:
  1. writes the source next to a generated `wrapper.zig` (root file — the
     user file is an imported module, so it cannot own `main`/exports);
  2. shells out to the already-present `zig` toolchain:
     `zig build-lib wrapper.zig -dynamic -OReleaseFast` (+ a vendored
     `tdb` SDK module pinned to the server's ABI version);
  3. `LoadLibrary`/`dlopen`s the result, resolves one exported vtable
     symbol (`thindb_tvf_v1`), registers it;
  4. persists the SOURCE (not the binary) in the catalog, keyed by
     content hash — recompiled lazily on server start, cached by hash.
     The database directory stays self-contained and cross-platform.
  Zig is the ideal language for this tier: single-binary toolchain already
  on every thinDB box, fast compiles (~1–3 s for a small lib), no runtime,
  cross-compilation built in, and the comptime SDK gives real type checking.

- **Tier 2 — DLL submission** (`CREATE FUNCTION ... USING 'lib.dll'`):
  same vtable, skip the compile. For users with their own build pipelines.
  Falls out of Tier 1 nearly free.

- **Tier 3 (later) — WASM safe tier**: same SDK compiled to wasm32-wasi,
  executed in a runtime. The only path that makes "accept code from
  untrusted users" honest. Costs: a new dependency (runtime) and a
  marshaling copy at the boundary. Park until there's demand.

### 3.4 Guardrails — honest framing

Native code (Tiers 0–2) is **trusted**: no compiler flag makes a `.dll`
safe. What we CAN do is prevent accidents and contain damage:

- the generated wrapper owns the root file and the export surface; user
  code is a leaf module;
- per-partition arena ⇒ leaks are structurally impossible; big allocations
  go through a query-accountant-wrapped allocator ⇒ budget enforcement and
  admission control apply;
- `ctx.shouldStop()` polled by the SDK's writer appends ⇒ runaway loops are
  cancellable at every output row;
- **validation run on CREATE**: compile in Debug, execute against a
  synthetic partition (derived from the declared input schema) under the
  testing allocator + timeout in a THROWAWAY SUBPROCESS. Panics, leaks,
  UB traps, and hangs reject the function with the captured stderr as the
  SQL error message. Production calls then run in-process at ReleaseFast.
- catalog records provenance (source hash, who, when) for auditability.

Security boundary = deployment (who can run CREATE FUNCTION), enforced by
the same auth story as DDL generally. Say this plainly in the docs.

### 3.5 The cheap sibling: SQL inline table functions

Independent of everything above, `CREATE FUNCTION f(a INT) RETURNS TABLE AS
(SELECT ...)` = **parameterized view, expanded at compile time** into the IR
(constant-fold the arguments, splice the subtree). No runtime, no loading,
maybe two days of work, and it already deduplicates the copy-pasted CTE
families in the wayroll stacks (the five near-identical
`rollforward_with_*` chains are one parameterized function). SQL Server's
inline-TVF experience says users adore this. Recommend doing it FIRST — it
also forces the parser/catalog work (CREATE FUNCTION statement, persistence)
that Tier 1 reuses.

### 3.6 Execution integration

- New IR node `table_fn` (sibling of `file_scan`): function name, scalar
  args, input subquery IR, partition/order spec.
- Compile: input subquery compiles normally (full pushdown/parallel
  machinery — it's just a block); the TVF operator sits above it and
  REUSES the window/SEPARABLE partition-grouping path (sort or
  partition-bucket by key, grouped buffers) and the claiming pool: DOP
  workers each claim partition ranges and run `process()` concurrently,
  appending to per-worker output sinks, concat on emit (strong contract).
- Stats: output row estimate = declared multiplier or input-row default;
  `upper_rows` unknown-safe like file_scan.
- EXPLAIN shows `TableFn(name) [partition by k, order by o]`.
- The operator is engine-neutral exec (like udfGroupBy) — usable from both
  the embedded API and SQL.

### 3.7 Explicitly rejected

- **A stored-procedure DSL** (interpreter): row-at-a-time perf, a whole
  language to design/parse/maintain, and the industry has already walked
  away from this. Zig-with-comptime-SDK gives strictly more power with
  less surface.
- **External-process UDFs** (ClickHouse style) as the primary path:
  serialization tax on 100M-row inputs contradicts the whole engine design.
  (Fine trick for the Tier-1 VALIDATION run, though.)
- **JVM/embedded-interpreter runtimes**: dependency policy, cold-start,
  marshaling. WASM strictly dominates as the eventual safe tier.

## 4. Suggested phasing

1. **P0 — SQL inline table functions** (parameterized views): parser +
   catalog persistence + IR splice. Immediate CTE-dedup value; builds the
   CREATE FUNCTION plumbing.
2. **P1 — TVF operator + embedded registration**: `table_fn` IR node, the
   partition-grouped parallel operator over the existing machinery,
   `TableUdf` registry entry. Prove it by porting ONE wayroll chain (e.g.
   the gap-fill walk) and A/B-ing vs the CTE version — values first, then
   wall-clock.
3. **P2 — `LANGUAGE zig` server-side compile + DLL load**: wrapper
   generation, zig build-lib, dlopen, source-in-catalog, hash cache,
   validation-subprocess battery.
4. **P3 — polish**: columnar bulk writer, per-partition arena pooling,
   partition-parallel stats, EXPLAIN costs.
5. **P4 (parked) — WASM tier** when untrusted submission matters.

## 5. Open questions

- Call-site grammar: `TABLE(f(<subquery>) PARTITION BY ...)` vs
  `f(<subquery>) OVER (PARTITION BY ...)` vs lateral-style. Pick after
  parser spike; Snowflake's TABLE(...) form is the least ambiguous.
- Multiple input tables per function (co-partitioned pairs — e.g. invoices
  + estimates per customer)? Snowflake doesn't; we eventually want it for
  the rollforward (v2: `p.table(.invoices)` / `p.table(.estimates)`
  co-grouped by the same key). Design the Partition API so a second table
  is additive.
- Deterministic/volatile declaration and its interaction with caching.
- ABI versioning for Tier-2 DLLs (`thindb_tvf_v1` symbol + struct version
  field; reject mismatches at load).
- Windows vs POSIX dynamic loading differences (LoadLibrary vs dlopen —
  both trivial; unloading is the hard part: never unload while a query
  runs; refcount or leak-until-restart like most engines).
