# thinDB — Working Guide for Claude

This file tells Claude how to work in this repo. The architecture lives in [DESIGN.md](./DESIGN.md) — read it before making any non-trivial change.

---

## Project context

**thinDB** is a single-node, columnar analytics database written in Zig. v1 is an embedded library only — no SQL, no server, no joins. The goal is raw speed via a small, predictable core. See `DESIGN.md` §1 for goals & non-goals.

Two principles run through every decision in this codebase:
1. **Thin.** If a feature doesn't earn its keep on one machine, it isn't here.
2. **Predictable.** Queries execute in the order the user wrote them. No runtime optimizer, no hidden magic.

---

## Repo layout

```
src/
  api/        public Database, Table, Query builder, .pipe() composition
  engine/     writer thread, memtable, flush, compaction, alter orchestration
  exec/       operators (scan, filter, project, aggregate, sort, limit, sink)
  storage/    segment reader/writer, manifest, encodings, compression, tombstones
  types/      type system, decimal kernel, datetime helpers
  cache/      LRU row-group cache
  util/       allocator helpers, small primitives
tests/
  integration/   end-to-end scenarios
  property/      property-based correctness (v2 target)
bench/
  harness.zig    shared bench helper
  scan_throughput.zig
  insert_rate.zig
build.zig
DESIGN.md
CLAUDE.md
```

When adding code, place it in the subsystem it logically belongs to. New encodings → `storage/encodings/`. New operators → `exec/`. Cross-cutting helpers → `util/`. Don't introduce new top-level directories without a real reason.

---

## Build & test

```
zig build                              # debug build
zig build test                         # all `test` blocks across the tree
zig build test -Dtest-filter="scan"    # subset by name
zig build -Doptimize=ReleaseFast       # production build
zig build bench                        # run benchmarks
```

Target Zig version: **0.16**. If a feature requires a newer Zig, raise it in PR rather than working around it.

---

## Style

### Functional by default, imperative in hot paths

Pure functions where possible:
- Type and schema computation (entirely `comptime`)
- Query plan / builder construction (each method returns a new builder; never mutates the prior one)
- Schema manipulation (`add_column`, `drop_column`, `project_schema`)
- Encoder/decoder helpers that take an output buffer

Imperative is fine in clearly-bounded hot paths:
- Operator inner loops (`next()` mutates batch buffers — that's where speed comes from)
- Column block decode/encode
- Memtable accumulation
- Sort permutation application

If a function is "pure-ish but takes an output buffer to avoid allocating," that's idiomatic and counts as functional in spirit. Don't write closures-and-clones to fake immutability when you'd just be allocating.

### Naming

- `snake_case` for variables, functions, fields, file names
- `PascalCase` for types
- `SCREAMING_SNAKE_CASE` for compile-time constants
- Names favor clarity over brevity. `decode_dictionary_column`, not `ddc`.

### No classes; structs only

Zig doesn't have classes. Don't simulate them. Methods on a struct are fine for fluent APIs (`Query`, builders). Don't build vtables unless there's a measured polymorphism need; the operator pipeline is generic over `anytype`, not virtual.

---

## Memory & allocation

Every function that allocates **takes an `Allocator` parameter**. No globals, no implicit allocations.

### Allocator strategy

| Scope | Allocator |
|---|---|
| Per-query | `std.heap.ArenaAllocator` created at query start, freed at query end. Most short-lived objects live here. |
| Per-database (long-lived) | The allocator passed into `Database.open` by the user. Typically `std.heap.GeneralPurposeAllocator` or `c_allocator`. |
| Tests | `std.testing.allocator` — detects leaks and double-frees, fails the test on any. |

### Allocation patterns

Always pair `try alloc` with `errdefer free` on the next line:

```zig
const buf = try allocator.alloc(u8, n);
errdefer allocator.free(buf);
// ... can now fail safely ...
```

Don't `defer free` early in a function and then return the pointer — the caller takes ownership. Use `errdefer` until the function commits to keeping the allocation; clear it explicitly if needed.

### No hidden allocations in operator inner loops

A `next()` call should not allocate (beyond the batch buffer it returns into, which is typically pooled). Operators preallocate their working buffers at construction time. If an operator needs scratch space, allocate it once in `init`.

---

## Error handling

### Public error set

The library exposes a single error set: `thindb.Error`. New error cases must be added there explicitly and documented in `DESIGN.md` §9.8.

### Style

- `try` to propagate. `catch` only when you can do something meaningful.
- Internal functions: narrow error sets, inferred where it makes the code clearer.
- **No `anyerror` at the library boundary.**
- No `unreachable` for "I think this can't happen but I'm not sure." Use it only when an invariant truly holds; otherwise return an error.

---

## Comptime

Comptime is a first-class tool here, not a fallback:

- **Schemas are comptime types.** An operator's input/output schema flows through Zig's type system. Type mismatches are compile errors.
- **`@compileError`** with a clear message when the type system catches misuse. Better than a runtime error the user has to debug.
- **Specialize at comptime** when there's a real win. E.g., a per-type filter kernel beats a generic one for hot columns.
- **Don't overuse comptime.** If a function has runtime behavior that's easier to read at runtime, leave it at runtime. Comptime that turns into "metaprogramming for its own sake" is a code-review red flag.

---

## SIMD

Vectorized loops use Zig's `@Vector(N, T)` builtin. **No inline assembly.**

- Vector width is platform-dependent. Write generic code (`@Vector(comptime_width, T)`) and let the compiler choose. If you need a specific width, comment why.
- The common pattern: loop over a column in chunks of `@Vector(N, T)`, fall back to a scalar tail for the remainder.
- Verify a hot kernel actually vectorized before claiming a perf improvement (`zig build -Doptimize=ReleaseFast` + read the disassembly or check perf counters).

---

## Slices, pointers, unions

- `[]const T` for immutable views, `[]T` for mutable. **Slices over single-element pointers** when length matters.
- Single-element pointers (`*T`, `*const T`) only when you genuinely need pointer semantics.
- **Tagged unions** (`union(enum)`) for variants: expression nodes, type variants, predicate values, alter operations. No sentinel-encoded variants ("if x == -1 then it's a flag").
- `@ptrCast`, `@intFromPtr`, `@as` of unrelated types: only at storage/IO boundaries. Anywhere else is a smell.

---

## Tests

### Layout

Two options, both fine:
- **Inline `test` blocks** at the bottom of a small source file (idiomatic for short modules).
- **Companion `_test.zig` files** when the test code is large enough to be its own thing.

Default to inline for files under ~300 lines, companion above. Both are picked up by `zig build test`.

### Required patterns

- Every test allocator-passing test uses `std.testing.allocator`. The leak detection is one of the strongest correctness tools we have — use it.
- Use `expectEqual`, `expectEqualSlices`, `expectEqualStrings`, `expectError` from `std.testing`. Don't roll your own.
- Table-driven tests via `inline for` over a tuple — no library required.

### Always end tests + benches with a manual `t.flush()` if you care about persistence

The library provides `Database.runBackgroundFlusher(io, poll_ms, &stop)` — a blocking loop the application can `std.Thread.spawn` to drive periodic flush sweeps. Without spawning that thread (or calling `Database.backgroundFlushSweep()` from the main thread), auto-flush only fires inline on `insert`/`delete`. Tests do NOT spawn the flusher by default. If a test/bench ends without an explicit `t.flush()` and below the auto-flush thresholds, the in-memory rows are silently lost on process exit.

Tests that *test* memtable-only behavior (no flush, no segments) are fine — but those should use small row counts that stay below the thresholds, or explicitly raise the thresholds in their `Config`. Tests that need data on disk must call `t.flush()` explicitly before reading.

Benches: same rule. If a bench measures **memtable-only** speed, **disable** auto-flush in its `Config` (set thresholds to `maxInt`) so the trigger doesn't contaminate the measurement. After taking the measurement, do a manual `t.flush()` so the data on disk is consistent for any later process to inspect.

```zig
test "decimal addition propagates precision correctly" {
    const cases = .{
        .{ .a = .{ .p = 5, .s = 2 }, .b = .{ .p = 5, .s = 2 }, .expected = .{ .p = 6, .s = 2 } },
        .{ .a = .{ .p = 10, .s = 4 }, .b = .{ .p = 5, .s = 2 }, .expected = .{ .p = 11, .s = 4 } },
    };
    inline for (cases) |c| {
        const result = decimal.addType(c.a, c.b);
        try std.testing.expectEqual(c.expected, result);
    }
}
```

### What to test

- **Unit tests** for kernels: encoders, decoders, decimal math, type propagation, predicate evaluation.
- **Integration tests** in `tests/integration/` for end-to-end scenarios: create table → insert → query → assert results. These catch issues that unit tests miss (manifest swaps, flush boundaries, segment readers seeing memtable data).
- **Benchmark regressions** aren't tests but should be tracked. Run `zig build bench` and record baseline numbers in PR descriptions for performance-affecting changes.

### Accuracy reference: DuckDB, not V1

When accuracy-checking query results (ClickBench or otherwise), the ground truth is **DuckDB**, not the V1/legacy engine. V1 is another thinDB engine and can carry the same or its own bugs, so "V2 matches V1" only proves *consistency*, not *correctness*.

**Reference DuckDB databases** (table `hits`, matches the thinDB `clickbench`/`clickbench_full` data under `.clickbench-db/`):
- `bench/clickbench/duckdb/hits_full.duckdb` — full **100M** (≈99,997,497 rows), the ground truth for `clickbench_full__public`.
- `bench/clickbench/duckdb/hits.duckdb` — **5M** sample.

Query directly, e.g. `duckdb bench/clickbench/duckdb/hits_full.duckdb "SELECT ..."`. (The empty `.clickbench-db/hits.duckdb` is a stub — ignore it.) Note that `ORDER BY ... LIMIT/OFFSET` over tied sort keys is order-nondeterministic across engines — compare the canonicalized result set (and the aggregate-value multiset), not row order, for those.

---

## Don'ts

- **No** comments explaining WHAT code does. Names should carry that. Comments only for non-obvious WHY: a hidden constraint, a workaround, an invariant a reader couldn't infer from the code.
- **No** scaffolding left in committed code: no `// TODO: remove`, no `unused_var: i32`, no half-written stubs, no commented-out blocks. Clean as you go.
- **No** new dependencies without discussion. The whole stdlib is fair game; everything else needs a real reason.
- **No** mutation of "logical" values: schemas, query plans, immutable segments. Always return new.
- **No** global state. Multiple `Database` instances must coexist cleanly in one process (tests rely on this).
- **No** hidden control flow: no exceptions, no operator overloading, no implicit type coercions.
- **No** `@ptrCast` / `@intFromPtr` outside `storage/` and `util/`.
- **No** runtime query optimization. Pre-execution rewrites (constant folding, predicate normalization) are fine; reordering joins or picking indexes at execution time is not.

---

## When working on this codebase

- **Read `DESIGN.md` before non-trivial work.** If a decision in the design doc would have to change for your work, surface that — don't quietly diverge.
- **Prefer editing existing files over creating new ones.** Every new file is one more thing to navigate.
- **Don't add features beyond what the task requires.** Three similar lines are better than a premature abstraction.
- **For UI/CLI changes (none in v1, but later)**: actually run the change end-to-end before declaring done. Tests verify code correctness, not feature correctness.
- **Commit messages**: short, in the imperative ("Add dictionary encoder", not "Added dictionary encoder"). Body explains WHY if the diff doesn't already.
- **Git integration: merge, don't rebase.** When the remote is ahead, use `git pull` (creating a merge commit) rather than `git pull --rebase`. The owner prefers a merge history over rebased history — fewer surprises across collaborators, simpler conflict resolution in one pass, and the merge commit is a useful anchor for "here's where the two threads of work joined." Never force-push unless explicitly asked.

---

## References

- [DESIGN.md](./DESIGN.md) — architecture spec
- [Zig stdlib docs](https://ziglang.org/documentation/master/std/)
- [Zig language reference](https://ziglang.org/documentation/master/)
