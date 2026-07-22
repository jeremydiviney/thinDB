# Narrow Column Encoding — Implementation Plan

Status: **planning / Phase 1 not started.** This is the working plan for adding
range/cardinality-aware narrow column representations to thinDB. See
[DESIGN.md](./DESIGN.md) for the architecture it plugs into.

---

## 1. Motivation

Two related techniques, one goal: **make values move and compare as the smallest
representation their actual data allows**, derived from stats we already keep
(per-segment min/max + HLL NDV).

- **Frame-of-Reference (FOR)** for numeric/temporal columns: store a per-segment
  `base` and the values as `value − base` in the narrowest aligned type. A
  `BIGINT` whose values live in `[1_000_000, 1_000_200]` becomes a `u8` column.
- **Segment-local string dictionary** for low-cardinality short string columns:
  store the distinct strings once + a narrow `code` per row.

The payoff shows up in four places at once:

1. **On-disk size** — smaller column blocks.
2. **Cache footprint** — the decompressed form stays narrow → more columns
   resident → fewer re-decompresses (a known large cost; see the cache-size
   finding in the gap notes).
3. **Hash keys for GROUP BY / DISTINCT / join** — a narrow key is a smaller
   slot = less probe bandwidth. The probe is bandwidth-bound, so this is the one
   remaining lever for the high-card aggregate queries.
4. **Predicate / function work on dict columns** — evaluate a `LIKE`/regex/scalar
   function once per *distinct value* (dict-bounded), then a per-row code lookup.
   Plus the dict is an **exact cardinality oracle** (exact NDV, and exact
   post-predicate NDV by scanning the dict).

FOR and dict are siblings: FOR for contiguous-ish ranges (`base + offset`),
dict for arbitrary low-card sets (`0..k` codes). Both reduce to "store a small
int, reconstruct via base+delta or a dict lookup."

### Honest scope

This is primarily a **general-engine + scaling/compression** investment. The
remaining ClickBench gaps (Q17/Q25 high-card strings, Q08 high-card int) are
mostly *outside* the FOR/dict sweet spot, so we do **not** bank on large
ClickBench movement. Phase 1's A/B quantifies what, if anything, moves. The
durable payoff is compression, cache footprint, low-card categorical speed, and
exact cardinality.

---

## 2. Locked decisions

- **Numeric: byte-width narrowing first** (round to u8/u16/u32 — aligned, no
  unpack CPU, SIMD-friendly). True **bit-packing is deferred** (Phase 5) until a
  bandwidth-vs-unpack microbench proves it beats byte-narrow.
- **Strings: 2 tiers only** — segment-local dictionary **or** raw. No persistent
  global/shared dictionary (its cross-segment benefit is recovered at query time
  by stitching; see §4).
- **Dict gate (per column, per segment, at flush):** encode iff
  - `avg value length ≤ 256 bytes` (exclude obviously-huge blobs; long-but-low-card
    URLs still qualify), **and**
  - `NDV ≤ 65_536` (2-byte codes; ≤8 KB pushdown bitset fits L1/L2; cheap stitch)
    — *abandon the dict mid-build and write raw if NDV crosses this*, **and**
  - `NDV ≤ N/2` (real repetition; else the dict ≈ the data).

  May be revisited upward toward u32 codes (~2²⁴) later **if measured** to still
  beat raw-string grouping.
- **NULLs stay in the validity bitmap** — orthogonal to FOR/dict, no sentinel
  code. NULL and `''` are distinguished only by the validity bit.
- **The memtable is always raw** — encoding is a flush/segment-only concern, so
  inserts never consult a dictionary or encode. No write-path cost.
- **Order:** FOR is inherently order-preserving (delta order = value order); the
  dict is sorted so code order = string order under the default byte collation.
  Any function over the column (`LOWER`, etc.) opts out → decode + apply.

---

## 3. Mixed-segment correctness (the invariants)

Per-segment encoding is decided independently at each flush and recorded in that
segment's metadata (`raw | FOR(base,width) | dict(...)`). A query reads any mix:

- **Crossing the cap is never an error.** A column that drifts high-card just
  produces newer **raw** segments; older **coded** segments stay valid forever
  (immutable + self-describing).
- **Per-segment + global NDV cap (64K)** is enforced at every segment write
  (flush *and* compaction): a string column is dict-eligible only when BOTH this
  segment's NDV and the global NDV (HLL merged across all coexisting segments)
  are ≤ 64K. The per-block `tryEncodeDict` then applies within eligible columns;
  high-card columns stay fully raw. (Mid-build abandon remains as the
  estimate-error safety net near the boundary.)
- **Query-merged cardinality** can exceed 64K even if each segment is under it →
  the **query-global code width floats** (u8→u16→u32). On-disk codes stay ≤u16;
  the query-scoped global code is whatever the merge needs.
- **Compaction re-tiers**: merged output re-decides encoding per the gates
  (decode inputs, re-encode FOR/dict/raw), off the insert path — self-heals
  mixed regimes over time.

---

## 4. Query-time dictionary stitching

Per-segment codes are local (segment A's `5` ≠ segment B's `5`). Cross-segment
GROUP BY/DISTINCT unifies through **one query-scoped `string → global_code`
map**, built at scan start:

1. Merge each participating segment's dict into the map (cost ∝ *distinct
   values*, not rows → microseconds for low-card), emitting a per-segment
   `local → global` LUT.
2. Per row: coded segment → `LUT[local_code]` (one indexed load); raw
   segment/memtable → hash the row string into the same map (normal cost).
3. Group/distinct on the narrow global code; width chosen from merged count.
4. ORDER BY: assign global codes in sorted order (sort the small merged dict) →
   code sort; else decode at output.
5. **Dict predicate pushdown:** evaluate the column's predicate against each
   segment's dict (`|dict|` evals, not `|rows|`) → matches-bitset → per-row
   bit-test (filter and translate fuse). Match count is irrelevant to per-row
   cost; the win is `D ≪ N`. Raw segments fall back to per-row evaluation.
6. **Exact cardinality:** dict-survivor count after a predicate → exact/upper
   post-filter NDV → feeds group-table sizing + predicate ordering.

---

## 5. Phased steps

Rules: reversible-first (in-memory before format changes); measure each
(microbench + ClickBench A/B + `zig build test` green); commit clean wins,
revert neutral/regressions.

### Phase 1 — slot-as-gid + FOR narrow group table *(in-memory, no format change)*
**Why this shape (decided 2026-05-25):** the group-table slot floor is 8 bytes
because the gid lives in the slot (`tier32 = {key32, gid32}`). Pure FOR can only
shrink tier96/128→tier32, which ClickBench's keys don't benefit from (already
tier32 i32, or un-shrinkable huge-range i64, or strings). The real lever is
**slot-as-gid**: drop the gid from the slot (gid = slot position), so a u64 key
is an 8-byte slot (vs tier96 16B → 2× on the i64-key GROUP BYs like Q15/Q19),
and a FOR-narrowed key is a 1/2/4-byte slot. FOR is *coupled* to slot-as-gid:
a gid-less slot needs an empty marker, and FOR provides it free by reserving
`range_max+1` as the EMPTY sentinel.

Mirrors the proven `CountSlotTable` pattern (#290): accumulate in the narrow
slot table, then **lower occupied slots into the existing dense
`gkeys_int`/`gstate` at emit** (`lowerCountSlot`), so the emit / top-k / ORDER BY
paths run completely unchanged. `gstate` is capacity-sized and indexed by slot
during accumulate; lowering compacts it to dense gids.

- [x] 1.2 `InlineSlotTable(KeyW, StateT)` in `group_table.zig`: generic slot = `{key:KeyW, state:StateT}`, gid = slot position, EMPTY = `maxInt(KeyW)`, prefetch-pipelined, grow carries state. Standalone + unit-tested. (commit `be5eb8d`)
- [x] 1.3 + 1.4 Aggregate integration (commit `50cce36`): `planInlineFor` eligibility (one non-nullable int key ≤64b + one SUM/MIN/MAX over int ≤64b, FOR range from proven stats with sentinel headroom, SUM-overflow guarded by proven `n·[lo,hi]` fitting i64, slot ≤16B), FOR-normalize at probe, inline-state fold, `lowerInlineFor` → dense `gkeys_int`/`gstate` at emit (mirrors `lowerCountSlot`); everything else falls back to the canonical tier path unchanged. (1.1 stat rollup folded in via `column_stats`.)
- [x] 1.5/1.6 591 tests pass (brute-force SUM/MIN/MAX + fallback cases); ClickBench neutral 43/43 — **no canonical query has the single-int-key+single-SUM/MIN/MAX shape**, so this is a general-engine win, not a ClickBench mover (as anticipated). The pure slot-as-gid-with-separate-gstate (for >16B state) is intentionally NOT built; inline-state covers the fits-in-slot case (see the multi-agg note below).

**Multi-agg note:** splitting many aggregates across N inline tables is a
DEAD END — N tables = N probes/misses per row, defeating the single-miss
win. For many/wide aggs the right shape is ONE key→gid table + ONE
contiguous per-gid `gstate` (2 misses, independent of agg count) — the
existing canonical path; the inline-state slot only helps the few-agg
fits-in-one-slot case. The real many-agg lever is vectorized batched agg
kernels (#278), not table splitting.

**Blast radius / correctness:** shared int-key GROUP BY path. Watch emit order
(lowering keeps dense-gid order — verify against tests), `gstate` capacity
sizing, and the combined-distinct gid coupling (its pack uses gid; if a query
mixes slot-as-gid grouping with COUNT(DISTINCT), the lowering must hand it dense
gids). The runner doesn't validate values → `zig build test` is the gate.

### Phase 2 — On-disk FOR for numeric/temporal columns *(format change)* — DONE
- [x] 2A (commit `107a90f`) format v8→v9: `Encoding {raw,for}` in a reserved block-header byte (old all-raw blocks byte-identical; strict v9 reader, v8 data re-imported — no back-compat read at this stage). FOR payload `[base:i128][width][deltas]` after the null bitmap. Writer FOR-encodes int-family columns when the delta width is strictly narrower AND the body shrinks (i128-safe gate; all-null/single-distinct/un-narrowable → raw). Reader expands base+delta to native (all consumers unchanged); borrow path bails FOR→owned-decode; `pub forBlockOf` exposes `{base,width,codes}`. Round-trip tests: negative base, min/max boundaries, single-distinct, NULLs, stays-raw, multi-segment.
- [x] 2B (commit `ae1d46b`) FOR-aware fused-scan filter: `translateForLeaf` maps `col OP C` into the FOR domain once (per-op empty/all/narrow-compare boundaries), SIMD-compares the narrow codes from cache (`forCompareInto`), ANDs validity+tombstones, expands only survivors. Falls back for unhandled shapes / raw blocks. **Bench: FOR 36.2 vs raw 73.5 ms/pass = 2.03× at larger-than-cache scale** (16MB pinned cache, raw spills / FOR fits), identical match count. 1003 tests; all 6 ops × boundary constants × widths × nullable × multi-segment × stays-raw brute-forced.
- [x] 2C (commit `ca75dca`) FOR always-on regression eliminated. Always-on FOR initially regressed ClickBench ~0.16s — a microbench proved the FOR SIMD decode + narrow filter BEAT raw in-cache, so the cost was structural: FOR-encoded int columns lost the scan's zero-copy fast paths. Fixes: (a) vectorized the FOR expand — dropped per-row i128 for native-width SIMD widen+add (`expandWidth`) with unaligned vector loads, in both full-column decode and survivor materialization (`forExpandSurvivors`), both now beat raw `memcpy`; (b) **per-column borrow** — `tryBorrowViews` no longer bails the whole row group when one column is FOR; raw/string columns keep zero-copy borrow, a FOR column is expanded ONCE into an owned buffer carried by `BorrowedBlock.expanded` (freed on release); (c) **multi-leaf FOR filter** — the fused filter handles an AND of comparison leaves, each FOR leaf evaluated narrow in the cached block, raw leaves over the borrowed view (Q36/Q39 stay scan-cheap). Result: ClickBench 4.90–4.94s at the 6-segment steady state, at/below raw-v9 (4.94) and the pre-FOR ~4.93; FOR now ≤ raw in-cache while keeping the 2.03× larger-than-cache filter win. 43/43 ClickBench, 1003+ tests green, always-on.
- [ ] 2.5 SUM base-correction (`SUM = N·base + SUM(delta)`) — deferred; the agg path reads native (FOR expands for non-filter consumers), so SUM is already correct, just not yet narrow-accumulated. Pick up with multi-agg / accumulator-narrowing work.

### Phase 3 — Segment-local string dictionary *(format change)* — DONE (storage increment; no query speedup yet, by design)
- [x] 3.1 Format: `Encoding.dict = 2` (`format.zig`). Post-bitmap payload `[ndv:u32][code_width:u8][3 pad][dict_offsets:(k+1)×u32][dict_bytes (SORTED)][codes:n×code_width]`. Codes are byte-narrow (1/2/4 by k). `blockEncoding` bound widened to accept dict.
- [x] 3.2 Writer `tryEncodeDict` (`segment_writer.zig`), tried after FOR (mutually exclusive by type), before raw. Gate: avg-len ≤ 256 (upfront), NDV ≤ 65536 with **mid-build abandon**, NDV ≤ n/2, body-shrinks vs the raw string block. Builds an insertion-order distinct map, sorts the distinct values, remaps codes to sorted order, emits the layout; else leaves scratch untouched → raw.
- [x] 3.3 Reader `decodeDictColumn` (`segment_reader.zig`) rebuilds the IDENTICAL raw-string `OwnedColumn` (every consumer unchanged); wired into `decodeColumnPayload` (now `pub`). `dictBlockOf` + `DictBlock{dictValue,rowCode}` expose codes+dict (the seam Phase 4 consumes). Scan borrow path (`scan.zig tryBorrowViews`) generalized from `== .for_` to `!= .raw`: a dict (or FOR) column expands ONCE into `BorrowedBlock.expanded`, raw columns stay zero-copy — one narrow column never bails the whole row group.
- [x] 3.4 Tests: `storage.zig` unit tests (gate fires dict for low-card / raw for high-card; NULL-vs-empty under codes; blob avg-len>256 → raw; NDV>65536 mid-build abandon → raw; sorted-dict invariant via `dictBlockOf`). `tests/integration/dict_encoding_test.zig` public-API round-trip (low+high-card+nullable/empty, GROUP BY over a dict column, single multi-row-group + multi-segment). **1011/1011 tests pass.**
- [x] 3.5 **Segment+global eligibility gate** (the §2/§3 "per column, per segment"
  decision, which the original per-block gate diverged from): `writeSegment` takes
  `prior_sketches` (coexisting segments' HLL), computes its own sketch up front, and
  marks a string column dict-eligible only when BOTH segment-local NDV and the merged
  global NDV are ≤ 64K. Flush + compaction pass all current manifest sketches (the
  merged output's sketch covers compaction inputs, so passing all is correct); ALTER
  passes `&.{}` (schema may add/drop columns → old sketches misalign; segment-local
  only, no regression). **Result on 5M ClickBench: high-card URL now fully raw on the
  compacted layout — Q33 dict-decode 158ms→0, Scan 180→56ms, Q33 411→272ms
  back-to-back. 1018 tests green.** *(Lesson: must rebuild `thindb-server` — not just
  the loader — for compaction to use a writer change; a stale server's old-gate
  compactor re-encoded URL.)*

### Phase 4 — Dictionary-aware execution *(the query wins)*
**Context:** Phase 3 dict storage is query-NEUTRAL but in fact regressed ClickBench
~2× (every dict string column re-expands to strings on each scan, where raw was
zero-copy). Phase 4 converts that into a win by operating on codes. Building it
incrementally, test+bench each step. **NOTE on benching:** this box's suite total
drifts 15-20% with thermal/load — trust only old-vs-new back-to-back deltas, not
absolute cross-run numbers (see [[feedback-incremental-bench]]).
- [x] 4.4 Dict predicate pushdown (commit `4266bf4`): `=`/`<>`/`LIKE` on a dict
  block → per-distinct eval into a matched-codes bitset → per-row code test, no
  expansion; routes through the guided filter so only survivors materialize.
  Recovered the URL-LIKE full-expansion (Q20). Comparison `<> ''` ~neutral (those
  cols are also grouped → projection expansion dominates). Plus string range
  comparisons enabled as a side correctness win (commit `19fbc11`).
- [x] 4.1 Query-time stitch (commit `33e2f88`): `src/exec/global_dict.zig`
  `GlobalDict` — intern→stable global code, `buildLut` per-segment local→global,
  owns byte copies, `decode` for emit. Foundation only (no consumer yet).
- [ ] 4.2 Group/distinct on global codes — **DECISION: Option A** (general
  coded-column through the batch interface; the contained Option B fused path was
  rejected — owner: "shooting for the moon"). The executor is built on
  materialized columns, so this is a core-type change. Decompose, green + tested
  + back-to-back-benched + **profiled** at each sub-step:
  **SEAMS DONE (all committed/pushed, 1017/1017 green) — fresh session implements the producer/consumer/gate against these:**
  - [x] **A1** `CodedColumn{codes:[]u32, dict:*GlobalDict}` + `materialize`
    choke point (`global_dict.zig`, commit `0c85a51`). NOT a `ValueView` variant
    — `ValueView` is `union(TypeTag)`, a variant pollutes the core type enum.
    Coded rides as a **batch sidecar**: `Batch.coded: ?[]const ?CodedColumn`
    (`9c3fce4`). `CompileCtx.queryGlobalDict()` lifecycle (`3a8248b`).
  - [x] **Profiling** (`3414935`): `dict-decode`/`for-decode`/`dict-filter` phase
    timers in the `[oprof]` dump. **Diagnostic: dict-decode = 60–97% of Scan time
    on string-heavy queries** — A2/A3 targets exactly this.
  - [ ] **A2 producer** — scan emits codes for the gated key column. Hook in
    `scan.zig next()` unfiltered decode loop (~L689): for `out_phys[code_col]`,
    instead of `decodeColumnMaybeCached`, call a new `decodeColumnAsCodes(seg,
    rg_idx, phys, rg_count, gdict)` → fills a scan-owned `code_buf: []u32` (dict
    block → `buildLut` + per-row `lut[rowCode(r)]`; raw block → intern each row's
    string per-row), set `self.decoded[code_col]` to a CHEAP empty-string
    placeholder OwnedColumn, set `coded_slots[code_col] = .{codes, gdict}`, emit
    `Batch{..., .coded = coded_slots}`. **HAZARDS found while scoping:**
    (1) `applyTombsIfAny` (~L718) compacts `self.decoded` survivors — the codes
    must compact in lockstep OR gate coding OFF when the rg has tombstones.
    (2) memtable path (~L732) has no dict block → intern per-row (or gate off;
    ClickBench memtable is empty post-flush). (3) the filtered path
    (`nextFiltered`/`materializeSurvivors`) is a SEPARATE emit site — first cut
    can gate to unfiltered only (no WHERE). (4) Scan fields: `code_col: ?usize`,
    `gdict: ?*exec.GlobalDict`, `code_buf`, `coded_slots` — free in deinit.
  - [x] **A2 producer DONE** (commit `0c62cb0`, dormant): `scan.zig` `code_col`/
    `gdict` fields + `fillKeyCodes` (dict→LUT translate / raw→intern) + empty
    placeholder + `Batch.coded` sidecar, on the unfiltered no-tombstone segment
    path. Gated off until the compile gate sets `code_col`.
  - [ ] **A3 consumer** — REUSE the int-key table (confirmed the hook):
    `accumulateBatchIntT` packs keys via `packIntKey(layout, batch, gci, row)`
    (aggregate.zig ~L1390). Plan: at agg setup, when the gate marks the key coded,
    FORCE `int_layout = bits32` (single u32 key) so `accumulateBatch` dispatch
    (~L945) routes to `accumulateBatchIntT`; make `packIntKey` (and `needsGrow`
    sizing) read `batch.coded[code_idx].codes[row]` as the u32 key instead of an
    int column; `gkeys_int[gid]` then holds the global code. At emit
    (`emitGroupKey` ~L1558, the `single_str_key`/int branch) decode the code via
    `GlobalDict.decode(code)` into the output string column. **This moves the
    bench.** Add an `agg.coded_key: ?struct{ col_idx, dict }` field set by the gate.
  - [ ] **Gate** (`local.zig` compile, the `.group_by` lowering ~L1835/L2363):
    fire only when single string group key, key NOT referenced by WHERE/any agg
    arg, child is a direct scan, no tombstones on the path. Set scan.code_col +
    gdict (from `ctx.queryGlobalDict()`) and mark the aggregate key coded. Decline
    → unchanged path. Covers Q33/34 first (single-key, no WHERE); SearchPhrase
    (Q12-17/21/22) need the filtered-path producer + multi-key later.
  - [x] **A3 consumer + gate DONE** (commit `13c6669`): two no-op-default VTable
    methods (`setDictCodeColumn`/`setCodedKey`, `@hasDecl`-gated → no operator
    ripple); compile gate in `compileOp .group_by` (hash path) fires for a single
    string key over a bare scan, key unread by any agg. Byte path keys on the 4
    code-bytes, emit decodes. **Correct: 1018 tests + 43/43 ClickBench under
    forced-hash; profile confirms `dict-decode` GONE for the coded column.**
  - **HONEST COVERAGE FINDING (2026-05-25):** the hash-path gate is **bench-neutral
    on default ClickBench**. The only single-string-key, no-WHERE GROUP BY is
    high-card URL (Q33/34) — which correctly routes to SORT (sort 392ms vs forced
    hash 838ms: hashing millions of distinct URLs loses to the bounded sort). So
    the gate doesn't fire by default, and forcing hash to fire it is a net loss
    for high-card. The win is LATENT — coding kills dict-decode, but only pays off
    for LOW-card dict-string keys (which hash well) or the WHERE'd/sort shapes.
    **To convert latent→real:** (a) cardinality-based routing — use the dict NDV
    (exact!) to route low-card dict-string GROUP BYs to hash+code (4.5 / #294);
    and/or (b) code the sort path (streamGroupBy) + the filtered emit path so the
    low-card SearchPhrase queries (Q12-17/21/22, which have WHERE / multi-key) get
    it. (b) is where the real ClickBench string-GROUP-BY time lives.
  - [ ] **A4** DISTINCT on codes; ORDER BY via sorted codes; exact cardinality (4.5).
- [ ] 4.3 ORDER BY via sorted global codes; else decode.
- [ ] 4.5 Exact-cardinality consumer: post-predicate dict-survivor count → sizing + predicate ordering.
- [ ] 4.6 *(optional)* function memoization for pure scalar fns over coded columns (Q28 REGEXP_REPLACE; but its result is the GROUP BY key, so entangled with 4.2).
- [ ] 4.7 Tests + A/B: low-card categorical GROUP BYs benefit; high-card raw queries don't regress.

### Phase 5 — Compaction re-tiering + (optional) bit-packing
- [ ] 5.1 Compaction re-decides encoding for merged output (decode inputs → re-encode per gates).
- [ ] 5.2 *(proof-gated)* bit-packing: only if a bandwidth-vs-unpack microbench beats byte-narrow.

---

## 5b. Future directions (noted, not scheduled)

- **Generic multi-key + multi-agg fast paths.** Push the inline-state /
  slot-as-gid machinery to cover *multiple* group keys and *multiple*
  aggregates, with the fastest specialized path selected per query shape
  (the long-term goal: a fast path for every common scenario, generic
  fallback only when nothing fits).
- **Two-slot packing when aggs exceed 128 bits.** When the aggregate state
  doesn't fit one ≤16-byte slot, instead of dropping to the generic
  gid+gstate path, use TWO (or N) inline tables — pack the overflow
  aggregates into a second 128-bit slot keyed on the same group key.
  *Caveat to measure:* the earlier analysis suggests this is likely a
  loss vs. the generic path (N tables = N probes/misses + N hashes/compares
  per row + the gid-correlation problem, whereas generic is 1 probe +
  1 contiguous `gstate` access = 2 misses regardless of agg count). But
  it's worth a microbench at the 2-slot boundary specifically — if the
  second probe is cheaper than a scattered `gstate` access in practice, it
  could win for the just-over-one-slot case. Don't assume; measure.

## 6. Dependencies

- Phase 1 is reversible and **gates** the rest.
- Phases 2 and 3 are independent format changes (either order).
- Phase 4 depends on Phase 3.
- Phase 5 depends on Phases 2 + 3.
