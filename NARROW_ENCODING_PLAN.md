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
- **Per-segment NDV cap (64K)** is enforced at flush (abandon→raw mid-build).
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

### Phase 3 — Segment-local string dictionary *(format change)*
- [ ] 3.1 Format: per-column `dict` encoding — sorted dict block + FOR-narrowed code column.
- [ ] 3.2 Writer gate (avg-len ≤ 256, NDV ≤ 64K with mid-build abandon, NDV ≤ N/2); else raw.
- [ ] 3.3 Reader: expose codes + dict; decode→string only at materialize; per-segment coded-vs-raw.
- [ ] 3.4 Tests: round-trip, NULL-vs-empty under codes, mixed coded/raw scan, long-string bail, mid-build abandon.

### Phase 4 — Dictionary-aware execution *(the query wins)*
- [ ] 4.1 Query-time stitch: merged `string→code` map + per-segment LUTs; raw/memtable hash per-row in; global width floats.
- [ ] 4.2 Group/distinct on global codes (feeds Phase 1 narrow keys).
- [ ] 4.3 ORDER BY via sorted global codes; else decode.
- [ ] 4.4 Dict predicate pushdown: per-segment dict eval → matches-bitset → per-row bit-test; raw fallback.
- [ ] 4.5 Exact-cardinality consumer: post-predicate dict-survivor count → sizing + predicate ordering.
- [ ] 4.6 *(optional)* function memoization for pure scalar fns over coded columns.
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
