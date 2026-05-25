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

### Phase 1 — In-memory FOR narrow keys *(no format change, reversible — the proof)*
- [ ] 1.1 Cross-segment stat rollup: query-global `min/max` (+ exact/upper NDV) from per-segment stats.
- [ ] 1.2 Query base/width at plan start: `base = global_min`, `bits = ceil(log2(range+1))`, pick u8/u16/u32 slot.
- [ ] 1.3 Narrow-key build in the group-table path: pack `(value − base)` into the narrowest slot (extend tiers with u8/u16; multi-col → bit-fields).
- [ ] 1.4 Emit/order: reconstruct `base + code` at output; order-preserving so ORDER BY on the packed key holds.
- [ ] 1.5 Microbench: narrow-key probe vs current at several ranges.
- [ ] 1.6 ClickBench A/B. **Decision gate:** keep only on a real win; quantifies ClickBench benefit before any format work.

### Phase 2 — On-disk FOR for numeric/temporal columns *(format change)*
- [ ] 2.1 Segment format: per-column encoding header `{raw | FOR(base,width)}`; manifest/version bump.
- [ ] 2.2 Writer: per-segment base/range at flush; byte-narrow when it saves width; gate; else raw.
- [ ] 2.3 Reader: decode to a narrow view + base; cache holds the encoded(narrow) form.
- [ ] 2.4 Predicate translation: numeric constant → `const − base` per segment; min/max skip in-domain.
- [ ] 2.5 SUM base-correction: `SUM = N·base + SUM(delta)` (algebraic-reduction path); MIN/MAX/COUNT trivial.
- [ ] 2.6 Tests + A/B: per-type round-trip, NULL orthogonality, edge ranges; segment-size + scan-bandwidth.

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

## 6. Dependencies

- Phase 1 is reversible and **gates** the rest.
- Phases 2 and 3 are independent format changes (either order).
- Phase 4 depends on Phase 3.
- Phase 5 depends on Phases 2 + 3.
