# Encoded scans and aggregation machinery — September 14, 2026

These changes reduce decoding, temporary allocation and group-state movement for
general operator shapes. They preserve SQL, key widths, storage formats and
public APIs. The implementation is based on de95110e3c7c538e97d337791928c357788d0afe.

## Changes

- Internal synchronous scan callbacks expose pinned raw/RLE blocks to consumers.
  Integer global reductions consume supported runs or borrowed raw views. Narrow
  integer batch sums use a provably safe 64-bit partial with the existing wide
  total. Unsupported encodings, nullable inputs and tombstones retain fallbacks.
- Count-only numeric grouping merges encoded key runs and their weights. It
  processes the source again on each execution; it does not cache aggregates.
- Group state grows in pages of 8,192 records, avoiding relocation of established
  pages. Published staging buffers acquire replacements only when needed.
- Count-ranked top-N rejects losing groups before constructing candidates.
  Block-pruned top-N borrows raw probe columns and reuses worker masks/views.
- Large count-only group states use a separate prefetch/update kernel with at
  most 16 pending increments in stack storage. Dispatch requires at least 2 MiB
  of actual live state in the current bucket. Pending writes drain before batch
  completion; new groups initialize immediately. Small-state and other aggregate
  programs keep their ordinary update loop.

No query numbers, SQL text or column names select these optimizations. Direct
staging-copy elimination and bitmap hash-table prototypes were slower and were
removed. Key-size optimization and cross-query workspace pooling remain deferred.

## Measurement

The local Windows ClickBench dataset contains 99,997,497 rows. Measurements use
DOP 12, a 16 GiB cache, separate 16 GiB shared/query budgets, disabled region pool
and compaction, and loopback MySQL on private port 7881. Each version/query block
starts a fresh service, excludes one warmup, measures three warm executions and
performs a separate decoded validation. Version order alternates by query. No
timed query overlaps our builds, tests or native benchmarks. The host remains
shared; DOP is a worker cap rather than CPU affinity or exclusive cores.

The first matched sweep, before adding the separate count-prefetch kernel, was:

| Query | Original ms | Encoded/paged machinery ms |
|---|---:|---:|
| Q16 | 185.40 | 183.90 |
| Q25 | 12.76 | 3.96 |
| Q30 | 14.36 | 3.51 |
| Sum of 43 means, seconds | 15.30 | 14.49 |

Q25/Q30 gains recurred in independent checks. Q16 varied. These figures must not
be mixed with a later sweep's baseline.

The final matched sweep compares that encoded/paged baseline with the additional
count-prefetch kernel. [The CSV](clickbench_encoded_machinery_20260914.csv) retains
all 43 means and every timed sample from both versions, including slow samples.

| Measurement | Before | Final |
|---|---:|---:|
| Sum of all 43 means, seconds | 15.693 | 15.002 |
| Sum excluding Q29, seconds | 9.071 | 9.211 |
| Q16 three-warm-run average, ms | 213.58 | 199.60 |
| Q16 balanced six-sample average, ms | 208.38 | 193.71 |

The final total's reduction is driven by Q29 (6,621.69 to 5,791.41 ms), whose
AVG/MIN program does not activate the new count-only path. It is not evidence of
a universal improvement. Q16 improved by approximately 6–7% in the final matched
and balanced comparisons, but an earlier focused block was slower. Q18 was
slower in the full sweep; a subsequent balanced comparison measured 574.03 versus
564.91 ms. Q36 was effectively unchanged in its balanced control. Timing noise
and remaining unexplained movement are retained rather than hidden by replacing
full-sweep samples with targeted reruns.

## Correctness and checks

- ReleaseFast distribution build and full unit/integration/client/V2 suite pass:
  1,546 tests passed, with five existing skips.
- Separate ReleaseSafe count-kernel tests and the native benchmark suite pass.
- Both final versions match 41 deterministic ClickBench fingerprints. Q42 matches
  serial-reference counts and OFFSET ranks with valid ties. Q18 has an unordered
  LIMIT; all 60 selected groups across the final sweep and its follow-up have
  exact counts checked against a separate serial query.
- Fixtures cover raw/RLE/memtable input, tombstones, empty predicates, compound
  run boundaries, serial/parallel execution, nullable strings, ties and OFFSET;
  page stability/reset and allocation failures; repeated count updates, large
  weights, buffer wraparound/drains, and both sides of the live-state threshold.

The detailed local records remain under
`.bench-data/clickbench-machinery-20260914/` and
`.bench-data/clickbench-direct-stage-20260914/`.

Windows binary SHA-256 identities:

- Original before encoded/paged changes:
  `8ea879d81374c42170b6e8b1b94069674ce73be48c7a236d4997f2f7e4f79056`
- Encoded/paged baseline for the final sweep:
  `d1ce4015a5282beb6cfd7e3ca8484674b420ee404a33e19877d110acdc3bb988`
- Final separate-kernel build:
  `79c81003023ae0c55c9fc0a9b252c05b25e42637b70a0da173a0663efea85136`
