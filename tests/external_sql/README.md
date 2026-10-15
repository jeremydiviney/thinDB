# Independent SQL correctness tests

This opt-in Rust harness runs pinned upstream SQL and expected results against
disposable thinDB servers. It is independent of `zig build test`, the internal
Zig tests, and `tests/bun/conformance.mjs`. No production library or runtime
dependency is added.

The initial corpus contains 9,766 statement and query checks in 12 files. Its
four SQLite-derived files contain 8,152 query checks plus 1,118 setup statements.
The measured baseline and validation details are recorded in `baseline.json`.
Two date-boundary discrepancies remain executable, tracked in
[KNOWN_ISSUES.md](KNOWN_ISSUES.md).

## Run locally

Install Zig 0.16.0 and Rust through rustup. `rust-toolchain.toml` selects Rust
1.94.0; `Cargo.lock` pins the test dependencies, including `sqllogictest` 0.29.1.
Rust's normal platform linker is required (Visual Studio Build Tools for the
Windows MSVC toolchain, or a C toolchain on Linux). Cargo downloads dependencies
on the first build. Corpus execution itself never downloads upstream inputs.

From the repository root:

```sh
zig build test-external-sql -Doptimize=ReleaseSafe
zig build test-external-sql -Doptimize=ReleaseSafe -Dsql-suite=sqlite
zig build test-external-sql -Doptimize=ReleaseSafe -Dsql-filter=duckdb/count
zig build test-external-sql -Doptimize=ReleaseSafe -Dsql-filter=starrocks/date_diff:9
zig build test-external-sql -Doptimize=ReleaseSafe -Dsql-timeout=60 -Dsql-file-timeout=900
```

`-Dsql-suite` accepts `all` (default), `sqlite`, `duckdb`, or `starrocks`.
A filter without a colon matches source identifiers by substring. A filter with
a colon selects exactly one original source line. Preceding setup statements
still execute in sequence; subsequent setup and unrelated queries are reported
as filtered. Cases within one file share a connection and database. Different
files always get new servers and data directories. Execution is serial.

Harness validation is separate too:

```sh
zig build test-external-sql-harness
zig build test-external-sql -Doptimize=ReleaseSafe -Dsql-self-test=true
cargo +1.94.0 fmt --manifest-path tests/external_sql/Cargo.toml --check
```

Unit tests inject wrong values, ordering, duplicates, counts, result shapes,
hashes, error classes, and invalid directives. The live self-test checks exact
BIGINT/DECIMAL values, whitespace, NULL versus empty text, empty-result metadata,
session continuity, SQL errors, timeout, disconnect, and database isolation.
The temporary files are intentionally discarded: these checks do not claim to
test persistence or restart recovery.

For direct execution against a locally built server binary:

```sh
cargo +1.94.0 run --locked --manifest-path tests/external_sql/Cargo.toml -- --server /absolute/path/to/thindb-server --suite all
```

The server argument is a binary path, never an existing server address. There
is no external host, port, or data-directory option. `--list` lists imported
case identifiers without starting the binary.

## Isolation and diagnostics

Each file launches an owned child process on loopback with a fresh OS temporary
directory, an unused ephemeral port, and a unique password. Port 13310 and
production data paths are rejected. Unused listeners are disabled, inherited
`THINDB_*` settings are removed, and memory and DOP settings are explicit. The
harness checks the child's startup log and authenticates before running SQL.
Cleanup terminates only that child, including on a handled Ctrl+C.

Every run creates `artifacts/run-<timestamp>/` containing:

- `results.json`: status, duration, SQL, expected output, actual result fingerprint
  and replay command for every selected source record, including excluded and
  blocked records. Failures retain the full actual output.
- `junit.xml`: CI results; known bugs and exclusions are visible as skipped tests.
- `run.json`: binary SHA-256, platform, protocol, DOP, filters and timeout settings.
- `sources.json` and `policies.json`: the exact provenance and compatibility policy.
- One server log per source, plus setup-and-query SQL reproductions for failures.

Unexpected results, SQL errors, transport failures, crashes, timeouts, blocked
checks, and unexpected known-bug passes return a nonzero exit status. A failed
setup stops dependent checks. A known bug must match its exact recorded failure
signature; a crash or timeout cannot satisfy it.

## Corpus provenance and conversion

`sources.json` records each original path, revision, format and SHA-256. Inputs
under `upstream/` are byte-preserved and verified before execution. Their Git
attributes prevent line-ending conversion. Upstream licenses and StarRocks's
notice are included there. `vendor.py` is an explicit maintenance command for
refreshing those pinned inputs; ordinary runs do not invoke it.

| Source | Revision | Imported coverage |
| --- | --- | --- |
| DuckDB, including SQLite-derived SQLLogicTests | `e08224d653fcade680e471709e9a9035223989df` | select1–4; COUNT, SUM, NULL aggregates, basic windows, LIKE with NULL, simple NOT IN |
| StarRocks SQL-tester result files | `27069855f0c1c282732dd20b03f8f8265975cbd1` | date_diff and self-contained basic window fixtures |

`policies.json` contains reviewed, exact-match SQL patches and case policies.
No patch changes an expected value or hash. The patches expand DuckDB's range
fixtures to identical literal rows, remove unsupported casts from a DOUBLE
fixture insert, and adapt StarRocks table DDL by removing distributed-engine
options, spelling DECIMAL64 as DECIMAL, and unquoting simple fixture identifiers
so the same setup parses through PostgreSQL's wire grammar. Original fixture
data and queries are otherwise retained.
Error-message translations are explicit SQLSTATE policies.

The StarRocks reader converts result blocks to SQLLogicTest records, including
`None` to NULL, empty fields to `(empty)`, and `[ORDER]` to ordered comparison.
Other StarRocks results use row sorting. Unsupported shell/function helpers,
unrecognized directives and truncated records are errors. The imported Trino
dialect section remains accounted for as unsupported.

The SQLLogicTest parser handles statements, queries, sort modes, labels, hashes,
and `skipif`/`onlyif` conditions for the `thindb` engine label. Unknown extended
directives and named connections require an explicit future import change.
Hash thresholds affect expected-output generation upstream; imported expected
hashes and values are always authoritative here.

## Comparison and classifications

The adapter retains wire metadata even for zero rows. Type-letter counts assert
column counts; source letters do not demand identical integer widths across
protocols. Values use the source format's value-per-line or tabular convention.
Ordered results retain row boundaries; row sorting retains duplicate rows;
value sorting follows SQLLogicTest's explicit flattening rule. Hashes use MD5
over each displayed value followed by a newline, and repeated labels must agree.

Integers and decimals compare numerically using exact decimal strings, without
floating-point conversion. Text keeps its whitespace; NULL and empty text are
distinct. SQLLogicTest's reserved textual sentinels (`NULL`, `(empty)`) remain
part of that format. Float tolerances apply only to explicitly listed cases
whose upstream output rounds floating-point values; decimals never use them.

An unclassified enabled case is supported and must pass. Exclusions must name
a source identifier or bounded source range and give a reason. Known bugs also
need a tracking reference and exact failure signature. The initial exclusions
cover unsupported ILIKE, selected aggregate/window syntax, DuckDB-specific
range/GROUP BY ALL queries, and the StarRocks fixture's Trino mode. The two SUM
semantic differences follow thinDB's documented BIGINT wrapping contract in
`DESIGN.md` §3.4. No broad arithmetic or date suppression is used.

When updating the corpus, change the revision deliberately, fetch originals,
review checksums and licenses, then review every conversion and exclusion.
Stale case IDs and patches fail validation. Never replace upstream expected
results with values generated by thinDB.

## Manual CI and configuration comparisons

`.github/workflows/external-sql.yml` runs only through `workflow_dispatch`, on
Windows and Linux. It installs the pinned toolchains, checks the harness,
exercises the live adapter, runs the selected suite, and uploads diagnostics
even after failure. Existing CI and the internal test target are unchanged.

The deterministic runner also supports PostgreSQL and DOP variations:

```sh
zig build test-external-sql -Doptimize=ReleaseSafe -Dsql-protocol=postgres
zig build test-external-sql -Doptimize=ReleaseSafe -Dsql-dop=4
zig build test-external-sql -Doptimize=ReleaseSafe -Dsql-dop=4 -Dsql-compare-run=/absolute/path/to/previous/results.json
```

`--compare-run` / `-Dsql-compare-run` requires identical candidate IDs and checks
the new run against both upstream expectations and the previous normalized
result fingerprints/statuses. This catches differences between repeat runs or
DOP settings. Float fingerprints are exact, even for cases whose reference
comparison has an explicit tolerance. Use identical suite and filter choices.

## Subsequent generated testing

SQLancer is the next phase after accepting this deterministic baseline. It is
not installed or invoked by these commands. Add a thinDB-specific provider;
MySQL wire compatibility alone is insufficient to use SQLancer's MySQL grammar.
Start with bounded integers, strings and NULLs, then implement Ternary Logic
Partitioning before NoREC-style count comparisons. Preserve seeds, schema/data,
query pairs and reduced reproductions, and retain discovered regressions here.
Keep generated jobs manual until a separate rollout decision enables schedules
or PR gating. ClickHouse corpus imports remain deferred because their engine,
type, setting and administrative dependencies need independent review.
