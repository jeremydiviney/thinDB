# SQLancer for thinDB

This test-only module implements a thinDB `SQLProviderAdapter` and a `TLP_WHERE`
`TestOracle` using [SQLancer 2.0.0](https://repo.maven.apache.org/maven2/com/sqlancer/sqlancer/2.0.0/).
It uses SQLancer's seeded `Randomly` generator and provider/oracle interfaces.
The bounded entry point invokes the thinDB provider directly: SQLancer 2.0.0's
general CLI has a built-in provider list and brings in other database drivers.
Only thinDB's provider is exposed here; no upstream fork or MySQL SQL generator
is used. SQLancer is MIT licensed; its dependency metadata is in `pom.xml`.

## Run

Install Zig 0.16.0, Rust (the parent package pins 1.94.0), Java 21 and Maven 3.9.9
or newer. Put `java`, `mvn` (`mvn.cmd` on Windows), and `cargo` on PATH and set
JAVA_HOME for Maven. The manual workflow uses Temurin 21.0.10+7.

```sh
zig build test-sqlancer -Doptimize=ReleaseSafe
zig build test-sqlancer -Doptimize=ReleaseSafe -Dsqlancer-seed=42 -Dsqlancer-seeds=10 -Dsqlancer-queries=1000
zig build test-sqlancer-provider
zig build test-external-sql-harness
```

The first command runs provider unit tests, builds the provider JAR and thinDB,
then checks seeds 0, 1 and 2 with 100 TLP checks each. Each check executes four
SELECTs. Generation is serial, with a fresh server and schema for each seed.
A failure stops the campaign and returns a nonzero status; unattempted seeds
remain apparent in the summary's requested and attempted counts.

| Build option | Default | Meaning |
| --- | --- | --- |
| `sqlancer-seed` | 0 | First signed 64-bit seed |
| `sqlancer-seeds` | 3 | Consecutive seeds, 1–1000 |
| `sqlancer-queries` | 100 | TLP checks per seed, 1–100000 |
| `sqlancer-max-rows` | 32 | Maximum rows per table, 0–256 |
| `sqlancer-query-timeout` | 10 | JDBC statement/socket timeout in seconds, 1–300 |
| `sqlancer-seed-timeout` | 300 | Provider process deadline in seconds, 1–3600 |
| `sqlancer-reduce-attempts` | 64 | Row reduction attempts after a mismatch; 0 disables |
| `sqlancer-dop` | 1 | Server max DOP, 1 or 4 |
| `sqlancer-replay` | unset | Absolute path to a saved replay/reduced JSON file |

All dependencies and Maven plugins have fixed versions. Maven downloads them
on the first build; `mvn --offline verify` works after the cache is populated.
The Rust lockfile is shared with the deterministic harness. Generated JARs,
reports, downloaded dependencies and server data are not source fixtures.

For a prebuilt JAR/server, from `tests/external_sql`:

```sh
cargo run --locked --bin thindb-sqlancer -- --server /absolute/path/to/thindb-server --seed 42 --seeds 1 --queries 1000
```

The launcher also accepts `--java`, `--jar` and `--output`. It deliberately has
no existing-server address, port or database-directory options. Do not invoke
the Java entry point directly: its connection credentials come from the owned
server launcher. The launcher reuses the deterministic harness's loopback,
ephemeral-port, unique-password, temporary-directory and child cleanup code.
Port 13310 and production data paths remain forbidden. Unused listeners and
inherited server configuration are disabled. A hard deadline or handled Ctrl+C
terminates only the Java and thinDB processes that this run created.

## What is checked

The provider generates one to three tables, each with one to three INT/BIGINT
columns and one to two VARCHAR columns. Rows include NULLs, duplicates, bounded
integers, empty strings, whitespace, quotes and LIKE wildcard characters. Zero
rows are allowed. Each file's session remains connected for setup and checks.

Typed expressions include integer addition/negation, COALESCE, comparisons,
LIKE, IN with NULL, BETWEEN, IS NULL, IS NOT NULL, AND, OR and NOT. Predicate
depth is bounded. Projections contain one or more columns and optionally an
integer expression. The initial provider checks single-table SELECTs; joins,
aggregates, windows, floating-point values, transactions, nondeterministic
functions and NoREC are outside this initial generator.

For a generated predicate `p`, the original SELECT must equal the combined
results of the same SELECT with `WHERE p`, `WHERE NOT (p)` and
`WHERE (p) IS NULL`. The three partitions execute separately; the oracle
combines their rows in Java. No DISTINCT or LIMIT invalidates the partition
identity. See [SQLancer's TLP description](https://github.com/sqlancer/sqlancer#approaches-and-papers).

Comparison uses full row multisets: ordering is irrelevant, duplicate counts
and column boundaries matter. JDBC metadata checks column counts and type
families even for zero rows. Integer widths map to one integer family and
values are read as exact BigInteger strings, never floating point. Text keeps
whitespace; NULL is separate from empty text and the string `NULL`. Unexpected
types, SQL errors, connection loss, server exits and timeouts fail. There are
no expected-error suppressions or known-bug exclusions in this provider.

## Diagnose and retain failures

Reports are under `../artifacts/sqlancer/run-*/`:

- `run.json`: configuration, platform and server/provider binary SHA-256.
- `summary.json` and `junit.xml`: seed status, completed checks and failures.
- Per seed: `provider.json`, `server.log`, `java.log`, `setup.sql`, and the
  flushed `statements.sql` trace, including the currently executing statement.
- `checks.jsonl`: completed check queries and order-independent result hashes.
- `replay.json`: setup and the most recently attempted four-query check, saved
  before execution. On failure, `repro.sql` gives a readable SQL script.
- `failure.json`: original error and mismatched results, saved before reduction.
  `replay-command.json` contains command arguments for exact replay. Generated
  runs also save `regenerate-command.json` with the seed and generation options.
- `reduced.json` and `reduced.sql`: when reduction runs, a reproduction with
  irrelevant INSERT statements removed. Reduction requires the same queries
  to retain the same kind of TLP mismatch; SQL errors do not count. Attempts
  are bounded, and new attempts stop after 30 seconds. An in-flight statement
  still has its own timeout; the outer process deadline bounds the whole run.

Replay a failure without relying on the generator's random sequence:

```sh
zig build test-sqlancer -Doptimize=ReleaseSafe -Dsqlancer-replay=/absolute/path/to/reduced.json
```

Replay uses another fresh server, executes the recorded setup in order, and
checks the recorded queries. A reproduced mismatch exits nonzero. The original
failure remains intact even if reduction fails or times out. Preserve the
whole artifact directory for infrastructure failures; it may have no finished
query check to reduce.

After investigating a discovered bug, retain its reviewed minimal JSON under
this module as a generated regression and fix the database in a separate change.
A regression's assertion is the TLP identity, not an expected answer generated
from thinDB. No database bug fixes are part of this provider integration.

## Manual workflow and validation

`.github/workflows/sqlancer.yml` has only `workflow_dispatch`, with Windows and
Linux jobs, bounded seed/query inputs and diagnostics uploaded even on failure.
It does not modify automatic CI or `zig build test`. The imported deterministic
suite remains usable without Java or Maven.

Provider unit tests inject lost/duplicate rows, changed values, large-integer
rounding, NULL/text confusion, empty-result shape/type errors, SQL errors,
disconnects and timeouts. They also verify seed determinism, JDBC conversion,
fingerprints and that the reducer keeps a triggering row while removing others.
The Rust tests cover reporting a failed, incomplete campaign.

After building the runner, the live failure tests can be repeated separately
with Python 3 (standard library only):

```sh
zig build dist -Doptimize=ReleaseSafe
python tests/external_sql/sqlancer/validate.py --runner tests/external_sql/target/debug/thindb-sqlancer --server zig-out/bin/thindb-server
```

Add `.exe` to both executable paths on Windows. These checks intentionally
inject incorrect results, SQL errors, startup failure and timeouts, verify
their exact failure categories, replay a reduced failure, and check temporary
data cleanup. Their `validation.json` records the expected nonzero exits;
the validation script itself succeeds only when all checks behave as expected.
The manual workflow runs these checks too.

The measured initial runs and live replay/failure checks are recorded in
`baseline.json`. This is a bounded sample, not a claim that all generated SQL
or all of thinDB's SQL implementation has been verified. Scheduling and PR
gating remain a separate rollout decision.
