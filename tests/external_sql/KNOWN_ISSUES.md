# External SQL correctness issues

These are executable compatibility failures, tracked separately from unsupported
features and deliberate thinDB semantics. A different failure or an unexpected
pass fails the harness. Fixes belong in separate database changes; remove each
classification when its original upstream expectation passes.

## Calendar boundaries

Status: open. Discovered on Windows and Linux with thinDB revision
`4dc148c5cb80492050958540d604073e78ad8162`, using the pinned StarRocks
`test_date_diff` fixture. Its expected values describe StarRocks semantics;
thinDB returns the same discrepancies through both wire protocols.

| Source identifier | Query | Upstream result | thinDB result |
| --- | --- | --- | --- |
| `starrocks/date_diff:9` | `select date_diff('year', '2017-02-28', '2016-02-28 00:00:00.01');` | 1 | 0 |
| `starrocks/date_diff:105` | `select date_diff('month', '2017-02-28', '2016-02-28 00:00:00.001');` | 12 | 11 |

The fractional second changes thinDB's calendar-unit result at these anniversary
boundaries. The fixture's neighboring leap-day and fractional-second cases
remain enabled. These two cases need investigation against the intended
StarRocks-compatible calendar contract; this integration does not change it.

Replay either check, including its original session setup:

```sh
zig build test-external-sql -Doptimize=ReleaseSafe -Dsql-filter=starrocks/date_diff:9
zig build test-external-sql -Doptimize=ReleaseSafe -Dsql-filter=starrocks/date_diff:105
```

The exact expected failure signatures live in `policies.json`. The report also
contains the original expected value, actual value, SQL, and replay command.

## PostgreSQL Describe on INSERT SELECT

Status: open, separate protocol investigation. While validating the PostgreSQL
adapter on the same revision, Parse/Describe of `INSERT INTO t SELECT * FROM t`
applied the insert before a Simple Query executed it again. Starting with 24
rows, the first copy statement reported 48 affected rows instead of 24; repeated
copy statements grew the table fourfold instead of doubling it.

The harness sends statement records through Simple Query exactly once. Only
query records request Describe metadata, needed to check empty-result column
counts and exact numeric types. The live adapter test now inserts one row,
copies the table once, and requires a count of two. This protocol issue is
outside the two SQL-result known-bug classifications above; a dedicated extended
protocol regression and engine fix belong in a separate change.
