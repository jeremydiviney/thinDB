# thinDB ClickBench harness

Tracks thinDB performance against the canonical
[ClickBench](https://benchmark.clickhouse.com/) workload — a single
denormalized table (`hits`), 43 standard queries, and a public
leaderboard comparing DuckDB / ClickHouse / StarRocks / Snowflake /
Umbra etc.

## Layout

- `schema.zig` — the 105-column `hits` table definition (mirrors the
  ClickHouse-flavored schema from the upstream repo, mapped to
  thinDB types).
- `loader.zig` — parses TSV input row-by-row, accumulates per-column
  ColumnStores into a typed batch, calls `Table.insertBatch` in
  chunks (default 64K rows per chunk).
- `main.zig` — opens a fresh DB, creates the table, runs the loader
  against `data/hits.tsv` (or a path passed on the command line),
  reports load throughput. Querying will be wired up after the load
  path is solid.
- `data/` — drop the TSV file here. **Not checked in** (multi-GB).
  See "Getting the dataset" below.

## Getting the dataset

The canonical ClickBench dataset lives at
[`datasets.clickhouse.com/hits_compatible/hits.tsv.gz`][hits-tsv-gz]
(~14 GB compressed, ~75 GB uncompressed, 100M rows). For dev
iteration take the first N rows of the decompressed file:

```sh
# Full file — production runs only
curl -L https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz | gunzip > data/hits.tsv

# Smaller dev subset — first 1M rows (~700 MB)
curl -L https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz | gunzip | head -n 1000000 > data/hits_1m.tsv
```

[hits-tsv-gz]: https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz

## Running

```sh
# Default — reads ./data/hits.tsv, loads as `hits` table into a
# fresh DB at ./.clickbench-db/
zig build clickbench -Doptimize=ReleaseFast

# Custom path / size
zig build clickbench -Doptimize=ReleaseFast -- data/hits_1m.tsv
```

## Status

This is a v1 setup. We're starting with **just the load path** —
verifying we can parse the 105-column TSV, build columnar batches,
and bulk-insert into a thinDB segment. Query benchmarking lands in
a second pass once the load story is solid.
