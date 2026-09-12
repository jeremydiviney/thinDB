# Local ThinDB ClickBench at DOP 12 — September 12, 2026

Full compatible dataset: 99,997,497 rows; all 43 canonical queries. ThinDB completed 43/43 queries.

Local machine: AMD Ryzen 9 9900X, 12 physical cores / 24 logical CPUs, 64 GiB installed DDR5-6000, WD_BLACK SN850X NVMe, Windows 11. ThinDB uses DOP 12, an 8 GiB source cache and 16 GiB query/shared budgets to leave memory available for resident applications. Each query starts a fresh private server on loopback port 7881, then runs three times. Startup and connection setup are excluded; query submission, execution and packet drain are included. A fourth, untimed execution records full text results and fingerprints.

The comparison uses the smaller of runs 2 and 3, following the [ClickBench hot-run rule](https://github.com/ClickHouse/ClickBench/blob/4c7705d105910527f55e7a51eea08090e702286f/README.md). The first run is recorded but is not described as a true cold run because Windows OS caches were not purged. No per-query tuning or result cache was used.

The closest common AMD x86 tier on the main leaderboard is c6a.4xlarge: AMD EPYC 7R13, 8 physical cores / 16 vCPUs and 32 GiB RAM, with EBS storage ([AWS specifications](https://docs.aws.amazon.com/ec2/latest/instancetypes/co.html)). This is an approximate hardware comparison: the local machine has more physical cores, more RAM, newer cores and NVMe. Published runs use their default parallelism, so they are not DOP-12-matched. No core-count scaling has been applied. The Ryzen entries on the separate hardware board use a different dataset and were excluded.

| System | Hardware | Run date | Queries | Sum of hot times, s | Adjusted geometric mean: published / ThinDB | ThinDB lower time |
|---|---|---|---:|---:|---:|---:|
| ThinDB | Local Ryzen 9900X, DOP 12 | 2026-09-12 | 43/43 | 19.296 | 1.00 | — |
| [ClickHouse](https://raw.githubusercontent.com/ClickHouse/ClickBench/4c7705d105910527f55e7a51eea08090e702286f/clickhouse/results/20260912/c6a.4xlarge.json) | c6a.4xlarge | 2026-09-12 | 43/43 | 18.150 | 1.34 | 31/43 |
| [DuckDB](https://raw.githubusercontent.com/ClickHouse/ClickBench/4c7705d105910527f55e7a51eea08090e702286f/duckdb/results/20260511/c6a.4xlarge.json) | c6a.4xlarge | 2026-05-11 | 43/43 | 26.252 | 2.18 | 42/43 |
| [StarRocks](https://raw.githubusercontent.com/ClickHouse/ClickBench/4c7705d105910527f55e7a51eea08090e702286f/starrocks/results/20260907/c6a.4xlarge.json) | c6a.4xlarge | 2026-09-07 | 43/43 | 44.467 | 3.12 | 40/43 |
| [Umbra](https://raw.githubusercontent.com/ClickHouse/ClickBench/4c7705d105910527f55e7a51eea08090e702286f/umbra/results/20260815/c6a.4xlarge.json) | c6a.4xlarge | 2026-08-15 | 43/43 | 8.097 | 0.63 | 9/43 |

The geometric mean uses the leaderboard’s +10 ms adjustment and its missing-result penalty. Values above 1 mean lower ThinDB times in this particular cross-hardware comparison. It is not a hardware-controlled engine ranking. Small timing differences are not significance claims.

The SQL differs from the canonical DuckDB query file only in Q28 and Q29: STRLEN is expressed as ThinDB octet_length, preserving byte-length semantics. The old local length expressions counted UTF-8 characters and were corrected before this run. No engine code was changed for ClickBench.

## All queries

| Query | ThinDB ms | ClickHouse ms | DuckDB ms | StarRocks ms | Umbra ms |
|---|---:|---:|---:|---:|---:|
| Q1 | **0.2** | 1.0 | 18.0 | 35.0 | 8.0 |
| Q2 | 4.9 | **1.0** | 41.0 | 59.0 | 4.0 |
| Q3 | **0.2** | 33.0 | 74.0 | 103.0 | 27.0 |
| Q4 | **0.2** | 44.0 | 86.0 | 136.0 | 26.0 |
| Q5 | 245.1 | 226.0 | 348.0 | 354.0 | **131.0** |
| Q6 | 197.3 | 403.0 | 338.0 | 872.0 | **174.0** |
| Q7 | **0.1** | 10.0 | 30.0 | 10.0 | 24.0 |
| Q8 | 4.1 | 10.0 | 38.0 | 69.0 | **4.0** |
| Q9 | 386.8 | 479.0 | 443.0 | 372.0 | **160.0** |
| Q10 | 434.5 | 548.0 | 612.0 | 716.0 | **227.0** |
| Q11 | 72.9 | 140.0 | 150.0 | 262.0 | **25.0** |
| Q12 | 78.8 | 167.0 | 170.0 | 309.0 | **28.0** |
| Q13 | **156.8** | 391.0 | 408.0 | 665.0 | 161.0 |
| Q14 | 532.4 | 593.0 | 781.0 | 1006.0 | **309.0** |
| Q15 | **157.5** | 471.0 | 473.0 | 940.0 | 183.0 |
| Q16 | 220.5 | 284.0 | 389.0 | 368.0 | **171.0** |
| Q17 | 674.3 | 1146.0 | 892.0 | 1355.0 | **368.0** |
| Q18 | 638.4 | 458.0 | 659.0 | **92.0** | 215.0 |
| Q19 | 1073.6 | 2141.0 | 1650.0 | 2509.0 | **846.0** |
| Q20 | 5.8 | **2.0** | 49.0 | 22.0 | **2.0** |
| Q21 | 660.4 | 385.0 | 711.0 | 906.0 | **133.0** |
| Q22 | 670.3 | 112.0 | 769.0 | 611.0 | **55.0** |
| Q23 | 958.6 | 549.0 | 1169.0 | 1876.0 | **64.0** |
| Q24 | 45.5 | 98.0 | 349.0 | 1298.0 | **24.0** |
| Q25 | 14.6 | 86.0 | 72.0 | 109.0 | **5.0** |
| Q26 | **8.5** | 199.0 | 166.0 | 155.0 | 10.0 |
| Q27 | 14.2 | 70.0 | 69.0 | 112.0 | **4.0** |
| Q28 | 1086.5 | 159.0 | 645.0 | 1097.0 | **113.0** |
| Q29 | 6411.3 | 1614.0 | 6478.0 | 9708.0 | **1393.0** |
| Q30 | **13.7** | 36.0 | 68.0 | 125.0 | 30.0 |
| Q31 | 144.8 | 260.0 | 407.0 | 624.0 | **84.0** |
| Q32 | 201.8 | 345.0 | 611.0 | 889.0 | **129.0** |
| Q33 | **1269.6** | 2222.0 | 2035.0 | 3173.0 | 1323.0 |
| Q34 | 1310.2 | 2030.0 | 2054.0 | 6050.0 | **730.0** |
| Q35 | 1305.0 | 2053.0 | 2198.0 | 6029.0 | **732.0** |
| Q36 | 138.4 | 213.0 | 468.0 | 688.0 | **123.0** |
| Q37 | 23.0 | 34.0 | 52.0 | 123.0 | **11.0** |
| Q38 | 23.5 | 19.0 | 39.0 | 97.0 | **5.0** |
| Q39 | 11.8 | 19.0 | 39.0 | 81.0 | **3.0** |
| Q40 | 60.2 | 68.0 | 86.0 | 234.0 | **22.0** |
| Q41 | 7.7 | 12.0 | 41.0 | 94.0 | **3.0** |
| Q42 | 16.5 | 10.0 | 38.0 | 83.0 | **3.0** |
| Q43 | 15.0 | 9.0 | 39.0 | 51.0 | **5.0** |

## Receipts

Native ReleaseFast binary SHA256: 6917ddaf899c6feed4f5c45bd4a9457354acf5f8bafc06c3f02322574aa485ea. SQL SHA256: 81a6900485733bd4d4c4afe31bfbec7685d5c19acc3331c2d98651764e90de49.

All three timings, per-query result rows, full text values, fingerprints, process/memory receipts, source references and server logs are retained beside this report. The full source table was reused; load time was not remeasured. Production port 13310 and its process were not touched.

Peak benchmark working set: 24.571 GiB. The private service is stopped; port 7881 has no listener. Local production remains PID 26688.

Artifacts: `.bench-data/clickbench-local-dop12-20260912/`; `per-query.csv` contains all three timings, all four published comparisons and row counts. `result.json` includes complete text results and fingerprints. Published source files and their pinned URLs are retained in `reference-manifest.json`.
