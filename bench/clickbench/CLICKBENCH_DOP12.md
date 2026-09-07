# ClickBench 100M — 2026-09-05 rerun: decoded block cache vs main, DuckDB per-process

- Same box, same `.clickbench-db` (`clickbench__public`, 99,997,497 rows, LZ4 at rest), both thinDB binaries ReleaseFast, `--max-dop 12 --query-memory-budget 32G`, default cache size (35% of RAM). Each thinDB arm ran the suite twice on a fresh server (best of 3 per query per pass); the table shows the better pass per query. DuckDB v1.4.4, threads=12, fresh process per query, best of 3.
- **main (add919a): 16.7s. Decoded block cache (this branch): 15.0s. DuckDB: 48.7s.** thinDB faster than DuckDB on 29/43.
- The decoded cache wins 12 queries outright, all of them scans over the wide string columns (URL, Title, Referer, SearchPhrase) that used to decompress per borrow: Q21 349→84 ms, Q22 768→95, Q27 1063→547, Q20 431→276, Q33/Q34 ~950→~800. Three int-key GROUP BYs (Q8, Q15, Q32) run 6-9% slower on both passes; the server's peak working set rose from 24.4 GB to 38.8 GB on this table (the decoded string blocks fill the 22 GiB cache budget), which is the likely cause.
- Artifacts: `tests/bun/dop12_2026-09-05_main.json`, `tests/bun/dop12_2026-09-05_decoded_cache.json`, `bench/clickbench/duckdb/duck_isolated_12_2026-09-05.txt`.

| Query | main ms | decoded cache ms | DuckDB ms | |
|---|---:|---:|---:|:--|
| Q0 | 0 | 0 | 0 |  |
| Q1 | 5 | 5 | 2 |  |
| Q2 | 0 | 0 | 12 |  |
| Q3 | 0 | 0 | 15 |  |
| Q4 | 215 | 226 | 231 |  |
| Q5 | 208 | 180 | 500 | win |
| Q6 | 0 | 0 | 4 |  |
| Q7 | 3 | 3 | 3 |  |
| Q8 | 336 | 358 | 287 |  |
| Q9 | 382 | 389 | 369 |  |
| Q10 | 68 | 69 | 113 |  |
| Q11 | 73 | 73 | 118 |  |
| Q12 | 170 | 130 | 512 | win |
| Q13 | 392 | 356 | 1287 |  |
| Q14 | 178 | 151 | 586 | win |
| Q15 | 171 | 188 | 278 |  |
| Q16 | 600 | 583 | 2013 |  |
| Q17 | 608 | 573 | 1251 |  |
| Q18 | 958 | 932 | 4727 |  |
| Q19 | 8 | 8 | 3 |  |
| Q20 | 431 | 276 | 704 | win |
| Q21 | 349 | 84 | 679 | win |
| Q22 | 768 | 95 | 1652 | win |
| Q23 | 69 | 44 | 114 | win |
| Q24 | 10 | 9 | 17 |  |
| Q25 | 10 | 6 | 17 |  |
| Q26 | 10 | 9 | 17 |  |
| Q27 | 1063 | 547 | 735 | win |
| Q28 | 5842 | 6297 | 6958 |  |
| Q29 | 14 | 16 | 15 |  |
| Q30 | 169 | 128 | 318 | win |
| Q31 | 207 | 191 | 375 |  |
| Q32 | 1124 | 1199 | 3621 |  |
| Q33 | 950 | 822 | 10650 | win |
| Q34 | 959 | 795 | 10079 | win |
| Q35 | 150 | 154 | 318 |  |
| Q36 | 39 | 24 | 23 |  |
| Q37 | 27 | 21 | 13 |  |
| Q38 | 22 | 11 | 10 |  |
| Q39 | 82 | 61 | 49 | win |
| Q40 | 6 | 7 | 5 |  |
| Q41 | 15 | 15 | 6 |  |
| Q42 | 13 | 13 | 6 |  |

---

# ClickBench 100M — thinDB vs DuckDB @ DOP 12 (corrected, fair)

- Hardware: 12 physical / 24 logical cores, 64 GiB RAM. 100M-row hits table.
- thinDB: thindb-server ReleaseFast, --max-dop 12, 32 GiB query budget, persistent server (warm cache), best-of-3.
- **DuckDB: v1.4.4, threads=12, EACH QUERY IN A FRESH PROCESS, best-of-3.**

> METHODOLOGY NOTE: running all 43 DuckDB queries in ONE process inflates the
> memory-heavy GROUP BYs 2-16× (buffer pool accumulates across the session: Q33
> 9.6s→50.7s, Q30 0.3s→4.8s). The one-session DuckDB total was a bogus 210s. The
> fair isolated-per-query total is 46.6s. Always benchmark DuckDB per-process here.

| Query | thinDB ms | DuckDB ms | winner |
|---|---|---|---|
| Q00 | 24 | 0 | 0.00× duck |
| Q01 | 18 | 2 | 0.11× duck |
| Q02 | 268 | 12 | 0.04× duck |
| Q03 | 449 | 15 | 0.03× duck |
| Q04 | 1354 | 227 | 0.17× duck |
| Q05 | 3705 | 495 | 0.13× duck |
| Q06 | 0 | 3 | 0.05× thinDB |
| Q07 | 21 | 3 | 0.15× duck |
| Q08 | 2172 | 283 | 0.13× duck |
| Q09 | 2641 | 366 | 0.14× duck |
| Q10 | 445 | 112 | 0.25× duck |
| Q11 | 559 | 118 | 0.21× duck |
| Q12 | 963 | 495 | 0.51× duck |
| Q13 | 1280 | 1222 | 0.95× duck |
| Q14 | 1752 | 594 | 0.34× duck |
| Q15 | 1707 | 266 | 0.16× duck |
| Q16 | 6551 | 2074 | 0.32× duck |
| Q17 | 6319 | 1204 | 0.19× duck |
| Q18 | 6164 | 4546 | 0.74× duck |
| Q19 | 39 | 3 | 0.08× duck |
| Q20 | 294 | 694 | 0.42× thinDB |
| Q21 | 342 | 688 | 0.50× thinDB |
| Q22 | 2372 | 1655 | 0.70× duck |
| Q23 | 283 | 109 | 0.39× duck |
| Q24 | 38 | 17 | 0.45× duck |
| Q25 | 968 | 15 | 0.02× duck |
| Q26 | 40 | 17 | 0.43× duck |
| Q27 | 3095 | 745 | 0.24× duck |
| Q28 | 7427 | 6787 | 0.91× duck |
| Q29 | 254 | 14 | 0.06× duck |
| Q30 | 1161 | 294 | 0.25× duck |
| Q31 | 1159 | 361 | 0.31× duck |
| Q32 | 5688 | 3402 | 0.60× duck |
| Q33 | 7023 | 9638 | 0.73× thinDB |
| Q34 | 6799 | 9729 | 0.70× thinDB |
| Q35 | 1175 | 295 | 0.25× duck |
| Q36 | 195 | 23 | 0.12× duck |
| Q37 | 115 | 13 | 0.11× duck |
| Q38 | 26 | 10 | 0.38× duck |
| Q39 | 274 | 47 | 0.17× duck |
| Q40 | 25 | 5 | 0.20× duck |
| Q41 | 23 | 5 | 0.21× duck |
| Q42 | 39 | 6 | 0.15× duck |

**Totals (both DOP12, fair): thinDB 75.2s vs DuckDB 46.6s — DuckDB 1.61× faster overall.** thinDB wins 5/43.

thinDB wins only the high-card string GROUP BY (Q33/Q34) and wildcard-LIKE (Q20/Q21); DuckDB wins the rest, dominating the cheap scalar aggregates where thinDB has high fixed per-query overhead.
