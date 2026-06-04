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
