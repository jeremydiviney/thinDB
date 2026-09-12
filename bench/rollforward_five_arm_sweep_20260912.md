# Full rollforward sweep — September 12, 2026

Both Sierra and AirDNA completed all 15 variants and all five methods at explicit DOP 12. Ordinary ThinDB SQL has a lower measured median than StarRocks in all 30 cases. UDF + regions has the lowest median in 27/30 cases. Small gaps are measured rankings, not significance claims.

Values below are median milliseconds from one warmup, five timed executions and one separate fingerprint execution per cell. The campaign contains 150 cells, 750 measured executions, 150 warmups and 150 fingerprints. Clients run on starrocks1 over loopback. Application lookups, SQL generation, temporary/session setup, execution, serialization and packet drain are included; process/module/connection startup, shutdown and fingerprinting are excluded. Final rows are not decoded during timing.

ThinDB uses the private snapshot on port 13311, a fresh service per method/case, DOP 12, a 2 GiB source cache, 4 GiB region pool, 16 GiB query/shared budgets, and a 20 GiB service ceiling. Compaction and operator profiling are disabled. StarRocks uses port 9030 with pipeline_dop=12, one fragment instance, adaptive DOP disabled and query cache disabled, verified on benchmark sessions. Method order rotates and reverses across case blocks.

Sierra (1000049, division 1000142) and AirDNA (1000073, division 1000339) retain the archived b5359d9e application generator, per-case dates, USD and hash range [a,d), meaning buckets a/b/c. SQL + regions adds only the existing KEYED BY declaration; UDF switches and declarations are validated in each saved query, and region engagement is checked for every sample.

AirDNA is not an identical-data cross-engine comparison: StarRocks reads live data and ThinDB reads the frozen snapshot. The source and output differences are recorded below. Sierra source counts match. Sierra plans-simple returns zero rows and crossplans three; those remain empty/selective tests.

## Sierra

| Variant | StarRocks SQL | ThinDB SQL | SQL + regions | Zig UDF | UDF + regions |
|---|---:|---:|---:|---:|---:|
| base simple | 437 | 143 | 118 | 151 | **56** |
| base cross | 527 | 349 | 316 | 307 | **84** |
| expanded simple | 1,902 | 432 | 164 | 455 | **101** |
| expanded cross | 1,961 | 940 | 1,215 | 816 | **105** |
| child simple | 442 | 166 | 124 | 163 | **61** |
| child cross | 531 | 320 | 314 | 323 | **85** |
| interval quarter | 448 | 148 | 125 | 170 | **63** |
| interval annual | 442 | 158 | 126 | 162 | **81** |
| fx latest | 590 | 148 | 122 | 104 | **94** |
| fx average | 585 | 152 | 129 | **111** | 118 |
| noest simple | 223 | 145 | 102 | 147 | **61** |
| plans simple | 717 | 107 | **104** | 136 | 158 |
| crossplans | 745 | 185 | 164 | 174 | **129** |
| detail simple | 498 | 195 | 157 | 240 | **69** |
| detail cross | 693 | 532 | 575 | 430 | **147** |

## AirDNA

| Variant | StarRocks SQL | ThinDB SQL | SQL + regions | Zig UDF | UDF + regions |
|---|---:|---:|---:|---:|---:|
| base simple | 1,748 | 1,026 | 1,024 | 1,028 | **619** |
| base cross | 1,839 | 1,655 | 1,536 | 2,021 | **548** |
| expanded simple | 8,972 | 4,601 | 1,126 | 4,534 | **519** |
| expanded cross | 10,047 | 8,235 | 7,461 | 11,039 | **648** |
| child simple | 1,368 | 1,078 | 1,138 | 1,067 | **604** |
| child cross | 1,930 | 1,696 | 1,573 | 1,994 | **584** |
| interval quarter | 1,526 | 1,466 | 1,076 | 873 | **430** |
| interval annual | 1,544 | 1,432 | 1,292 | 1,130 | **481** |
| fx latest | 1,631 | 1,044 | 1,108 | 863 | **804** |
| fx average | 1,632 | 1,049 | 1,070 | **881** | 897 |
| noest simple | 1,086 | 1,029 | 978 | 778 | **462** |
| plans simple | 2,426 | 1,844 | 1,249 | 1,023 | **638** |
| crossplans | 4,876 | 2,931 | 2,897 | 2,548 | **1,452** |
| detail simple | 3,272 | 2,566 | 2,248 | 1,975 | **1,263** |
| detail cross | 4,033 | 3,436 | 3,658 | 3,173 | **1,490** |

## Sum of the 15 case medians

| Company | StarRocks SQL s | ThinDB SQL s | SQL + regions s | Zig UDF s | UDF + regions s |
|---|---:|---:|---:|---:|---:|
| Sierra | 10.742 | 4.122 | 3.854 | 3.889 | 1.414 |
| AirDNA | 47.931 | 35.088 | 29.434 | 34.927 | 11.442 |

These totals summarize the query medians, not the elapsed duration of the campaign.

## Correctness and source checks

All 120 ThinDB fingerprints match the archived DOP-12 results. All 60 keyed/unkeyed pairs match. SQL and UDF fingerprints match in 28/30 variants; the two AirDNA detail differences are also present in the archived fingerprints and were not introduced by this engine build. Sierra raw cross-engine fingerprints match in 13/15 variants, with both detail cases differing. The earlier ordinary-SQL diagnosis established DATE-versus-DATETIME serialization as the detail-cross discrepancy; this sweep retains raw fingerprint checks rather than repeating that normalization. Raw hashes alone do not establish cross-engine equivalence for those cases.

| Company | Scope | Source table | ThinDB rows | StarRocks rows |
|---|---|---|---:|---:|
| Sierra | simple | invoice_import_amortized | 27,485 | 27,485 |
| Sierra | simple | report_customer_revenue_rollforward | 14,239 | 14,239 |
| Sierra | cross | invoice_import_amortized | 64,186 | 64,186 |
| Sierra | cross | report_customer_revenue_rollforward | 31,558 | 31,558 |
| AirDNA | simple | invoice_import_amortized | 667,609 | 668,408 |
| AirDNA | simple | report_customer_revenue_rollforward | 622,170 | 622,772 |
| AirDNA | cross | invoice_import_amortized | 760,777 | 761,699 |
| AirDNA | cross | report_customer_revenue_rollforward | 655,515 | 656,130 |

AirDNA detail-simple returns 622,169 ThinDB rows versus 622,797 StarRocks rows; detail-cross returns 653,529 versus 654,171. The per-case row counts and every raw comparison are retained in matrix.json. All four ThinDB methods agree on row counts.

## Timing ranges

| Company | Variant | Method | Median ms | Min–max ms |
|---|---|---|---:|---:|
| Sierra | base cross | ThinDB SQL | 348.6 | 340.1–558.1 |
| Sierra | base cross | SQL + regions | 316.1 | 300.9–338.6 |
| Sierra | base cross | StarRocks SQL | 527.5 | 484.3–543.9 |
| Sierra | base cross | UDF + regions | 84.3 | 79.8–127.9 |
| Sierra | base cross | Zig UDF | 306.5 | 300.6–351.8 |
| Sierra | base simple | ThinDB SQL | 143.3 | 139.7–149.9 |
| Sierra | base simple | SQL + regions | 118.1 | 111.7–130.9 |
| Sierra | base simple | StarRocks SQL | 437.1 | 415.0–466.5 |
| Sierra | base simple | UDF + regions | 55.6 | 54.2–111.8 |
| Sierra | base simple | Zig UDF | 151.3 | 149.3–173.4 |
| Sierra | child cross | ThinDB SQL | 319.9 | 310.4–346.4 |
| Sierra | child cross | SQL + regions | 313.8 | 304.0–328.5 |
| Sierra | child cross | StarRocks SQL | 530.6 | 526.3–545.3 |
| Sierra | child cross | UDF + regions | 84.9 | 78.1–116.0 |
| Sierra | child cross | Zig UDF | 323.0 | 314.7–334.6 |
| Sierra | child simple | ThinDB SQL | 165.9 | 153.3–189.5 |
| Sierra | child simple | SQL + regions | 123.7 | 115.2–127.8 |
| Sierra | child simple | StarRocks SQL | 442.2 | 437.1–474.8 |
| Sierra | child simple | UDF + regions | 61.2 | 59.5–109.5 |
| Sierra | child simple | Zig UDF | 163.2 | 158.3–177.0 |
| Sierra | crossplans | ThinDB SQL | 185.4 | 175.7–232.0 |
| Sierra | crossplans | SQL + regions | 163.8 | 149.8–258.4 |
| Sierra | crossplans | StarRocks SQL | 745.2 | 729.1–789.1 |
| Sierra | crossplans | UDF + regions | 128.9 | 114.9–136.4 |
| Sierra | crossplans | Zig UDF | 173.5 | 168.3–241.4 |
| Sierra | detail cross | ThinDB SQL | 532.1 | 473.0–621.4 |
| Sierra | detail cross | SQL + regions | 575.3 | 565.9–663.3 |
| Sierra | detail cross | StarRocks SQL | 693.4 | 683.3–785.2 |
| Sierra | detail cross | UDF + regions | 147.0 | 137.6–232.6 |
| Sierra | detail cross | Zig UDF | 430.2 | 411.2–482.7 |
| Sierra | detail simple | ThinDB SQL | 195.4 | 187.4–209.1 |
| Sierra | detail simple | SQL + regions | 156.8 | 145.0–179.7 |
| Sierra | detail simple | StarRocks SQL | 498.5 | 484.4–507.9 |
| Sierra | detail simple | UDF + regions | 69.4 | 67.4–109.6 |
| Sierra | detail simple | Zig UDF | 239.6 | 196.4–324.9 |
| Sierra | expanded cross | ThinDB SQL | 940.2 | 934.8–961.6 |
| Sierra | expanded cross | SQL + regions | 1214.7 | 1063.8–1376.3 |
| Sierra | expanded cross | StarRocks SQL | 1961.2 | 1931.4–2062.4 |
| Sierra | expanded cross | UDF + regions | 105.5 | 98.2–135.4 |
| Sierra | expanded cross | Zig UDF | 815.7 | 799.6–875.5 |
| Sierra | expanded simple | ThinDB SQL | 432.2 | 421.1–441.3 |
| Sierra | expanded simple | SQL + regions | 164.5 | 135.5–257.8 |
| Sierra | expanded simple | StarRocks SQL | 1902.0 | 1841.7–1931.1 |
| Sierra | expanded simple | UDF + regions | 101.3 | 69.2–145.0 |
| Sierra | expanded simple | Zig UDF | 455.3 | 435.5–541.9 |
| Sierra | fx average | ThinDB SQL | 152.4 | 148.0–165.4 |
| Sierra | fx average | SQL + regions | 128.9 | 117.2–139.6 |
| Sierra | fx average | StarRocks SQL | 584.9 | 564.5–605.5 |
| Sierra | fx average | UDF + regions | 118.4 | 113.9–125.8 |
| Sierra | fx average | Zig UDF | 110.8 | 102.0–118.1 |
| Sierra | fx latest | ThinDB SQL | 148.1 | 145.4–157.3 |
| Sierra | fx latest | SQL + regions | 121.9 | 116.3–138.3 |
| Sierra | fx latest | StarRocks SQL | 589.7 | 577.9–602.1 |
| Sierra | fx latest | UDF + regions | 94.1 | 90.1–107.8 |
| Sierra | fx latest | Zig UDF | 104.4 | 102.8–111.6 |
| Sierra | interval annual | ThinDB SQL | 158.3 | 153.5–165.4 |
| Sierra | interval annual | SQL + regions | 126.1 | 121.5–146.6 |
| Sierra | interval annual | StarRocks SQL | 441.8 | 419.8–466.4 |
| Sierra | interval annual | UDF + regions | 81.1 | 68.2–130.2 |
| Sierra | interval annual | Zig UDF | 162.4 | 156.6–178.5 |
| Sierra | interval quarter | ThinDB SQL | 147.8 | 145.6–157.4 |
| Sierra | interval quarter | SQL + regions | 124.7 | 121.7–134.2 |
| Sierra | interval quarter | StarRocks SQL | 448.0 | 434.0–453.9 |
| Sierra | interval quarter | UDF + regions | 63.3 | 57.8–124.5 |
| Sierra | interval quarter | Zig UDF | 169.9 | 155.4–180.8 |
| Sierra | noest simple | ThinDB SQL | 145.3 | 142.6–150.6 |
| Sierra | noest simple | SQL + regions | 101.6 | 98.6–116.3 |
| Sierra | noest simple | StarRocks SQL | 222.6 | 206.3–229.2 |
| Sierra | noest simple | UDF + regions | 60.9 | 54.6–114.2 |
| Sierra | noest simple | Zig UDF | 147.1 | 142.2–169.4 |
| Sierra | plans simple | ThinDB SQL | 107.4 | 100.5–133.3 |
| Sierra | plans simple | SQL + regions | 104.1 | 89.3–129.5 |
| Sierra | plans simple | StarRocks SQL | 717.2 | 686.0–856.6 |
| Sierra | plans simple | UDF + regions | 157.8 | 108.0–230.6 |
| Sierra | plans simple | Zig UDF | 135.9 | 126.7–188.2 |
| AirDNA | base cross | ThinDB SQL | 1654.5 | 1646.8–1682.6 |
| AirDNA | base cross | SQL + regions | 1536.1 | 1492.7–1647.4 |
| AirDNA | base cross | StarRocks SQL | 1839.3 | 1806.9–1920.2 |
| AirDNA | base cross | UDF + regions | 548.3 | 540.7–614.6 |
| AirDNA | base cross | Zig UDF | 2020.9 | 1945.6–2076.3 |
| AirDNA | base simple | ThinDB SQL | 1026.1 | 997.7–1066.8 |
| AirDNA | base simple | SQL + regions | 1024.0 | 996.9–1101.5 |
| AirDNA | base simple | StarRocks SQL | 1748.3 | 1622.4–1986.7 |
| AirDNA | base simple | UDF + regions | 618.9 | 606.9–634.5 |
| AirDNA | base simple | Zig UDF | 1027.6 | 953.7–1138.4 |
| AirDNA | child cross | ThinDB SQL | 1696.0 | 1583.6–1709.2 |
| AirDNA | child cross | SQL + regions | 1573.5 | 1490.1–1627.1 |
| AirDNA | child cross | StarRocks SQL | 1929.8 | 1819.2–2023.2 |
| AirDNA | child cross | UDF + regions | 584.2 | 542.5–612.5 |
| AirDNA | child cross | Zig UDF | 1993.6 | 1935.8–2023.5 |
| AirDNA | child simple | ThinDB SQL | 1078.5 | 1055.2–1100.3 |
| AirDNA | child simple | SQL + regions | 1137.9 | 1038.6–1207.7 |
| AirDNA | child simple | StarRocks SQL | 1367.6 | 1358.2–1404.0 |
| AirDNA | child simple | UDF + regions | 603.6 | 520.6–688.9 |
| AirDNA | child simple | Zig UDF | 1067.4 | 1003.2–1108.4 |
| AirDNA | crossplans | ThinDB SQL | 2931.2 | 2771.9–3228.3 |
| AirDNA | crossplans | SQL + regions | 2897.3 | 2836.7–3066.8 |
| AirDNA | crossplans | StarRocks SQL | 4876.1 | 4643.6–4914.8 |
| AirDNA | crossplans | UDF + regions | 1452.4 | 1395.0–1790.4 |
| AirDNA | crossplans | Zig UDF | 2548.2 | 2517.6–2745.8 |
| AirDNA | detail cross | ThinDB SQL | 3435.7 | 3421.7–3499.5 |
| AirDNA | detail cross | SQL + regions | 3657.7 | 3474.7–3821.1 |
| AirDNA | detail cross | StarRocks SQL | 4033.3 | 3994.1–4157.2 |
| AirDNA | detail cross | UDF + regions | 1490.2 | 1461.3–1562.5 |
| AirDNA | detail cross | Zig UDF | 3172.9 | 3105.0–3299.1 |
| AirDNA | detail simple | ThinDB SQL | 2565.8 | 2379.6–2861.4 |
| AirDNA | detail simple | SQL + regions | 2248.5 | 2238.5–2337.9 |
| AirDNA | detail simple | StarRocks SQL | 3272.5 | 3215.6–3324.3 |
| AirDNA | detail simple | UDF + regions | 1263.4 | 1249.1–1281.4 |
| AirDNA | detail simple | Zig UDF | 1974.7 | 1967.6–1986.6 |
| AirDNA | expanded cross | ThinDB SQL | 8234.9 | 8116.4–8289.6 |
| AirDNA | expanded cross | SQL + regions | 7460.8 | 7422.8–8360.5 |
| AirDNA | expanded cross | StarRocks SQL | 10046.8 | 9733.0–10123.3 |
| AirDNA | expanded cross | UDF + regions | 648.1 | 618.4–667.5 |
| AirDNA | expanded cross | Zig UDF | 11039.4 | 9894.0–12932.8 |
| AirDNA | expanded simple | ThinDB SQL | 4600.8 | 4384.7–5080.9 |
| AirDNA | expanded simple | SQL + regions | 1126.2 | 1106.9–1184.1 |
| AirDNA | expanded simple | StarRocks SQL | 8972.3 | 8636.5–9045.1 |
| AirDNA | expanded simple | UDF + regions | 519.4 | 506.5–550.3 |
| AirDNA | expanded simple | Zig UDF | 4533.8 | 4388.2–4628.1 |
| AirDNA | fx average | ThinDB SQL | 1049.1 | 1043.2–1130.7 |
| AirDNA | fx average | SQL + regions | 1069.9 | 1058.4–1102.5 |
| AirDNA | fx average | StarRocks SQL | 1632.2 | 1514.3–1779.6 |
| AirDNA | fx average | UDF + regions | 897.2 | 872.0–906.9 |
| AirDNA | fx average | Zig UDF | 880.9 | 869.1–951.5 |
| AirDNA | fx latest | ThinDB SQL | 1043.9 | 1019.7–1089.5 |
| AirDNA | fx latest | SQL + regions | 1107.9 | 1092.4–1144.7 |
| AirDNA | fx latest | StarRocks SQL | 1631.1 | 1576.3–1873.4 |
| AirDNA | fx latest | UDF + regions | 804.1 | 800.1–834.7 |
| AirDNA | fx latest | Zig UDF | 862.5 | 849.6–921.4 |
| AirDNA | interval annual | ThinDB SQL | 1432.4 | 1409.5–1479.7 |
| AirDNA | interval annual | SQL + regions | 1291.6 | 1284.3–1369.1 |
| AirDNA | interval annual | StarRocks SQL | 1543.8 | 1516.6–1715.0 |
| AirDNA | interval annual | UDF + regions | 481.4 | 477.2–536.2 |
| AirDNA | interval annual | Zig UDF | 1130.1 | 1099.6–1215.2 |
| AirDNA | interval quarter | ThinDB SQL | 1466.4 | 1234.3–1531.4 |
| AirDNA | interval quarter | SQL + regions | 1076.0 | 1032.0–1143.6 |
| AirDNA | interval quarter | StarRocks SQL | 1525.9 | 1436.8–1763.1 |
| AirDNA | interval quarter | UDF + regions | 430.4 | 403.0–460.3 |
| AirDNA | interval quarter | Zig UDF | 873.5 | 850.5–918.8 |
| AirDNA | noest simple | ThinDB SQL | 1028.8 | 1008.8–1093.3 |
| AirDNA | noest simple | SQL + regions | 977.6 | 971.4–1068.2 |
| AirDNA | noest simple | StarRocks SQL | 1086.1 | 1056.2–1221.4 |
| AirDNA | noest simple | UDF + regions | 462.1 | 430.2–493.7 |
| AirDNA | noest simple | Zig UDF | 778.2 | 775.5–834.7 |
| AirDNA | plans simple | ThinDB SQL | 1843.6 | 1735.7–1922.3 |
| AirDNA | plans simple | SQL + regions | 1249.2 | 1202.0–1357.5 |
| AirDNA | plans simple | StarRocks SQL | 2426.5 | 2108.5–2616.3 |
| AirDNA | plans simple | UDF + regions | 638.4 | 611.5–688.9 |
| AirDNA | plans simple | Zig UDF | 1023.4 | 996.3–1055.0 |

## Build and operational receipts

Engine SHA256: affae8694249dd49e3b1aeaab26f8f51cf4f5688ecfb21b5d7dd607df4932ee6; Zig 0.16.0, ReleaseFast, x86_64-linux-gnu. The engine patch matches the verified no-estimates column-pruning build. No engine change was made for this sweep.

All 120 benchmark services recorded zero memory-limit/OOM events. Maximum candidate memory was 17.131 GiB. The runner waited for host headroom twice; no settings were relaxed and those waits are outside timings.

Final health at 2026-09-12T21:14:33.231192+00:00: production ThinDB PID 2579063, restart count 0; StarRocks BE PID 848762; CDC RUNNING. The private service is stopped and port 13311 is free. Production was not deployed or restarted.

Artifacts: `.bench-data/full-five-arm-20260912/` locally and `/home/ubuntu/wayroll-bench/full-five-arm-20260912/` on starrocks1. `full-results.tgz` contains the complete portable result bundle. `results/samples.csv` retains every execution; `results/summary.json`, `results/matrix.json`, captured SQL, session settings, logs, memory receipts and source checks retain the audit trail. Reproduction uses `bench/rollforward_five_arm_sweep.py`, its Node client, and `bench/rollforward_five_arm_report.cjs`.
