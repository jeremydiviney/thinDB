# Engine V2 ClickBench Shape Catalog

Engine V2 keeps SQL parsing and IR generation unchanged. Each query block is
classified after existing IR rewrites, then routed to one physical shape.

This catalog maps the 43 ClickBench queries to the first V2 shape that should
own them. It is a migration checklist, not a promise that every query is
accepted by the first implementation of that shape.

## Shape Order

1. `group_topn`
2. `group_aggregate`
3. `group_full_sort`
4. `global_aggregate`
5. `scan_topn`
6. `scan_full_sort`
7. `stream_scan`
8. `materialized_input`

The order starts with grouped shapes because that is where the current scaling
work and harness data are strongest.

## ClickBench Mapping

| Shape | Queries | Notes |
| --- | --- | --- |
| `group_topn` | Q8-Q16, Q18, Q21-Q22, Q27-Q28, Q30-Q42 | Main target. Q30-Q32 are the numeric-key harness queries. Q33-Q34 are high-card string keys. Q27-Q28 include HAVING; Q28 has regex compute; Q38-Q42 include OFFSET. |
| `group_aggregate` | Q17 | Grouped query with no ORDER BY. Limit can use arbitrary-group emit semantics. |
| `group_full_sort` | Q7 | GROUP BY with ORDER BY and no LIMIT. |
| `global_aggregate` | Q0-Q6, Q20, Q29 | No group keys. Q4-Q6 use COUNT DISTINCT. Q20 has LIKE filter. Q29 has many derived SUM inputs. |
| `scan_topn` | Q23-Q26 | ORDER BY LIMIT without GROUP BY. Q23 projects `*`; Q24-Q26 are narrower string/order-key cases. |
| `scan_full_sort` | none in canonical ClickBench | Keep the runner for general SQL coverage. |
| `stream_scan` | Q19 | Point-filter projection with no blocking stage. |
| `materialized_input` | none directly in single-block ClickBench | Used after CTE/subquery/join/window blocking boundaries. |

## First Online Shape

Start with `group_topn`, using Q30, Q31, and Q32 as smoke tests:

- Q30: `SearchEngineID, ClientIP`, filtered on `SearchPhrase <> ''`
- Q31: `WatchID, ClientIP`, filtered on `SearchPhrase <> ''`
- Q32: `WatchID, ClientIP`, no filter

These avoid string group-key ownership and COUNT DISTINCT at first, but still
exercise the scan/filter/route/group/top-N shape that the harness optimized.

Smoke criteria before consulting:

- V2 accepts only the intended shape/query subset.
- Results match the existing engine for the selected ClickBench queries.
- Warm DOP 1 and DOP 12 timings are captured against ClickBench data.
- The old engine remains the fallback for every non-accepted query.
