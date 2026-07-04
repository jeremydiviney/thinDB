// SEPARABLE BY ClickBench: every query rewritten (where the contract allows)
// as   WITH base AS (raw cols + WHERE), s AS (aggregation SEPARABLE BY (k)),
//      SELECT ... ORDER/LIMIT/re-aggregate outside the block
// so the slice range predicate fuses into the scan below the aggregate.
// Slice keys prefer the order key (CounterID leading = zone-map pruning);
// otherwise the highest-NDV grouping column. Global aggregates re-aggregate
// manually outside (SUM of per-slice COUNT/SUM, MIN of MIN, ...);
// COUNT(DISTINCT x) slices BY x so per-slice distinct counts add.
// Queries with derived grouping keys / overflow hazards stay as-is.
//   PORT=7880 bun _sepbench.mjs
import mysql from "mysql2/promise";
import { readFileSync } from "fs";
const port = Number(process.env.PORT || 7880);
const qs = readFileSync("C:/development/thinDB/bench/clickbench/queries.sql", "utf8")
  .split("\n").map(s => s.trim()).filter(Boolean);

const q29_inner = Array.from({ length: 90 }, (_, i) => `SUM(ResolutionWidth + ${i}) AS s${i}`).join(", ");
const q29_outer = Array.from({ length: 90 }, (_, i) => `SUM(s${i})`).join(", ");

// null = run the original (separability inapplicable); {sql, cmp} otherwise.
// cmp: "exact" rows-as-multiset; "tol" numeric tolerance; "count" row count only
// (top-N over ties / OFFSET pages are order-nondeterministic across plans).
const T = [
  null, // Q0 COUNT(*): metadata handler, nothing to slice
  { sql: `WITH base AS (SELECT CounterID FROM hits WHERE AdvEngineID <> 0), s AS (SELECT COUNT(*) AS c FROM base SEPARABLE BY (CounterID)) SELECT SUM(c) FROM s`, cmp: "exact" },
  { sql: `WITH base AS (SELECT CounterID, AdvEngineID, ResolutionWidth FROM hits), s AS (SELECT SUM(AdvEngineID) AS a, COUNT(*) AS c, SUM(ResolutionWidth) AS r FROM base SEPARABLE BY (CounterID)) SELECT SUM(a), SUM(c), SUM(r) * 1.0 / SUM(c) FROM s`, cmp: "tol" },
  null, // Q3 AVG(UserID): SUM(UserID) over 100M would overflow i64
  { sql: `WITH base AS (SELECT UserID FROM hits), s AS (SELECT COUNT(DISTINCT UserID) AS u FROM base SEPARABLE BY (UserID)) SELECT SUM(u) FROM s`, cmp: "exact" },
  { sql: `WITH base AS (SELECT SearchPhrase FROM hits), s AS (SELECT COUNT(DISTINCT SearchPhrase) AS u FROM base SEPARABLE BY (SearchPhrase)) SELECT SUM(u) FROM s`, cmp: "exact" },
  { sql: `WITH base AS (SELECT CounterID, EventDate FROM hits), s AS (SELECT MIN(EventDate) AS mn, MAX(EventDate) AS mx FROM base SEPARABLE BY (CounterID)) SELECT MIN(mn), MAX(mx) FROM s`, cmp: "exact" },
  { sql: `WITH base AS (SELECT AdvEngineID FROM hits WHERE AdvEngineID <> 0), s AS (SELECT AdvEngineID, COUNT(*) AS c FROM base GROUP BY AdvEngineID SEPARABLE BY (AdvEngineID)) SELECT AdvEngineID, c FROM s ORDER BY c DESC`, cmp: "exact" },
  { sql: `WITH base AS (SELECT RegionID, UserID FROM hits), s AS (SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM base GROUP BY RegionID SEPARABLE BY (RegionID)) SELECT RegionID, u FROM s ORDER BY u DESC LIMIT 10`, cmp: "exact" },
  { sql: `WITH base AS (SELECT RegionID, AdvEngineID, ResolutionWidth, UserID FROM hits), s AS (SELECT RegionID, SUM(AdvEngineID) AS a, COUNT(*) AS c, AVG(ResolutionWidth) AS r, COUNT(DISTINCT UserID) AS u FROM base GROUP BY RegionID SEPARABLE BY (RegionID)) SELECT RegionID, a, c, r, u FROM s ORDER BY c DESC LIMIT 10`, cmp: "tol" },
  { sql: `WITH base AS (SELECT MobilePhoneModel, UserID FROM hits WHERE MobilePhoneModel <> ''), s AS (SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM base GROUP BY MobilePhoneModel SEPARABLE BY (MobilePhoneModel)) SELECT MobilePhoneModel, u FROM s ORDER BY u DESC LIMIT 10`, cmp: "exact" },
  { sql: `WITH base AS (SELECT MobilePhone, MobilePhoneModel, UserID FROM hits WHERE MobilePhoneModel <> ''), s AS (SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM base GROUP BY MobilePhone, MobilePhoneModel SEPARABLE BY (MobilePhoneModel)) SELECT MobilePhone, MobilePhoneModel, u FROM s ORDER BY u DESC LIMIT 10`, cmp: "exact" },
  { sql: `WITH base AS (SELECT SearchPhrase FROM hits WHERE SearchPhrase <> ''), s AS (SELECT SearchPhrase, COUNT(*) AS c FROM base GROUP BY SearchPhrase SEPARABLE BY (SearchPhrase)) SELECT SearchPhrase, c FROM s ORDER BY c DESC LIMIT 10`, cmp: "exact" },
  { sql: `WITH base AS (SELECT SearchPhrase, UserID FROM hits WHERE SearchPhrase <> ''), s AS (SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM base GROUP BY SearchPhrase SEPARABLE BY (SearchPhrase)) SELECT SearchPhrase, u FROM s ORDER BY u DESC LIMIT 10`, cmp: "exact" },
  { sql: `WITH base AS (SELECT SearchEngineID, SearchPhrase FROM hits WHERE SearchPhrase <> ''), s AS (SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM base GROUP BY SearchEngineID, SearchPhrase SEPARABLE BY (SearchPhrase)) SELECT SearchEngineID, SearchPhrase, c FROM s ORDER BY c DESC LIMIT 10`, cmp: "exact" },
  { sql: `WITH base AS (SELECT UserID FROM hits), s AS (SELECT UserID, COUNT(*) AS c FROM base GROUP BY UserID SEPARABLE BY (UserID)) SELECT UserID, c FROM s ORDER BY c DESC LIMIT 10`, cmp: "exact" },
  { sql: `WITH base AS (SELECT UserID, SearchPhrase FROM hits), s AS (SELECT UserID, SearchPhrase, COUNT(*) AS c FROM base GROUP BY UserID, SearchPhrase SEPARABLE BY (UserID)) SELECT UserID, SearchPhrase, c FROM s ORDER BY c DESC LIMIT 10`, cmp: "exact" },
  { sql: `WITH base AS (SELECT UserID, SearchPhrase FROM hits), s AS (SELECT UserID, SearchPhrase, COUNT(*) AS c FROM base GROUP BY UserID, SearchPhrase SEPARABLE BY (UserID)) SELECT UserID, SearchPhrase, c FROM s LIMIT 10`, cmp: "count" },
  { sql: `WITH base AS (SELECT UserID, EventTime, SearchPhrase FROM hits), s AS (SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) AS c FROM base GROUP BY UserID, m, SearchPhrase SEPARABLE BY (UserID)) SELECT UserID, m, SearchPhrase, c FROM s ORDER BY c DESC LIMIT 10`, cmp: "exact" },
  null, // Q19 point lookup: already ~ms via pruning
  { sql: `WITH base AS (SELECT CounterID FROM hits WHERE URL LIKE '%google%'), s AS (SELECT COUNT(*) AS c FROM base SEPARABLE BY (CounterID)) SELECT SUM(c) FROM s`, cmp: "exact" },
  { sql: `WITH base AS (SELECT SearchPhrase, URL FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> ''), s AS (SELECT SearchPhrase, MIN(URL) AS mu, COUNT(*) AS c FROM base GROUP BY SearchPhrase SEPARABLE BY (SearchPhrase)) SELECT SearchPhrase, mu, c FROM s ORDER BY c DESC LIMIT 10`, cmp: "exact" },
  { sql: `WITH base AS (SELECT SearchPhrase, URL, Title, UserID FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''), s AS (SELECT SearchPhrase, MIN(URL) AS mu, MIN(Title) AS mt, COUNT(*) AS c, COUNT(DISTINCT UserID) AS u FROM base GROUP BY SearchPhrase SEPARABLE BY (SearchPhrase)) SELECT SearchPhrase, mu, mt, c, u FROM s ORDER BY c DESC LIMIT 10`, cmp: "exact" },
  { sql: `WITH base AS (SELECT * FROM hits WHERE URL LIKE '%google%'), s AS (SELECT * FROM base ORDER BY EventTime LIMIT 10 SEPARABLE BY (CounterID)) SELECT * FROM s ORDER BY EventTime LIMIT 10`, cmp: "count" },
  { sql: `WITH base AS (SELECT SearchPhrase, EventTime, CounterID FROM hits WHERE SearchPhrase <> ''), s AS (SELECT SearchPhrase, EventTime FROM base ORDER BY EventTime LIMIT 10 SEPARABLE BY (CounterID)) SELECT SearchPhrase FROM s ORDER BY EventTime LIMIT 10`, cmp: "count" },
  { sql: `WITH base AS (SELECT SearchPhrase FROM hits WHERE SearchPhrase <> ''), s AS (SELECT SearchPhrase FROM base ORDER BY SearchPhrase LIMIT 10 SEPARABLE BY (SearchPhrase)) SELECT SearchPhrase FROM s ORDER BY SearchPhrase LIMIT 10`, cmp: "exact" },
  { sql: `WITH base AS (SELECT SearchPhrase, EventTime, CounterID FROM hits WHERE SearchPhrase <> ''), s AS (SELECT SearchPhrase, EventTime FROM base ORDER BY EventTime, SearchPhrase LIMIT 10 SEPARABLE BY (CounterID)) SELECT SearchPhrase FROM s ORDER BY EventTime, SearchPhrase LIMIT 10`, cmp: "count" },
  { sql: `WITH base AS (SELECT CounterID, URL FROM hits WHERE URL <> ''), s AS (SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM base GROUP BY CounterID HAVING COUNT(*) > 100000 SEPARABLE BY (CounterID)) SELECT CounterID, l, c FROM s ORDER BY l DESC LIMIT 25`, cmp: "tol" },
  null, // Q28 derived regex grouping key: no physical slice column respects the groups
  { sql: `WITH base AS (SELECT CounterID, ResolutionWidth FROM hits), s AS (SELECT ${q29_inner} FROM base SEPARABLE BY (CounterID)) SELECT ${q29_outer} FROM s`, cmp: "exact" },
  { sql: `WITH base AS (SELECT SearchEngineID, ClientIP, IsRefresh, ResolutionWidth FROM hits WHERE SearchPhrase <> ''), s AS (SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh) AS sr, AVG(ResolutionWidth) AS ar FROM base GROUP BY SearchEngineID, ClientIP SEPARABLE BY (ClientIP)) SELECT SearchEngineID, ClientIP, c, sr, ar FROM s ORDER BY c DESC LIMIT 10`, cmp: "tol" },
  { sql: `WITH base AS (SELECT WatchID, ClientIP, IsRefresh, ResolutionWidth FROM hits WHERE SearchPhrase <> ''), s AS (SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh) AS sr, AVG(ResolutionWidth) AS ar FROM base GROUP BY WatchID, ClientIP SEPARABLE BY (WatchID)) SELECT WatchID, ClientIP, c, sr, ar FROM s ORDER BY c DESC LIMIT 10`, cmp: "tol" },
  { sql: `WITH base AS (SELECT WatchID, ClientIP, IsRefresh, ResolutionWidth FROM hits), s AS (SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh) AS sr, AVG(ResolutionWidth) AS ar FROM base GROUP BY WatchID, ClientIP SEPARABLE BY (WatchID)) SELECT WatchID, ClientIP, c, sr, ar FROM s ORDER BY c DESC LIMIT 10`, cmp: "tol" },
  { sql: `WITH base AS (SELECT URL FROM hits), s AS (SELECT URL, COUNT(*) AS c FROM base GROUP BY URL SEPARABLE BY (URL)) SELECT URL, c FROM s ORDER BY c DESC LIMIT 10`, cmp: "exact" },
  null, // Q34 GROUP BY ordinal-literal: leave as-is
  { sql: `WITH base AS (SELECT ClientIP FROM hits), s AS (SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM base GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 SEPARABLE BY (ClientIP)) SELECT * FROM s ORDER BY c DESC LIMIT 10`, cmp: "exact" },
  { sql: `WITH base AS (SELECT URL FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> ''), s AS (SELECT URL, COUNT(*) AS PageViews FROM base GROUP BY URL SEPARABLE BY (URL)) SELECT URL, PageViews FROM s ORDER BY PageViews DESC LIMIT 10`, cmp: "exact" },
  { sql: `WITH base AS (SELECT Title FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> ''), s AS (SELECT Title, COUNT(*) AS PageViews FROM base GROUP BY Title SEPARABLE BY (Title)) SELECT Title, PageViews FROM s ORDER BY PageViews DESC LIMIT 10`, cmp: "exact" },
  { sql: `WITH base AS (SELECT URL FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0), s AS (SELECT URL, COUNT(*) AS PageViews FROM base GROUP BY URL SEPARABLE BY (URL)) SELECT URL, PageViews FROM s ORDER BY PageViews DESC LIMIT 10 OFFSET 1000`, cmp: "count" },
  { sql: `WITH base AS (SELECT TraficSourceID, SearchEngineID, AdvEngineID, Referer, URL FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0), s AS (SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM base GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst SEPARABLE BY (URL)) SELECT TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst, PageViews FROM s ORDER BY PageViews DESC LIMIT 10 OFFSET 1000`, cmp: "count" },
  { sql: `WITH base AS (SELECT URLHash, EventDate FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465), s AS (SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM base GROUP BY URLHash, EventDate SEPARABLE BY (URLHash)) SELECT URLHash, EventDate, PageViews FROM s ORDER BY PageViews DESC LIMIT 10 OFFSET 100`, cmp: "count" },
  { sql: `WITH base AS (SELECT WindowClientWidth, WindowClientHeight FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622), s AS (SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM base GROUP BY WindowClientWidth, WindowClientHeight SEPARABLE BY (WindowClientWidth)) SELECT WindowClientWidth, WindowClientHeight, PageViews FROM s ORDER BY PageViews DESC LIMIT 10 OFFSET 10000`, cmp: "count" },
  null, // Q42 DATE_TRUNC grouping key (derived)
];

const c = await mysql.createConnection({ host: "127.0.0.1", port, user: "root", password: "", database: "clickbench_fsst__public", rowsAsArray: true });

function canon(rows) {
  return rows.map(r => JSON.stringify(r.map(v => typeof v === "number" && !Number.isInteger(v) ? v.toFixed(4) : String(v)))).sort();
}
function valuesMatch(a, b, mode) {
  if (mode === "count") return a.length === b.length;
  const ca = canon(a), cb = canon(b);
  if (ca.length !== cb.length) return false;
  if (mode === "exact") return ca.every((r, i) => r === cb[i]);
  for (let i = 0; i < ca.length; i++) {
    const ra = JSON.parse(ca[i]), rb = JSON.parse(cb[i]);
    for (let j = 0; j < ra.length; j++) {
      const na = Number(ra[j]), nb = Number(rb[j]);
      if (Number.isFinite(na) && Number.isFinite(nb)) {
        if (Math.abs(na - nb) > Math.max(1, Math.abs(na)) * 1e-6) return false;
      } else if (ra[j] !== rb[j]) return false;
    }
  }
  return true;
}

async function run(sql) {
  const s = performance.now();
  try {
    const [rows] = await c.query({ sql, timeout: 180000 });
    return { ms: performance.now() - s, rows };
  } catch (e) {
    return { ms: performance.now() - s, err: e.sqlMessage || e.code || e.message };
  }
}

// Warm both variants once, then time both.
for (let i = 0; i < qs.length; i++) { await run(qs[i]); if (T[i]) await run(T[i].sql); }

let base_tot = 0, sep_tot = 0, transformed = 0, wins = 0;
console.log("Q#   baseline    separable   speedup  values");
for (let i = 0; i < qs.length; i++) {
  const b = await run(qs[i]);
  base_tot += b.ms;
  if (!T[i]) {
    sep_tot += b.ms;
    console.log(`Q${String(i).padStart(2)}  ${b.ms.toFixed(0).padStart(8)}ms   (as-is)`);
    continue;
  }
  transformed++;
  const s = await run(T[i].sql);
  sep_tot += s.ms;
  let v;
  if (b.err || s.err) v = `ERR ${s.err || b.err}`;
  else v = valuesMatch(b.rows, s.rows, T[i].cmp) ? "MATCH" : "MISMATCH";
  const sp = b.ms / s.ms;
  if (sp > 1.05 && v === "MATCH") wins++;
  console.log(`Q${String(i).padStart(2)}  ${b.ms.toFixed(0).padStart(8)}ms  ${s.ms.toFixed(0).padStart(8)}ms  ${sp.toFixed(2).padStart(6)}x  ${v}`);
}
console.log(`----\nbaseline total ${(base_tot / 1000).toFixed(2)}s | separable total ${(sep_tot / 1000).toFixed(2)}s | ${transformed} transformed, ${wins} wins >5%`);
await c.end();
