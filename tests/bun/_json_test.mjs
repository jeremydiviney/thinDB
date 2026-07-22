// JSON type + extraction regression over the mysql wire. Needs a server on
// :7953 with a WRITABLE scratch data dir (not a shared/production data dir):
//   ./zig-out/bin/thindb-server.exe --data-dir .json-db --mysql-port 7953 \
//     --pg-port 0 --native-port 0 --max-dop 4
import mysql from "mysql2/promise";
const c = await mysql.createConnection({ host: "127.0.0.1", port: 7953, user: "root", password: "", database: "main" });
let failed = 0;

const q = async (label, sql, expectErr = null) => {
  try {
    const [r] = await c.query({ sql, timeout: 60000 });
    if (expectErr) { console.log(`${label}: FAIL (expected ${expectErr}, got OK)`); failed++; }
    else console.log(`${label}: OK`);
    return r;
  } catch (e) {
    const msg = e.sqlMessage || e.code || e.message;
    if (expectErr && msg.includes(expectErr)) console.log(`${label}: OK (${msg})`);
    else { console.log(`${label}: FAIL (${msg})`); failed++; }
    return null;
  }
};

// Compare a single-column single-row scalar result.
const scalar = async (label, sql, want) => {
  const r = await q(label + "-run", sql);
  if (!r) { failed++; return; }
  const got = r.length ? String(Object.values(r[0])[0]) : "<none>";
  if (got === String(want)) console.log(`${label}: OK`);
  else { console.log(`${label}: FAIL (want '${want}', got '${got}')`); failed++; }
};

await q("drop", "DROP TABLE IF EXISTS jt");
await q("create", "CREATE TABLE jt (id INT, doc JSON, PRIMARY KEY (id))");
await q(
  "insert",
  `INSERT INTO jt (id, doc) VALUES
    (1, '{"name": "alice", "age": 30, "city": "NYC", "tags": ["a","b","c"], "addr": {"zip": "10001"}}'),
    (2, '{"name": "bob", "age": 25, "city": "LA", "tags": [], "addr": {"zip": "90001"}}'),
    (3, '{"name": "carol", "age": 41, "city": "NYC", "tags": ["x"], "addr": {"zip": "10002"}}')`,
);

// JSON_EXTRACT returns a JSON-typed column; the mysql2 client auto-parses
// it (245 = MYSQL_TYPE_JSON), so a JSON string arrives as a JS string, a
// JSON number as a JS number.
await scalar("extract-str", "SELECT JSON_EXTRACT(doc, '$.name') FROM jt WHERE id = 1", "alice");
await scalar("extract-int", "SELECT JSON_EXTRACT(doc, '$.age') FROM jt WHERE id = 1", "30");
await scalar("extract-nested", "SELECT JSON_EXTRACT(doc, '$.addr.zip') FROM jt WHERE id = 1", "10001");
await scalar("extract-arr-elem", "SELECT JSON_EXTRACT(doc, '$.tags[1]') FROM jt WHERE id = 1", "b");

// -> is JSON_EXTRACT sugar; ->> is JSON_VALUE (unquoted text).
await scalar("arrow", "SELECT doc->'$.city' FROM jt WHERE id = 2", "LA");
await scalar("arrow2", "SELECT doc->>'$.city' FROM jt WHERE id = 2", "LA");
await scalar("arrow-chain", "SELECT doc->>'$.addr.zip' FROM jt WHERE id = 3", "10002");

// JSON_VALUE unquotes; missing path -> NULL.
await scalar("value", "SELECT JSON_VALUE(doc, '$.name') FROM jt WHERE id = 3", "carol");
await scalar("value-missing", "SELECT COALESCE(JSON_VALUE(doc, '$.nope'), 'MISSING') FROM jt WHERE id = 1", "MISSING");

// JSON_UNQUOTE on an already-extracted string.
await scalar("unquote", "SELECT JSON_UNQUOTE(JSON_EXTRACT(doc, '$.name')) FROM jt WHERE id = 1", "alice");

// JSON_TYPE / JSON_LENGTH / JSON_VALID.
await scalar("type-obj", "SELECT JSON_TYPE(doc) FROM jt WHERE id = 1", "OBJECT");
await scalar("type-str", "SELECT JSON_TYPE(JSON_EXTRACT(doc, '$.name')) FROM jt WHERE id = 1", "STRING");
await scalar("type-arr", "SELECT JSON_TYPE(JSON_EXTRACT(doc, '$.tags')) FROM jt WHERE id = 1", "ARRAY");
await scalar("length-arr", "SELECT JSON_LENGTH(JSON_EXTRACT(doc, '$.tags')) FROM jt WHERE id = 1", "3");
await scalar("length-obj", "SELECT JSON_LENGTH(doc) FROM jt WHERE id = 2", "5");
await scalar("valid-true", "SELECT JSON_VALID(doc) FROM jt WHERE id = 1", "1");
await scalar("valid-false", "SELECT JSON_VALID('{bad json') FROM jt WHERE id = 1", "0");

// Filtering on an extracted value directly in a WHERE predicate. `->>` lowers
// to JSON_VALUE(doc, path), a scalar function of a column; the scan-select
// builder runs that derived Compute before the Filter (see where_expr_test).
const filtered = await q("where", "SELECT id FROM jt WHERE doc->>'$.city' = 'NYC' ORDER BY id");
const ids = filtered ? filtered.map((r) => r.id).join(",") : "";
if (ids === "1,3") console.log("where-filter: OK");
else { console.log(`where-filter: FAIL (want '1,3', got '${ids}')`); failed++; }

// JSON_CONTAINS structural containment.
await scalar("contains-arr", "SELECT JSON_CONTAINS(JSON_EXTRACT(doc,'$.tags'), '[\"a\"]') FROM jt WHERE id = 1", "1");
await scalar("contains-obj", "SELECT JSON_CONTAINS(doc, '{\"city\":\"NYC\"}') FROM jt WHERE id = 1", "1");
await scalar("contains-no", "SELECT JSON_CONTAINS(doc, '{\"city\":\"SF\"}') FROM jt WHERE id = 1", "0");

// JSON_KEYS returns the object's keys as a JSON array (mysql2 parses it).
// JSONB canonicalizes objects by sorting keys (like PostgreSQL jsonb), so the
// keys come back in sorted order regardless of insertion order.
const keys = await q("keys-run", "SELECT JSON_KEYS(doc) AS k FROM jt WHERE id = 1");
const kk = keys && keys.length ? JSON.stringify(keys[0].k) : "<none>";
if (kk === '["addr","age","city","name","tags"]') console.log("keys: OK");
else { console.log(`keys: FAIL (got ${kk})`); failed++; }

// CAST(... AS JSON) validates + normalizes.
await scalar("cast-json", "SELECT JSON_TYPE(CAST('[1,2,3]' AS JSON)) FROM jt WHERE id = 1", "ARRAY");

// A malformed doc extracts to SQL NULL, not an error.
await scalar("bad-doc-null", "SELECT COALESCE(JSON_VALUE('nonsense', '$.a'), 'NIL') FROM jt WHERE id = 1", "NIL");

// JSON column reports as JSON on the wire (column metadata).
const meta = await q("meta", "SELECT doc FROM jt WHERE id = 1");
console.log(meta ? "meta: OK" : "meta: FAIL");

await q("drop-final", "DROP TABLE IF EXISTS jt");

console.log(failed === 0 ? "\nALL JSON TESTS PASSED" : `\n${failed} JSON TEST(S) FAILED`);
await c.end();
process.exit(failed === 0 ? 0 : 1);
