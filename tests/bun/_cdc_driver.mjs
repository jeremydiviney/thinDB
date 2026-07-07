// CDC test driver. Talks to the MySQL source (host :13307) and the thinDB sink
// (host :13306). Subcommands: seed <N> | live <from> <count> | mutate | check.
import mysql from "mysql2/promise";

const src = () => mysql.createConnection({ host: "127.0.0.1", port: 13307, user: "root", password: "root", database: "src" });
const thin = () => mysql.createConnection({ host: "127.0.0.1", port: 13306, user: "root", password: "", database: "main" });
const cmd = process.argv[2];

function rowVals(id) {
  return [id, (id * 7) % 1000, id * 10 + (id % 13), ["new", "paid", "shipped", "cancelled"][id % 4]];
}

if (cmd === "seed") {
  const n = Number(process.argv[3] ?? "1000");
  const c = await src();
  await c.query("DROP TABLE IF EXISTS orders");
  await c.query("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, status VARCHAR(16))");
  const vals = [];
  for (let i = 1; i <= n; i++) vals.push(rowVals(i));
  // chunked multi-row insert
  for (let i = 0; i < vals.length; i += 500) {
    const chunk = vals.slice(i, i + 500);
    await c.query("INSERT INTO orders (id,customer_id,amount,status) VALUES ?", [chunk]);
  }
  console.log(`seeded ${n} rows into src.orders`);
  await c.end();
} else if (cmd === "live") {
  const from = Number(process.argv[3]), count = Number(process.argv[4]);
  const c = await src();
  for (let i = from; i < from + count; i++) await c.query("INSERT INTO orders (id,customer_id,amount,status) VALUES (?,?,?,?)", rowVals(i));
  console.log(`live-inserted ids ${from}..${from + count - 1} into src.orders`);
  await c.end();
} else if (cmd === "mutate") {
  const c = await src();
  await c.query("UPDATE orders SET amount = 999999, status = 'paid' WHERE id = 1");
  await c.query("DELETE FROM orders WHERE id = 2");
  console.log("mutated: UPDATE id=1 (amount=999999), DELETE id=2");
  await c.end();
} else if (cmd === "check") {
  const c = await thin();
  const [[{ n }]] = [await c.query("SELECT COUNT(*) AS n FROM orders")].map((r) => r[0]);
  const [r1] = await c.query("SELECT id,amount,status FROM orders WHERE id = 1");
  const [r2] = await c.query("SELECT id FROM orders WHERE id = 2");
  console.log(`thinDB main.orders COUNT = ${n}`);
  console.log(`  id=1 -> ${JSON.stringify(r1[0] ?? null)}`);
  console.log(`  id=2 -> ${r2.length ? "present" : "DELETED"}`);
  await c.end();
} else {
  console.log("usage: seed <N> | live <from> <count> | mutate | check");
  process.exit(1);
}
