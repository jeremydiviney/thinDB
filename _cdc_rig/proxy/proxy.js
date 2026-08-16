// Truncation-injecting TCP proxy: sits between the CDC connector and MySQL,
// reproducing the WAN failure from the 2026-08-10 incident: the server->client
// stream dies mid-binlog-event, delivering a partial event then EOF
// ("Failed to read remaining N of M bytes").
//
// Modes (switch live via control API, no restart):
//   pass    - forward everything untouched
//   poison  - ANY server->client chunk >= minsize bytes is cut at cutFrac and
//             the connection killed. Every reconnect hits the same rule at the
//             same stream position => deterministic Aug-10 wedge.
//   storm   - each server->client chunk is cut with probability prob
//             (random WAN flakiness).
//
// Control: GET :8123/mode?m=poison&minsize=4096&cutFrac=0.6&prob=0.02
//          GET :8123/       -> {mode, stats}
//          GET :8123/reset  -> zero the counters

const net = require("net");
const http = require("http");

const TARGET_HOST = process.env.TARGET_HOST || "mysql";
const TARGET_PORT = +(process.env.TARGET_PORT || 3306);
const LISTEN_PORT = +(process.env.LISTEN_PORT || 3307);
const CTRL_PORT = +(process.env.CTRL_PORT || 8123);

const mode = { name: "pass", minsize: 4096, cutFrac: 0.6, prob: 0.02 };
const stats = { conns: 0, active: 0, truncations: 0, stalls: 0, bytesS2C: 0, lastTruncAt: null };

net
  .createServer((client) => {
    stats.conns++;
    stats.active++;
    const up = net.connect(TARGET_PORT, TARGET_HOST);
    let dead = false;
    const kill = () => {
      if (dead) return;
      dead = true;
      stats.active--;
      client.destroy();
      up.destroy();
    };
    client.on("error", kill);
    up.on("error", kill);
    client.on("close", kill);
    up.on("close", kill);

    // client -> server: never faulted (auth, queries, dump requests)
    client.on("data", (b) => {
      if (!dead) up.write(b);
    });

    // server -> client: fault injection point
    let stalled = false;
    up.on("data", (b) => {
      if (dead) return;
      stats.bytesS2C += b.length;
      if (stalled) return; // silently eat everything after a stall begins
      let cut = false;
      if ((mode.name === "poison" || mode.name === "stallpoison") && b.length >= mode.minsize) cut = true;
      else if (mode.name === "storm" && Math.random() < mode.prob) cut = true;

      if (cut) {
        stats.truncations++;
        stats.lastTruncAt = new Date().toISOString();
        const keep = Math.max(1, Math.floor(b.length * mode.cutFrac));
        client.write(b.subarray(0, keep));
        if (mode.name === "stallpoison" || (mode.name === "storm" && Math.random() < (mode.stallFrac || 0))) {
          // partial event, then dead silence with the socket held open:
          // the WAN "black hole". Keepalive threads spawn a second reader
          // while this one is still parked mid-event.
          stalled = true;
          stats.stalls++;
          setTimeout(kill, (mode.stallSecs || 90) * 1000);
        } else {
          setTimeout(kill, 5); // partial delivery, then the link "drops"
        }
      } else {
        client.write(b);
      }
    });
  })
  .listen(LISTEN_PORT, "0.0.0.0", () =>
    console.log(`proxy ${LISTEN_PORT} -> ${TARGET_HOST}:${TARGET_PORT}`)
  );

http
  .createServer((req, res) => {
    const u = new URL(req.url, "http://x");
    if (u.pathname === "/mode") {
      const m = u.searchParams.get("m");
      if (m) mode.name = m;
      for (const k of ["minsize", "cutFrac", "prob", "stallSecs", "stallFrac"]) {
        const v = u.searchParams.get(k);
        if (v !== null) mode[k] = +v;
      }
      console.log("mode ->", JSON.stringify(mode));
    } else if (u.pathname === "/reset") {
      stats.conns = 0;
      stats.truncations = 0;
      stats.bytesS2C = 0;
      stats.lastTruncAt = null;
    }
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify({ mode, stats }));
  })
  .listen(CTRL_PORT, "0.0.0.0", () => console.log(`ctrl ${CTRL_PORT}`));
