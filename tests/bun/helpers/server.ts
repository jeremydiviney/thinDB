import { spawn, type Subprocess } from "bun";
import { existsSync } from "node:fs";
import { makeTmpDir, serverBinary } from "./paths.ts";
import { pickThreeFreePorts } from "./ports.ts";

export type ServerConfig = {
  /** Each set to a free port; pass 0 in overrides to disable a wire. */
  overrides?: Partial<{ mysql: number; pg: number; native: number }>;
  /** Bind address; default 127.0.0.1 (NOT 0.0.0.0 — keep tests local-only). */
  bind?: string;
  /** Identifying string for tmp-dir naming. */
  label?: string;
  /** When set, server requires this password on the MySQL wire
   * (mysql_native_password). Without it the wire is in trust mode. */
  mysqlPassword?: string;
  /** When set, server requires this password on the PG wire
   * (SCRAM-SHA-256). Without it the wire is in trust mode. */
  pgPassword?: string;
};

export type ServerHandle = {
  /** TCP ports we actually told the server to listen on. */
  ports: { mysql: number; pg: number; native: number };
  /** Test asserts use this; default 127.0.0.1. */
  bind: string;
  /** Tear down the server + data-dir. */
  close: () => Promise<void>;
};

const BIN = serverBinary();

export async function startServer(cfg: ServerConfig = {}): Promise<ServerHandle> {
  if (!existsSync(BIN)) {
    throw new Error(
      `thindb-server binary not found at ${BIN}. ` +
        `Run \`zig build\` from the repo root first.`,
    );
  }

  const free = await pickThreeFreePorts();
  const mysql = cfg.overrides?.mysql ?? free.mysql;
  const pg = cfg.overrides?.pg ?? free.pg;
  const native = cfg.overrides?.native ?? free.native;
  const bind = cfg.bind ?? "127.0.0.1";

  const dataDir = await makeTmpDir(cfg.label ?? "srv");

  const args = [
    "--data-dir",
    dataDir.path,
    "--mysql-port",
    String(mysql),
    "--pg-port",
    String(pg),
    "--native-port",
    String(native),
    "--bind",
    bind,
  ];
  if (cfg.mysqlPassword !== undefined) {
    args.push("--mysql-password", cfg.mysqlPassword);
  }
  if (cfg.pgPassword !== undefined) {
    args.push("--pg-password", cfg.pgPassword);
  }

  const proc = spawn({
    cmd: [BIN, ...args],
    stdout: "pipe",
    stderr: "pipe",
    stdin: "ignore",
  });

  // Wait for startup banner — count expected "listening on" lines.
  const expected = [mysql, pg, native].filter((p) => p !== 0).length;
  try {
    await waitForListeners(proc, expected, 10_000);
  } catch (err) {
    try {
      proc.kill();
    } catch {
      // process may already be exited
    }
    await dataDir.cleanup().catch(() => undefined);
    throw err;
  }

  // Drain stdout/stderr in the background after handshake so the child
  // pipe buffer never fills up (Windows blocks the server otherwise).
  drainStream(proc.stdout);
  drainStream(proc.stderr);

  return {
    ports: { mysql, pg, native },
    bind,
    close: async () => {
      try {
        proc.kill();
      } catch {
        // already exited
      }
      await proc.exited.catch(() => undefined);
      await dataDir.cleanup();
    },
  };
}

async function waitForListeners(
  proc: Subprocess<"ignore", "pipe", "pipe">,
  expected: number,
  timeoutMs: number,
): Promise<void> {
  if (expected === 0) return;
  const reader = proc.stdout.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  let seen = 0;
  const deadline = Date.now() + timeoutMs;
  try {
    while (seen < expected) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        throw new Error(
          `server didn't print ${expected} listener line(s) within ${timeoutMs}ms`,
        );
      }
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<{ value: undefined; done: true }>((resolve) => {
        timer = setTimeout(() => resolve({ value: undefined, done: true }), remaining);
      });
      const chunk = await Promise.race([reader.read(), timeout]);
      if (timer !== undefined) clearTimeout(timer);
      if (chunk.done) {
        if (chunk.value === undefined && seen < expected) {
          // The timeout side resolved.
          continue;
        }
        throw new Error("server stdout closed before listeners were ready");
      }
      buf += decoder.decode(chunk.value);
      let nl = buf.indexOf("\n");
      while (nl !== -1) {
        const line = buf.slice(0, nl);
        buf = buf.slice(nl + 1);
        if (line.includes("listening on")) seen += 1;
        nl = buf.indexOf("\n");
      }
    }
  } finally {
    reader.releaseLock();
  }
}

function drainStream(stream: ReadableStream<Uint8Array>): void {
  // Read-and-discard loop; tolerate the stream closing.
  const reader = stream.getReader();
  void (async () => {
    try {
      while (true) {
        const { done } = await reader.read();
        if (done) return;
      }
    } catch {
      // pipe closed; ignore
    } finally {
      try {
        reader.releaseLock();
      } catch {
        // already released
      }
    }
  })();
}
