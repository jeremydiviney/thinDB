import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const REPO_ROOT = resolve(import.meta.dir, "..", "..", "..");

export function serverBinary(): string {
  const suffix = process.platform === "win32" ? ".exe" : "";
  return join(REPO_ROOT, "zig-out", "bin", `thindb-server${suffix}`);
}

export async function makeTmpDir(prefix: string): Promise<{
  path: string;
  cleanup: () => Promise<void>;
}> {
  const path = await mkdtemp(join(tmpdir(), `thindb-bun-${prefix}-`));
  return {
    path,
    cleanup: async () => {
      try {
        await rm(path, { recursive: true, force: true });
      } catch {
        // Windows tends to hold file handles briefly post-shutdown;
        // a single retry covers it.
        await new Promise((r) => setTimeout(r, 100));
        await rm(path, { recursive: true, force: true });
      }
    },
  };
}
