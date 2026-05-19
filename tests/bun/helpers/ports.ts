import { createServer } from "node:net";

/**
 * Picks an ephemeral TCP port by binding to :0 and immediately closing.
 * Race window: another process can claim the port between close and our
 * spawn. For local testing the race is negligible; on CI you may want
 * to retry the whole server-spawn cycle if the bind fails.
 */
export async function pickFreePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const srv = createServer();
    srv.unref();
    srv.once("error", reject);
    srv.listen(0, () => {
      const addr = srv.address();
      if (addr === null || typeof addr === "string") {
        srv.close();
        reject(new Error("unexpected listen address"));
        return;
      }
      const port = addr.port;
      srv.close(() => resolve(port));
    });
  });
}

export async function pickThreeFreePorts(): Promise<{
  mysql: number;
  pg: number;
  native: number;
}> {
  return {
    mysql: await pickFreePort(),
    pg: await pickFreePort(),
    native: await pickFreePort(),
  };
}
