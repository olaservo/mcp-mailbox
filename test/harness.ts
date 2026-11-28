import { randomBytes } from "node:crypto";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { handle } from "../src/core.ts";
import { MemoryStore } from "../src/memory.ts";
import type { Fetched } from "../src/reader/verify.ts";

export interface Target {
  base: string;
  key: string;
  /** True when the tests run against MAILBOX_URL, such as `wrangler dev`. */
  remote: boolean;
  close(): Promise<void>;
}

/** MAILBOX_URL and MAILBOX_KEY point the tests at a running mailbox. Without them, one starts here in memory. */
export async function startMailbox(opts: { maxStored?: number; maxPerHook?: number; now?: () => number } = {}): Promise<Target> {
  if (process.env.MAILBOX_URL) {
    return { base: process.env.MAILBOX_URL.replace(/\/$/, ""), key: process.env.MAILBOX_KEY ?? "", remote: true, close: async () => {} };
  }
  const key = randomBytes(32).toString("base64url");
  const store = new MemoryStore(opts);
  const now = opts.now;
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", async () => {
      const headers = new Headers();
      for (const [name, value] of Object.entries(req.headers)) {
        if (value !== undefined) headers.set(name, Array.isArray(value) ? value.join(", ") : value);
      }
      const hasBody = req.method !== "GET" && req.method !== "HEAD";
      const request = new Request(`http://${req.headers.host}${req.url}`, { method: req.method, headers, body: hasBody ? Buffer.concat(chunks) : undefined });
      const response = await handle(request, store, { key, now });
      res.writeHead(response.status, Object.fromEntries(response.headers));
      res.end(Buffer.from(await response.arrayBuffer()));
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  return {
    base: `http://127.0.0.1:${port}`,
    key,
    remote: false,
    close: () => new Promise((resolve) => { server.closeAllConnections(); server.close(() => resolve()); }),
  };
}

/** Hooks made by a run against a remote mailbox are removed this long after their last fetch. */
export const TEST_HOOK_IDLE_SECONDS = 3600;

/** The reader's side of the mailbox API. */
export function reader(target: Target) {
  const auth = { authorization: `Bearer ${target.key}` };
  return {
    async createHook(label = "", idleSeconds = target.remote ? TEST_HOOK_IDLE_SECONDS : undefined): Promise<{ token: string; url: string }> {
      const res = await fetch(`${target.base}/hooks`, { method: "POST", headers: auth, body: JSON.stringify({ label, idleSeconds }) });
      if (res.status !== 201) throw new Error(`create hook: ${res.status}`);
      return (await res.json()) as { token: string; url: string };
    },
    deleteHook: (token: string) => fetch(`${target.base}/hooks/${token}`, { method: "DELETE", headers: auth }),
    async deliveries(after: number, query = ""): Promise<Fetched[]> {
      const res = await fetch(`${target.base}/deliveries?after=${after}${query}`, { headers: auth });
      if (res.status !== 200) throw new Error(`deliveries: ${res.status}`);
      return ((await res.json()) as { deliveries: Fetched[] }).deliveries;
    },
    /** The highest sequence number held now, so a test only looks at what it sends. */
    async tip(): Promise<number> {
      let after = 0;
      for (;;) {
        const batch = await this.deliveries(after, "&limit=500");
        if (batch.length === 0) return after;
        after = batch.at(-1)!.seq;
      }
    },
    async ack(upTo: number, hook?: string): Promise<number> {
      const res = await fetch(`${target.base}/ack`, { method: "POST", headers: auth, body: JSON.stringify({ upTo, hook }) });
      return ((await res.json()) as { deleted: number }).deleted;
    },
  };
}
