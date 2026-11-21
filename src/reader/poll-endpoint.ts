import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import type { EventBody } from "./reader.ts";

interface PollParams {
  name?: unknown;
  cursor?: unknown;
  maxEvents?: unknown;
  maxAgeMs?: unknown;
}

const INVALID_PARAMS = -32602;
const METHOD_NOT_FOUND = -32601;
const POLL_MS = 1000;

class RpcError extends Error {
  readonly code: number;
  constructor(code: number, message: string) {
    super(message);
    this.code = code;
  }
}

/**
 * Serves the reader's events as SEP-3415 poll delivery (`events/list`, `events/poll`), for a client that
 * polls and has no webhook support. The events are kept in memory. Cursors are `<epoch>:<seq>` and the
 * epoch is per process, so a cursor from before a restart polls as `truncated`.
 */
export class PollEndpoint {
  private readonly names: string[];
  private readonly capacity: number;
  private readonly epoch = randomUUID().slice(0, 8);
  private readonly entries: { seq: number; event: EventBody }[] = [];
  private seq = 0;

  constructor(names: string[], capacity = 1000) {
    this.names = names;
    this.capacity = capacity;
  }

  /** Keeps an event for clients to poll. Use as the reader's `handle`. */
  add(event: EventBody): void {
    const { eventId, name, timestamp, data } = event;
    this.entries.push({ seq: ++this.seq, event: { eventId, name, timestamp, data } });
    if (this.entries.length > this.capacity) this.entries.shift();
  }

  /** The result of one request, or an `RpcError`. */
  call(method: string, params: PollParams = {}): unknown {
    if (method === "events/list") return { events: this.names.map((name) => ({ name, delivery: ["poll"] })) };
    if (method !== "events/poll") throw new RpcError(METHOD_NOT_FOUND, `${method} is not served here`);
    const { name, cursor } = params;
    if (typeof name !== "string" || !this.names.includes(name)) throw new RpcError(INVALID_PARAMS, `unknown event ${String(name)}`);
    const head = `${this.epoch}:${this.seq}`;
    const fresh = { events: [], cursor: head, hasMore: false, nextPollMs: POLL_MS };
    // No cursor: the client starts from now.
    if (cursor == null) return { ...fresh, truncated: false };

    const [epoch, raw] = String(cursor).split(":");
    const from = Number(raw);
    const oldest = this.entries[0]?.seq ?? this.seq + 1;
    if (epoch !== this.epoch || !Number.isInteger(from) || from > this.seq || from < oldest - 1) return { ...fresh, truncated: true };

    const floor = typeof params.maxAgeMs === "number" ? Date.now() - params.maxAgeMs : -Infinity;
    const since = this.entries.filter((e) => e.seq > from && e.event.name === name);
    const pending = since.filter((e) => Date.parse(e.event.timestamp) >= floor);
    const batch = pending.slice(0, typeof params.maxEvents === "number" && params.maxEvents > 0 ? params.maxEvents : 100);
    const hasMore = pending.length > batch.length;
    return {
      events: batch.map((e) => e.event),
      cursor: hasMore ? `${this.epoch}:${batch.at(-1)!.seq}` : head,
      truncated: pending.length < since.length,
      hasMore,
      nextPollMs: POLL_MS,
    };
  }

  /** Answers JSON-RPC POSTs on any path. There is no authentication, so the default host is loopback. */
  async listen(port: number, host = "127.0.0.1"): Promise<{ port: number; close: () => Promise<void> }> {
    const http = createServer((req, res) => {
      const reply = (status: number, body: object) => res.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify(body));
      // A browser page sends an Origin. No MCP client does.
      if (req.method !== "POST" || req.headers.origin !== undefined) return reply(403, { error: "POST from a local client only" });
      let text = "";
      req.on("data", (chunk) => (text += chunk));
      req.on("end", () => {
        let id: unknown = null;
        try {
          const message = JSON.parse(text) as { id?: unknown; method?: unknown; params?: PollParams };
          id = message.id ?? null;
          reply(200, { jsonrpc: "2.0", id, result: this.call(String(message.method), message.params) });
        } catch (err) {
          const code = err instanceof RpcError ? err.code : -32700;
          reply(200, { jsonrpc: "2.0", id, error: { code, message: (err as Error).message } });
        }
      });
    });
    await new Promise<void>((resolve, reject) => {
      http.once("error", reject);
      http.listen(port, host, resolve);
    });
    return {
      port: (http.address() as AddressInfo).port,
      close: () => new Promise<void>((resolve) => { http.closeAllConnections(); http.close(() => resolve()); }),
    };
  }
}
