import { randomBytes } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { MAX_HELD_MS, refusal, type Fetched } from "./verify.ts";

/** An event occurrence as SEP-3415 delivers it by webhook. */
export interface EventBody {
  eventId: string;
  name: string;
  timestamp: string;
  data: unknown;
  cursor?: string | null;
}

export interface Received {
  subscriptionId: string;
  webhookId: string;
  /** Epoch ms when the mailbox received it. */
  receivedAt: number;
  event: EventBody;
}

export interface ReaderOptions {
  mailbox: { url: string; key: string };
  /** The MCP server's Streamable HTTP endpoint, and its bearer token. */
  server: { url: string; token?: string };
  /** Event names to subscribe to. */
  events: string[];
  /** JSON file with the signing secret, the hook and the position in the mailbox. */
  stateFile: string;
  /** Called with each event, one at a time, in order. */
  handle: (received: Received) => Promise<void> | void;
  label?: string;
  /** Subscription lifetime to ask for. */
  ttlMs?: number;
  /** Time between refreshes. A refresh also restores a subscription the server lost in a restart. */
  refreshMs?: number;
  /** How long one fetch waits at the mailbox, up to 25. */
  waitSeconds?: number;
  /** After a fetch returns deliveries, fetch again after this long, so events sent together are ordered together. */
  settleMs?: number;
  now?: () => number;
  log?: (line: string) => void;
}

interface State {
  secret: string;
  hook?: { token: string; url: string };
  /** The highest mailbox sequence number handled. */
  after: number;
  /** Event name to the subscription the server granted. */
  subscriptions: Record<string, { id: string; refreshBefore: string | null }>;
  /** `<subscription id> <webhook-id>` of handled deliveries, with the time of receipt. */
  seen: Record<string, number>;
}

const PROTOCOL_VERSION = "2026-07-28";
const sleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve) => {
    if (signal?.aborted) return resolve();
    const done = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal?.addEventListener("abort", done);
  });

/** Event ids that end in a number after the same prefix are ordered by it. Others by timestamp, then by arrival. */
function inOrder(a: { received: Received; seq: number }, b: { received: Received; seq: number }): number {
  const x = /^(.*?)(\d+)$/.exec(a.received.event.eventId);
  const y = /^(.*?)(\d+)$/.exec(b.received.event.eventId);
  if (x && y && x[1] === y[1] && x[2] !== y[2]) return Number(x[2]) - Number(y[2]);
  const byTime = Date.parse(a.received.event.timestamp) - Date.parse(b.received.event.timestamp);
  return byTime || a.seq - b.seq;
}

/**
 * Subscribes to an MCP server with a mailbox hook as the webhook callback, then fetches from the mailbox,
 * verifies, deduplicates, orders and hands on each event. The mailbox never has the signing secret.
 */
export class Reader {
  private readonly opts: ReaderOptions;
  private readonly now: () => number;
  private readonly log: (line: string) => void;
  private state: State;
  private rpcId = 0;
  /** Set while `run` is active, so stopping ends a waiting fetch. */
  private stopping: AbortSignal | undefined;

  constructor(opts: ReaderOptions) {
    this.opts = opts;
    this.now = opts.now ?? Date.now;
    this.log = opts.log ?? (() => {});
    this.state = this.load();
  }

  /** The callback URL, once `subscribe` has run. */
  get hookUrl(): string | undefined {
    return this.state.hook?.url;
  }

  /** Subscribes or refreshes every event. Creates the hook first when the mailbox does not have it. */
  async subscribe(): Promise<void> {
    const hook = await this.ensureHook();
    for (const name of this.opts.events) {
      const result = (await this.rpc("events/subscribe", {
        name,
        arguments: {},
        delivery: { mode: "webhook", url: hook.url, secret: this.state.secret },
        ttlMs: this.opts.ttlMs ?? 60 * 60 * 1000,
        cursor: null,
      })) as { id: string; refreshBefore?: string | null; truncated?: boolean; deliveryStatus?: { lastError?: string | null; failedSince?: string } };
      const known = this.state.subscriptions[name]?.id === result.id;
      this.state.subscriptions[name] = { id: result.id, refreshBefore: result.refreshBefore ?? null };
      if (!known) this.log(`subscribed ${name} as ${result.id}`);
      if (result.truncated) this.log(`${name}: the server reports a gap`);
      if (result.deliveryStatus?.lastError) this.log(`${name}: the server's last delivery failed: ${result.deliveryStatus.lastError}, failing since ${result.deliveryStatus.failedSince ?? "unknown"}`);
    }
    this.save();
  }

  /** Unsubscribes every event and removes the hook. */
  async unsubscribe(): Promise<void> {
    const hook = this.state.hook;
    if (!hook) return;
    for (const name of this.opts.events) {
      await this.rpc("events/unsubscribe", { name, arguments: {}, delivery: { mode: "webhook", url: hook.url } });
    }
    await this.mailbox("DELETE", `/hooks/${hook.token}`);
    this.state.hook = undefined;
    this.state.subscriptions = {};
    this.save();
  }

  /** One fetch from the mailbox. Returns how many events were handed on. */
  async poll(waitSeconds = 0): Promise<number> {
    const hook = this.state.hook;
    if (!hook) throw new Error("subscribe first");
    const fetchAfter = async (after: number, wait: number) => {
      const res = await this.mailbox("GET", `/deliveries?after=${after}&limit=500&wait=${wait}&hook=${hook.token}`);
      return ((await res.json()) as { deliveries: Fetched[] }).deliveries;
    };
    const batch = await fetchAfter(this.state.after, waitSeconds);
    if (batch.length === 0) return 0;
    for (;;) {
      await sleep(this.opts.settleMs ?? 500);
      const more = await fetchAfter(batch.at(-1)!.seq, 0);
      if (more.length === 0) break;
      batch.push(...more);
    }

    const now = this.now();
    const byId = new Map(Object.entries(this.state.subscriptions).map(([name, sub]) => [sub.id, name]));
    const accepted: { received: Received; seq: number; key: string }[] = [];
    const keys = new Set<string>();
    for (const delivery of batch) {
      // A mailbox from before per-hook fetches returns every hook's deliveries.
      if (delivery.hook !== hook.token) continue;
      const subscriptionId = delivery.headers["x-mcp-subscription-id"] ?? "";
      const webhookId = delivery.headers["webhook-id"] ?? "";
      const key = `${subscriptionId} ${webhookId}`;
      const refuse = (why: string) => this.log(`refused delivery ${delivery.seq} (${key}): ${why}`);
      const why = refusal(this.state.secret, delivery, now);
      if (why) {
        refuse(why);
        continue;
      }
      const name = byId.get(subscriptionId);
      if (name === undefined) {
        refuse("unknown subscription");
        continue;
      }
      let event: EventBody;
      try {
        event = JSON.parse(Buffer.from(delivery.body, "base64").toString("utf8")) as EventBody;
      } catch {
        refuse("body is not JSON");
        continue;
      }
      if ((event as { type?: unknown }).type !== undefined && event.eventId === undefined) {
        this.log(`control envelope ${String((event as { type?: unknown }).type)} on ${subscriptionId}`);
        continue;
      }
      // The subscription id header is not signed. The event name is, so a delivery moved to another subscription is caught.
      if (event.name !== name) {
        refuse(`event ${event.name} on a subscription for ${name}`);
        continue;
      }
      if (this.state.seen[key] !== undefined || keys.has(key)) {
        refuse("repeat");
        continue;
      }
      keys.add(key);
      accepted.push({ received: { subscriptionId, webhookId, receivedAt: delivery.receivedAt, event }, seq: delivery.seq, key });
    }

    accepted.sort(inOrder);
    for (const { received, key } of accepted) {
      try {
        await this.opts.handle(received);
      } catch (err) {
        this.log(`handler failed on ${received.event.eventId}: ${(err as Error).message}`);
      }
      this.state.seen[key] = received.receivedAt;
    }

    this.state.after = batch.at(-1)!.seq;
    for (const [key, at] of Object.entries(this.state.seen)) {
      if (at < now - MAX_HELD_MS) delete this.state.seen[key];
    }
    // Saved before the ack, so a stop between the two repeats nothing.
    this.save();
    await this.mailbox("POST", "/ack", { upTo: this.state.after, hook: hook.token });
    return accepted.length;
  }

  /** Subscribes, then fetches and refreshes until `signal` aborts. A failed request is logged and tried again. */
  async run(signal: AbortSignal): Promise<void> {
    this.stopping = signal;
    const refreshMs = this.opts.refreshMs ?? 5 * 60 * 1000;
    const refreshing = (async () => {
      while (!signal.aborted) {
        let wait = refreshMs;
        try {
          await this.subscribe();
        } catch (err) {
          if (signal.aborted) break;
          this.log(`subscribe failed: ${(err as Error).message}`);
          wait = Math.min(refreshMs, 15_000);
        }
        await sleep(wait, signal);
      }
    })();
    let failures = 0;
    while (!signal.aborted) {
      if (!this.state.hook) {
        await sleep(500, signal);
        continue;
      }
      try {
        await this.poll(this.opts.waitSeconds ?? 25);
        failures = 0;
      } catch (err) {
        if (signal.aborted) break;
        this.log(`fetch failed: ${(err as Error).message}`);
        await sleep(Math.min(30_000, 1000 * 2 ** failures++), signal);
      }
    }
    await refreshing;
  }

  private async ensureHook(): Promise<{ token: string; url: string }> {
    const held = this.state.hook;
    if (held) {
      const { hooks } = (await (await this.mailbox("GET", "/hooks")).json()) as { hooks: { token: string }[] };
      if (hooks.some((h) => h.token === held.token)) return held;
      this.log("the mailbox no longer has the hook; creating another");
    }
    const created = (await (await this.mailbox("POST", "/hooks", { label: this.opts.label ?? "reader" })).json()) as { token: string; url: string };
    this.state.hook = { token: created.token, url: created.url };
    this.state.subscriptions = {};
    this.save();
    return this.state.hook;
  }

  private async mailbox(method: string, path: string, body?: unknown): Promise<Response> {
    const res = await fetch(`${this.opts.mailbox.url.replace(/\/$/, "")}${path}`, {
      method,
      headers: { authorization: `Bearer ${this.opts.mailbox.key}` },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: this.stopping,
    });
    if (!res.ok) throw new Error(`mailbox ${method} ${path.split("?")[0]}: ${res.status}`);
    return res;
  }

  /** One JSON-RPC request over Streamable HTTP, in the 2026-07-28 form: no session, the envelope in `_meta`. */
  private async rpc(method: string, params: { name: string } & Record<string, unknown>): Promise<unknown> {
    const { url, token } = this.opts.server;
    const name = /^[\x20-\x7e]*$/.test(params.name) ? params.name : `=?base64?${Buffer.from(params.name).toString("base64")}?=`;
    const meta = {
      "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
      "io.modelcontextprotocol/clientInfo": { name: "mcp-mailbox-reader", version: "0.0.0" },
      "io.modelcontextprotocol/clientCapabilities": {},
    };
    const res = await fetch(url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        accept: "application/json, text/event-stream",
        "mcp-protocol-version": PROTOCOL_VERSION,
        "mcp-method": method,
        "mcp-name": name,
        ...(token ? { authorization: `Bearer ${token}` } : {}),
      },
      body: JSON.stringify({ jsonrpc: "2.0", id: ++this.rpcId, method, params: { ...params, _meta: meta } }),
      signal: this.stopping,
    });
    const text = await res.text();
    const json = text.trimStart().startsWith("{") ? text : (text.split("\n").find((line) => line.startsWith("data: "))?.slice(6) ?? "");
    let message: { result?: unknown; error?: { code: number; message: string; data?: unknown } };
    try {
      message = JSON.parse(json) as typeof message;
    } catch {
      throw new Error(`${method}: HTTP ${res.status}`);
    }
    if (message.error) throw new Error(`${method}: ${message.error.code} ${message.error.message}${message.error.data ? ` ${JSON.stringify(message.error.data)}` : ""}`);
    return message.result;
  }

  private load(): State {
    const fresh: State = { secret: `whsec_${randomBytes(32).toString("base64")}`, after: 0, subscriptions: {}, seen: {} };
    if (!existsSync(this.opts.stateFile)) return fresh;
    return { ...fresh, ...(JSON.parse(readFileSync(this.opts.stateFile, "utf8")) as Partial<State>) };
  }

  private save(): void {
    mkdirSync(dirname(this.opts.stateFile), { recursive: true });
    const temp = `${this.opts.stateFile}.tmp`;
    writeFileSync(temp, JSON.stringify(this.state, null, 2), { mode: 0o600 });
    renameSync(temp, this.opts.stateFile);
  }
}
