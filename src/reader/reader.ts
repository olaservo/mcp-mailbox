import { createPrivateKey, generateKeyPairSync, randomBytes, type JsonWebKey } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, renameSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { openSeq, setupR } from "./hpke.ts";
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

export interface Server {
  url: string;
  token?: string;
}

export interface ReaderOptions {
  mailbox: { url: string; key: string };
  /** An MCP server's Streamable HTTP endpoint, and its bearer token. */
  server?: Server;
  /**
   * A directory where servers announce themselves, one `*.json` file each with `url` and optionally `token`.
   * It is read again every `scanMs`. `token` here is for a file without one.
   */
  servers?: { dir: string; token?: string };
  scanMs?: number;
  /** Event names to subscribe to. */
  events: string[];
  /**
   * Asks each server to encrypt event data to the reader's key, so the mailbox cannot read it. A server that
   * does not list the scheme is not subscribed, and a delivery that is not encrypted is skipped.
   */
  encrypt?: boolean;
  /** JSON file with the signing secret, the key pair, the hook and the position in the mailbox. */
  stateFile: string;
  /** Called with each event, one at a time, in order. */
  handle: (received: Received) => Promise<void> | void;
  label?: string;
  /** Passed as `idleSeconds` when the hook is created, so the mailbox removes it after that long unread. Unset: kept. */
  hookIdleSeconds?: number;
  /** How long one fetch waits at the mailbox, up to 25. */
  waitSeconds?: number;
  /** After a fetch returns deliveries, fetch again after this long, so events sent together are ordered together. */
  settleMs?: number;
  now?: () => number;
  log?: (line: string) => void;
}

interface State {
  secret: string;
  /** The X25519 key pair event data is encrypted to, as a JWK: `x` is the public key and `d` the private key. */
  key?: JsonWebKey;
  hook?: { token: string; url: string };
  /** The highest mailbox sequence number handled. */
  after: number;
  /** Per server URL and event name: the subscription id it granted and the cursor it last returned. */
  servers: Record<string, { subscriptions: Record<string, string>; cursors: Record<string, string> }>;
  /** `<subscription id> <webhook-id>` of handled deliveries, with the time of receipt. */
  seen: Record<string, number>;
}

const PROTOCOL_VERSION = "2026-07-28";
/** DHKEM(X25519, HKDF-SHA256), HKDF-SHA256, AES-128-GCM, for the experiment in forge-mcp-server issue 9. SEP-3415 does not define it. */
const SCHEME = "hpke-x25519-sha256-aes128gcm";
const AES_128_GCM = 1;
/** Subscription lifetime asked for. */
const TTL_MS = 60 * 60 * 1000;
/** Longest time between refreshes. A refresh also restores a subscription the server lost in a restart. */
const REFRESH_MS = 60 * 1000;
/** A server that could not be subscribed is tried again after this long. */
const RETRY_MS = 15 * 1000;
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
  const x = /^(.*\D)(\d+)$/.exec(a.received.event.eventId);
  const y = /^(.*\D)(\d+)$/.exec(b.received.event.eventId);
  if (x && y && x[1] === y[1]) return Number(x[2]) - Number(y[2]) || a.seq - b.seq;
  return Date.parse(a.received.event.timestamp) - Date.parse(b.received.event.timestamp) || a.seq - b.seq;
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
    if (!opts.server && !opts.servers) throw new Error("no server to subscribe to");
    this.state = this.load();
    if (opts.encrypt && !this.state.key) {
      this.state.key = generateKeyPairSync("x25519").privateKey.export({ format: "jwk" });
      // Saved before a server is sent the public key.
      this.save();
    }
  }

  /** The callback URL, once `subscribe` has run. */
  get hookUrl(): string | undefined {
    return this.state.hook?.url;
  }

  /** Subscribes or refreshes every event on every server. Creates the hook first when the mailbox does not have it. Returns when the first subscription lapses. */
  async subscribe(): Promise<number> {
    const hook = await this.ensureHook();
    let lapses = Infinity;
    let failure: unknown;
    for (const server of this.servers()) {
      try {
        lapses = Math.min(lapses, await this.subscribeTo(server, hook.url));
      } catch (err) {
        failure ??= err;
      }
    }
    if (failure) throw failure;
    return lapses;
  }

  /** Unsubscribes every event on every server and removes the hook. */
  async unsubscribe(): Promise<void> {
    const hook = this.state.hook;
    if (!hook) return;
    for (const server of this.servers()) {
      for (const name of this.opts.events) {
        await this.rpc(server, "events/unsubscribe", { name, arguments: {}, delivery: { mode: "webhook", url: hook.url } });
      }
    }
    await this.mailbox("DELETE", `/hooks/${hook.token}`);
    this.state.hook = undefined;
    this.state.servers = {};
    this.save();
  }

  /** What each server says is still waiting for `name`: its answer to a poll with no cursor. A server that fails or is slow is left out. */
  async pending(name: string): Promise<EventBody[]> {
    const answers = await Promise.all(
      this.servers().map(async (server) => {
        try {
          const result = (await this.rpc(server, "events/poll", { name, arguments: {}, cursor: null }, AbortSignal.timeout(2000))) as { events?: EventBody[] };
          return (result.events ?? []).filter((event) => event.name === name);
        } catch {
          return [];
        }
      }),
    );
    return answers.flat();
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
    const accepted = new Map<string, { received: Received; seq: number }>();
    for (const delivery of batch) {
      const key = `${delivery.headers["x-mcp-subscription-id"]} ${delivery.headers["webhook-id"]}`;
      const read = this.read(delivery, now);
      const why = typeof read === "string" ? read : this.state.seen[key] !== undefined || accepted.has(key) ? "repeat" : undefined;
      if (why !== undefined) this.log(`skipped delivery ${delivery.seq} (${key}): ${why}`);
      else accepted.set(key, { received: read as Received, seq: delivery.seq });
    }

    for (const [key, { received }] of [...accepted].sort((a, b) => inOrder(a[1], b[1]))) {
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
    return accepted.size;
  }

  /** Subscribes, then fetches and refreshes until `signal` aborts. A failed request is logged and tried again. */
  async run(signal: AbortSignal): Promise<void> {
    this.stopping = signal;
    /** Per server URL, when to subscribe next. */
    const due = new Map<string, number>();
    /** The last failure logged for a server or for the mailbox, so a repeat is logged once. */
    const failed = new Map<string, string>();
    const fail = (key: string, err: unknown) => {
      const message = (err as Error).message;
      if (failed.get(key) !== message) this.log(`subscribe failed at ${key}: ${message}`);
      failed.set(key, message);
    };
    const refreshing = (async () => {
      while (!signal.aborted) {
        const servers = this.servers();
        for (const url of due.keys()) if (!servers.some((server) => server.url === url)) due.delete(url);
        let ready = servers.filter((server) => (due.get(server.url) ?? 0) <= Date.now());
        if (ready.length > 0) {
          let hookUrl: string | undefined;
          try {
            const held = this.state.hook?.token;
            const hook = await this.ensureHook();
            // A new hook is a new callback URL for every server.
            if (held !== hook.token) ready = servers;
            hookUrl = hook.url;
            failed.delete("the mailbox");
          } catch (err) {
            if (signal.aborted) break;
            fail("the mailbox", err);
          }
          for (const server of ready) {
            let wait = RETRY_MS;
            try {
              if (hookUrl === undefined) continue;
              // Half the granted lifetime, so one failed refresh leaves time for another.
              wait = Math.max(1000, Math.min(REFRESH_MS, ((await this.subscribeTo(server, hookUrl)) - Date.now()) / 2));
              failed.delete(server.url);
            } catch (err) {
              if (signal.aborted) break;
              fail(server.url, err);
            } finally {
              due.set(server.url, Date.now() + wait);
            }
          }
        }
        const next = Math.min(...due.values()) - Date.now();
        await sleep(this.opts.servers ? (this.opts.scanMs ?? 5000) : Math.max(1000, next), signal);
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

  /** The event in a delivery, or the reason it is not handed on. */
  private read(delivery: Fetched, now: number): Received | string {
    const why = refusal(this.state.secret, delivery, now);
    if (why) return why;
    const subscriptionId = delivery.headers["x-mcp-subscription-id"]!;
    const name = Object.values(this.state.servers).flatMap((server) => Object.entries(server.subscriptions)).find(([, id]) => id === subscriptionId)?.[0];
    if (name === undefined) return "unknown subscription";
    let body: (Partial<EventBody> & { type?: unknown; encrypted?: { enc: string; ct: string } }) | null;
    try {
      body = JSON.parse(Buffer.from(delivery.body, "base64").toString("utf8")) as typeof body;
    } catch {
      body = null;
    }
    if (body === null || typeof body !== "object") return "body is not a JSON object";
    // `gap` and `terminated`. The next refresh subscribes again after a `terminated`.
    if (typeof body.type === "string") return `control envelope ${body.type}`;
    const { encrypted, ...clear } = body;
    if (!this.opts.encrypt && encrypted !== undefined) return "encrypted, and encryption is off";
    if (this.opts.encrypt) {
      if (encrypted === undefined) return "not encrypted";
      try {
        body = { ...clear, data: this.open(subscriptionId, clear, encrypted) };
      } catch {
        return "does not decrypt";
      }
    }
    // The subscription id header is not signed and the event name is.
    if (body.name !== name) return `event ${body.name} on a subscription for ${name}`;
    return { subscriptionId, webhookId: delivery.headers["webhook-id"]!, receivedAt: delivery.receivedAt, event: body as EventBody };
  }

  /** The `data` of an encrypted event. Throws unless it was sealed to this reader's key, for this subscription and these clear fields. */
  private open(subscriptionId: string, event: Partial<EventBody>, sealed: { enc: string; ct: string }): unknown {
    const key = createPrivateKey({ key: this.state.key!, format: "jwk" });
    // The subscription id is in `info`, so a delivery moved to another subscription does not open.
    const context = setupR(AES_128_GCM, Buffer.from(sealed.enc, "base64url"), key, Buffer.from(`mcp-events v1 ${subscriptionId}`));
    const aad = Buffer.from(`${event.eventId}\n${event.name}\n${event.timestamp}`);
    return JSON.parse(openSeq(AES_128_GCM, context, 0, aad, Buffer.from(sealed.ct, "base64url")).toString("utf8"));
  }

  /** The servers to subscribe to now. One whose file is gone is forgotten. */
  private servers(): Server[] {
    const found = new Map<string, Server>();
    if (this.opts.server) found.set(this.opts.server.url, this.opts.server);
    const registry = this.opts.servers;
    let files: string[] = [];
    try {
      if (registry) files = readdirSync(registry.dir).filter((name) => name.endsWith(".json"));
    } catch {
      // No directory yet: no server has started.
    }
    for (const name of files) {
      try {
        const entry = JSON.parse(readFileSync(join(registry!.dir, name), "utf8")) as { url?: unknown; token?: unknown };
        if (typeof entry.url !== "string") continue;
        const token = typeof entry.token === "string" ? entry.token : (found.get(entry.url)?.token ?? registry!.token);
        found.set(entry.url, { url: entry.url, token });
      } catch {
        // A file removed or half written since the listing. The next scan reads it.
      }
    }
    for (const url of Object.keys(this.state.servers)) if (!found.has(url)) delete this.state.servers[url];
    return [...found.values()];
  }

  /** Subscribes or refreshes every event on one server. Returns when its first subscription lapses. */
  private async subscribeTo(server: Server, hookUrl: string): Promise<number> {
    const held = (this.state.servers[server.url] ??= { subscriptions: {}, cursors: {} });
    const encryption = this.opts.encrypt ? { scheme: SCHEME, publicKey: this.state.key!.x } : undefined;
    if (encryption) {
      // Asked before every subscribe. A server without the field would take the subscription and deliver in the clear.
      const { events = [] } = (await this.rpc(server, "events/list", {})) as { events?: { name: string; encryption?: unknown }[] };
      const lists = (name: string) => {
        const schemes = events.find((event) => event.name === name)?.encryption;
        return Array.isArray(schemes) && schemes.includes(SCHEME);
      };
      const without = this.opts.events.filter((name) => !lists(name));
      if (without.length > 0) {
        // Subscriptions from a run without encryption, or from before the server lost it.
        for (const name of Object.keys(held.subscriptions)) await this.drop(server, hookUrl, name);
        this.save();
        throw new Error(`events/list does not list ${SCHEME} for ${without.join(", ")}`);
      }
    }
    let lapses = Infinity;
    const refused: string[] = [];
    for (const name of this.opts.events) {
      const result = (await this.rpc(server, "events/subscribe", {
        name,
        arguments: {},
        delivery: { mode: "webhook", url: hookUrl, secret: this.state.secret, encryption },
        ttlMs: TTL_MS,
        cursor: held.cursors[name] ?? null,
      })) as { id: string; cursor?: string | null; refreshBefore?: string | null; truncated?: boolean; delivery?: { encryption?: { scheme?: string } }; deliveryStatus?: { lastError?: string | null; failedSince?: string } };
      if (encryption && result.delivery?.encryption?.scheme !== SCHEME) {
        await this.drop(server, hookUrl, name);
        refused.push(name);
        continue;
      }
      if (held.subscriptions[name] !== result.id) this.log(`subscribed ${name} as ${result.id} at ${server.url}`);
      held.subscriptions[name] = result.id;
      if (typeof result.cursor === "string") held.cursors[name] = result.cursor;
      if (result.refreshBefore) lapses = Math.min(lapses, Date.parse(result.refreshBefore));
      if (result.truncated) this.log(`${name} at ${server.url}: the server reports a gap`);
      if (result.deliveryStatus?.lastError) this.log(`${name} at ${server.url}: the server's last delivery failed: ${result.deliveryStatus.lastError}, failing since ${result.deliveryStatus.failedSince ?? "unknown"}`);
    }
    this.save();
    if (refused.length > 0) throw new Error(`the result for ${refused.join(", ")} does not say ${SCHEME} is in effect; unsubscribed`);
    return lapses;
  }

  /** Gives up a subscription the server would deliver in the clear. */
  private async drop(server: Server, hookUrl: string, name: string): Promise<void> {
    delete this.state.servers[server.url]?.subscriptions[name];
    await this.rpc(server, "events/unsubscribe", { name, arguments: {}, delivery: { mode: "webhook", url: hookUrl } }).catch(() => {});
  }

  private async ensureHook(): Promise<{ token: string; url: string }> {
    const held = this.state.hook;
    if (held) {
      const { hooks } = (await (await this.mailbox("GET", "/hooks")).json()) as { hooks: { token: string }[] };
      if (hooks.some((h) => h.token === held.token)) return held;
      this.log("the mailbox no longer has the hook; creating another");
    }
    const created = (await (await this.mailbox("POST", "/hooks", { label: this.opts.label ?? "reader", idleSeconds: this.opts.hookIdleSeconds })).json()) as { token: string; url: string };
    this.state.hook = { token: created.token, url: created.url };
    for (const server of Object.values(this.state.servers)) server.subscriptions = {};
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
    if (!res.ok) {
      // The body names the reason: the mailbox's own `error`, or the platform's error page.
      const said = (await res.text().catch(() => "")).replace(/\s+/g, " ").trim().slice(0, 200);
      throw new Error(`mailbox ${method} ${path.split("?")[0]}: ${res.status}${said ? ` ${said}` : ""}`);
    }
    return res;
  }

  /** One JSON-RPC request over Streamable HTTP, in the 2026-07-28 form: no session, the envelope in `_meta`. */
  private async rpc(server: Server, method: string, params: { name?: string } & Record<string, unknown>, signal = this.stopping): Promise<unknown> {
    const { url, token } = server;
    const name = params.name === undefined || /^[\x20-\x7e]*$/.test(params.name) ? params.name : `=?base64?${Buffer.from(params.name).toString("base64")}?=`;
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
        ...(name === undefined ? {} : { "mcp-name": name }),
        ...(token ? { authorization: `Bearer ${token}` } : {}),
      },
      body: JSON.stringify({ jsonrpc: "2.0", id: ++this.rpcId, method, params: { ...params, _meta: meta } }),
      signal,
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
    const fresh: State = { secret: `whsec_${randomBytes(32).toString("base64")}`, after: 0, servers: {}, seen: {} };
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
