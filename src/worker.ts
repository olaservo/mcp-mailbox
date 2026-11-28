import { DurableObject } from "cloudflare:workers";
import { handle, MAX_PER_HOOK, MAX_STORED, RETENTION_MS, retrying, Waiters, type Delivery, type Hook, type Store, type StoredHook } from "./core.ts";

type DeliveryRow = { seq: number; hook: string; received_at: number; headers: string; body: ArrayBuffer };
type HookRow = { token: string; label: string; created_at: number; last_read_at: number | null; idle_seconds: number | null; read_key_hash?: string | null };

/** One mailbox: its hook paths and the deliveries waiting to be fetched. */
export class Mailbox extends DurableObject<Env> implements Store {
  private readonly waiters = new Waiters();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS hooks (token TEXT PRIMARY KEY, label TEXT NOT NULL, created_at INTEGER NOT NULL, last_read_at INTEGER, idle_seconds INTEGER);
      CREATE TABLE IF NOT EXISTS deliveries (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        hook TEXT NOT NULL,
        received_at INTEGER NOT NULL,
        headers TEXT NOT NULL,
        body BLOB NOT NULL
      );
      CREATE INDEX IF NOT EXISTS deliveries_hook ON deliveries (hook, seq);
      CREATE TABLE IF NOT EXISTS read_keys (token TEXT PRIMARY KEY, hash TEXT NOT NULL);
    `);
  }

  async createHook(token: string, label: string, now: number, idleSeconds: number | undefined, readKeyHash: string, maxHooks?: number): Promise<boolean> {
    const sql = this.ctx.storage.sql;
    this.expire(now);
    if (maxHooks !== undefined && sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM hooks").one().n >= maxHooks) return false;
    sql.exec("INSERT INTO hooks (token, label, created_at, last_read_at, idle_seconds) VALUES (?, ?, ?, ?, ?)", token, label, now, now, idleSeconds ?? null);
    sql.exec("INSERT INTO read_keys (token, hash) VALUES (?, ?)", token, readKeyHash);
    return true;
  }

  async deleteHook(token: string): Promise<boolean> {
    this.ctx.storage.sql.exec("DELETE FROM deliveries WHERE hook = ?", token);
    this.ctx.storage.sql.exec("DELETE FROM read_keys WHERE token = ?", token);
    return this.ctx.storage.sql.exec("DELETE FROM hooks WHERE token = ?", token).rowsWritten > 0;
  }

  async getHook(token: string): Promise<StoredHook | undefined> {
    const row = this.ctx.storage.sql
      .exec<HookRow>("SELECT hooks.*, read_keys.hash AS read_key_hash FROM hooks LEFT JOIN read_keys USING (token) WHERE token = ?", token)
      .toArray()[0];
    return row && { ...toHook(row), ...(row.read_key_hash ? { readKeyHash: row.read_key_hash } : {}) };
  }

  async listHooks(): Promise<Hook[]> {
    return this.ctx.storage.sql.exec<HookRow>("SELECT token, label, created_at, last_read_at, idle_seconds FROM hooks ORDER BY created_at").toArray().map(toHook);
  }

  async insert(token: string, receivedAt: number, headers: Record<string, string>, body: Uint8Array): Promise<number | "unknown" | "full" | "hook full"> {
    const sql = this.ctx.storage.sql;
    this.expire(receivedAt);
    if (sql.exec("SELECT 1 FROM hooks WHERE token = ?", token).toArray().length === 0) return "unknown";
    sql.exec("DELETE FROM deliveries WHERE received_at < ?", receivedAt - RETENTION_MS);
    if (sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM deliveries").one().n >= MAX_STORED) return "full";
    if (sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM deliveries WHERE hook = ?", token).one().n >= MAX_PER_HOOK) return "hook full";
    const bytes = body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength) as ArrayBuffer;
    const { seq } = sql
      .exec<{ seq: number }>(
        "INSERT INTO deliveries (hook, received_at, headers, body) VALUES (?, ?, ?, ?) RETURNING seq",
        token,
        receivedAt,
        JSON.stringify(headers),
        bytes,
      )
      .one();
    this.waiters.wake(token);
    return seq;
  }

  async list(after: number, limit: number, waitMs: number, hook: string | undefined, now: number): Promise<Delivery[]> {
    if (hook !== undefined) this.ctx.storage.sql.exec("UPDATE hooks SET last_read_at = ? WHERE token = ?", now, hook);
    this.expire(now);
    const read = () =>
      this.ctx.storage.sql
        .exec<DeliveryRow>("SELECT seq, hook, received_at, headers, body FROM deliveries WHERE seq > ? AND (? IS NULL OR hook = ?) ORDER BY seq LIMIT ?", after, hook ?? null, hook ?? null, limit)
        .toArray()
        .map((row) => ({
          seq: row.seq,
          hook: row.hook,
          receivedAt: row.received_at,
          headers: JSON.parse(row.headers) as Record<string, string>,
          body: new Uint8Array(row.body),
        }));
    let found = read();
    if (found.length === 0 && waitMs > 0) {
      await this.waiters.wait(waitMs, hook);
      found = read();
    }
    return found;
  }

  async ack(upTo: number, hook?: string): Promise<number> {
    return this.ctx.storage.sql.exec("DELETE FROM deliveries WHERE seq <= ? AND (? IS NULL OR hook = ?)", upTo, hook ?? null, hook ?? null).rowsWritten;
  }

  /** Removes hooks not fetched within their `idle_seconds`, with their deliveries. */
  private expire(now: number): void {
    const sql = this.ctx.storage.sql;
    const idle = "SELECT token FROM hooks WHERE idle_seconds IS NOT NULL AND last_read_at < ? - idle_seconds * 1000";
    sql.exec(`DELETE FROM deliveries WHERE hook IN (${idle})`, now);
    sql.exec(`DELETE FROM read_keys WHERE token IN (${idle})`, now);
    sql.exec(`DELETE FROM hooks WHERE token IN (${idle})`, now);
  }
}

function toHook(row: HookRow): Hook {
  return {
    token: row.token,
    label: row.label,
    createdAt: row.created_at,
    lastReadAt: row.last_read_at ?? row.created_at,
    ...(row.idle_seconds !== null ? { idleSeconds: row.idle_seconds } : {}),
  };
}

export default {
  async fetch(request, env): Promise<Response> {
    // A stub that has thrown can be broken, so each call gets a new one.
    const store = retrying(() => env.MAILBOX.getByName("default") as unknown as Store);
    // The limiter counts per Cloudflare location and per address, the only thing known about an anonymous caller.
    const limiter = env.HOOK_LIMITER;
    const allowOpenHook = limiter && (async (req: Request) => (await limiter.limit({ key: req.headers.get("cf-connecting-ip") ?? "unknown" })).success);
    return handle(request, store, { key: env.MAILBOX_KEY, openHooks: env.OPEN_HOOKS === "1", allowOpenHook });
  },
} satisfies ExportedHandler<Env>;
