import { DurableObject } from "cloudflare:workers";
import { handle, MAX_PER_HOOK, MAX_STORED, RETENTION_MS, Waiters, type Delivery, type Hook, type Store } from "./core.ts";

type DeliveryRow = { seq: number; hook: string; received_at: number; headers: string; body: ArrayBuffer };
type HookRow = { token: string; label: string; created_at: number; last_read_at: number | null; idle_seconds: number | null };

/** One mailbox: its hook paths and the deliveries waiting to be fetched. */
export class Mailbox extends DurableObject<Env> implements Store {
  private readonly waiters = new Waiters();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS hooks (token TEXT PRIMARY KEY, label TEXT NOT NULL, created_at INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS deliveries (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        hook TEXT NOT NULL,
        received_at INTEGER NOT NULL,
        headers TEXT NOT NULL,
        body BLOB NOT NULL
      );
      CREATE INDEX IF NOT EXISTS deliveries_hook ON deliveries (hook, seq);
    `);
    // Hooks from before expiry have neither column. A null idle_seconds is never expired.
    for (const column of ["last_read_at INTEGER", "idle_seconds INTEGER"]) {
      try {
        ctx.storage.sql.exec(`ALTER TABLE hooks ADD COLUMN ${column}`);
      } catch {}
    }
  }

  async createHook(token: string, label: string, now: number, idleSeconds?: number): Promise<void> {
    this.expire(now);
    this.ctx.storage.sql.exec(
      "INSERT INTO hooks (token, label, created_at, last_read_at, idle_seconds) VALUES (?, ?, ?, ?, ?)",
      token,
      label,
      now,
      now,
      idleSeconds ?? null,
    );
  }

  async deleteHook(token: string): Promise<boolean> {
    this.ctx.storage.sql.exec("DELETE FROM deliveries WHERE hook = ?", token);
    return this.ctx.storage.sql.exec("DELETE FROM hooks WHERE token = ?", token).rowsWritten > 0;
  }

  async hasHook(token: string): Promise<boolean> {
    return this.ctx.storage.sql.exec("SELECT 1 FROM hooks WHERE token = ?", token).toArray().length > 0;
  }

  async listHooks(): Promise<Hook[]> {
    return this.ctx.storage.sql
      .exec<HookRow>("SELECT token, label, created_at, last_read_at, idle_seconds FROM hooks ORDER BY created_at")
      .toArray()
      .map((row) => ({
        token: row.token,
        label: row.label,
        createdAt: row.created_at,
        lastReadAt: row.last_read_at ?? row.created_at,
        ...(row.idle_seconds !== null ? { idleSeconds: row.idle_seconds } : {}),
      }));
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
    const idle = "SELECT token FROM hooks WHERE idle_seconds IS NOT NULL AND COALESCE(last_read_at, created_at) < ? - idle_seconds * 1000";
    sql.exec(`DELETE FROM deliveries WHERE hook IN (${idle})`, now);
    sql.exec(`DELETE FROM hooks WHERE token IN (${idle})`, now);
  }
}

export default {
  async fetch(request, env): Promise<Response> {
    const store = env.MAILBOX.getByName("default") as unknown as Store;
    return handle(request, store, { key: env.MAILBOX_KEY });
  },
} satisfies ExportedHandler<Env>;
