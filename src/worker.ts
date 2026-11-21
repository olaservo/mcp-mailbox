import { DurableObject } from "cloudflare:workers";
import { handle, MAX_STORED, RETENTION_MS, Waiters, type Delivery, type Hook, type Store } from "./core.ts";

type DeliveryRow = { seq: number; hook: string; received_at: number; headers: string; body: ArrayBuffer };

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
    `);
  }

  async createHook(token: string, label: string, now: number): Promise<void> {
    this.ctx.storage.sql.exec("INSERT INTO hooks (token, label, created_at) VALUES (?, ?, ?)", token, label, now);
  }

  async deleteHook(token: string): Promise<boolean> {
    return this.ctx.storage.sql.exec("DELETE FROM hooks WHERE token = ?", token).rowsWritten > 0;
  }

  async hasHook(token: string): Promise<boolean> {
    return this.ctx.storage.sql.exec("SELECT 1 FROM hooks WHERE token = ?", token).toArray().length > 0;
  }

  async listHooks(): Promise<Hook[]> {
    return this.ctx.storage.sql
      .exec<{ token: string; label: string; created_at: number }>("SELECT token, label, created_at FROM hooks ORDER BY created_at")
      .toArray()
      .map((row) => ({ token: row.token, label: row.label, createdAt: row.created_at }));
  }

  async insert(token: string, receivedAt: number, headers: Record<string, string>, body: Uint8Array): Promise<number | "unknown" | "full"> {
    const sql = this.ctx.storage.sql;
    if (sql.exec("SELECT 1 FROM hooks WHERE token = ?", token).toArray().length === 0) return "unknown";
    sql.exec("DELETE FROM deliveries WHERE received_at < ?", receivedAt - RETENTION_MS);
    if (sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM deliveries").one().n >= MAX_STORED) return "full";
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
    this.waiters.wake();
    return seq;
  }

  async list(after: number, limit: number, waitMs: number): Promise<Delivery[]> {
    const read = () =>
      this.ctx.storage.sql
        .exec<DeliveryRow>("SELECT seq, hook, received_at, headers, body FROM deliveries WHERE seq > ? ORDER BY seq LIMIT ?", after, limit)
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
      await this.waiters.wait(waitMs);
      found = read();
    }
    return found;
  }

  async ack(upTo: number): Promise<number> {
    return this.ctx.storage.sql.exec("DELETE FROM deliveries WHERE seq <= ?", upTo).rowsWritten;
  }
}

export default {
  async fetch(request, env): Promise<Response> {
    const store = env.MAILBOX.getByName("default") as unknown as Store;
    return handle(request, store, { key: env.MAILBOX_KEY });
  },
} satisfies ExportedHandler<Env>;
