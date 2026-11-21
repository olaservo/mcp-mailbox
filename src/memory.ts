import { MAX_STORED, RETENTION_MS, Waiters, type Delivery, type Hook, type Store } from "./core.ts";

/** A store that keeps everything in memory. Used by the tests. */
export class MemoryStore implements Store {
  private readonly hooks = new Map<string, Hook>();
  private deliveries: Delivery[] = [];
  private seq = 0;
  private readonly waiters = new Waiters();
  private readonly maxStored: number;

  constructor(opts: { maxStored?: number } = {}) {
    this.maxStored = opts.maxStored ?? MAX_STORED;
  }

  async createHook(token: string, label: string, now: number): Promise<void> {
    this.hooks.set(token, { token, label, createdAt: now });
  }

  async deleteHook(token: string): Promise<boolean> {
    return this.hooks.delete(token);
  }

  async hasHook(token: string): Promise<boolean> {
    return this.hooks.has(token);
  }

  async listHooks(): Promise<Hook[]> {
    return [...this.hooks.values()];
  }

  async insert(token: string, receivedAt: number, headers: Record<string, string>, body: Uint8Array): Promise<number | "unknown" | "full"> {
    if (!this.hooks.has(token)) return "unknown";
    this.deliveries = this.deliveries.filter((d) => d.receivedAt >= receivedAt - RETENTION_MS);
    if (this.deliveries.length >= this.maxStored) return "full";
    const seq = ++this.seq;
    this.deliveries.push({ seq, hook: token, receivedAt, headers, body });
    this.waiters.wake();
    return seq;
  }

  async list(after: number, limit: number, waitMs: number): Promise<Delivery[]> {
    const read = () => this.deliveries.filter((d) => d.seq > after).slice(0, limit);
    let found = read();
    if (found.length === 0 && waitMs > 0) {
      await this.waiters.wait(waitMs);
      found = read();
    }
    return found;
  }

  async ack(upTo: number): Promise<number> {
    const before = this.deliveries.length;
    this.deliveries = this.deliveries.filter((d) => d.seq > upTo);
    return before - this.deliveries.length;
  }
}
