import { MAX_PER_HOOK, MAX_STORED, RETENTION_MS, Waiters, type Delivery, type Hook, type Store, type StoredHook } from "./core.ts";

/** A store that keeps everything in memory. Used by the tests. */
export class MemoryStore implements Store {
  private readonly hooks = new Map<string, Hook>();
  private readonly readKeyHashes = new Map<string, string>();
  private deliveries: Delivery[] = [];
  private seq = 0;
  private readonly waiters = new Waiters();
  private readonly maxStored: number;
  private readonly maxPerHook: number;

  constructor(opts: { maxStored?: number; maxPerHook?: number } = {}) {
    this.maxStored = opts.maxStored ?? MAX_STORED;
    this.maxPerHook = opts.maxPerHook ?? MAX_PER_HOOK;
  }

  async createHook(token: string, label: string, now: number, idleSeconds: number | undefined, readKeyHash: string, maxHooks?: number): Promise<boolean> {
    this.expire(now);
    if (maxHooks !== undefined && this.hooks.size >= maxHooks) return false;
    this.hooks.set(token, { token, label, createdAt: now, lastReadAt: now, ...(idleSeconds !== undefined ? { idleSeconds } : {}) });
    this.readKeyHashes.set(token, readKeyHash);
    return true;
  }

  async deleteHook(token: string): Promise<boolean> {
    this.deliveries = this.deliveries.filter((d) => d.hook !== token);
    this.readKeyHashes.delete(token);
    return this.hooks.delete(token);
  }

  async getHook(token: string): Promise<StoredHook | undefined> {
    const hook = this.hooks.get(token);
    const readKeyHash = this.readKeyHashes.get(token);
    return hook && { ...hook, ...(readKeyHash !== undefined ? { readKeyHash } : {}) };
  }

  async listHooks(): Promise<Hook[]> {
    return [...this.hooks.values()];
  }

  async insert(token: string, receivedAt: number, headers: Record<string, string>, body: Uint8Array): Promise<number | "unknown" | "full" | "hook full"> {
    this.expire(receivedAt);
    if (!this.hooks.has(token)) return "unknown";
    this.deliveries = this.deliveries.filter((d) => d.receivedAt >= receivedAt - RETENTION_MS);
    if (this.deliveries.length >= this.maxStored) return "full";
    if (this.deliveries.filter((d) => d.hook === token).length >= this.maxPerHook) return "hook full";
    const seq = ++this.seq;
    this.deliveries.push({ seq, hook: token, receivedAt, headers, body });
    this.waiters.wake(token);
    return seq;
  }

  async list(after: number, limit: number, waitMs: number, hook: string | undefined, now: number): Promise<Delivery[]> {
    const read = () => this.deliveries.filter((d) => d.seq > after && (hook === undefined || d.hook === hook)).slice(0, limit);
    const known = hook === undefined ? undefined : this.hooks.get(hook);
    if (known) known.lastReadAt = now;
    this.expire(now);
    let found = read();
    if (found.length === 0 && waitMs > 0) {
      await this.waiters.wait(waitMs, hook);
      found = read();
    }
    return found;
  }

  async ack(upTo: number, hook?: string): Promise<number> {
    const before = this.deliveries.length;
    this.deliveries = this.deliveries.filter((d) => d.seq > upTo || (hook !== undefined && d.hook !== hook));
    return before - this.deliveries.length;
  }

  /** Removes hooks not fetched within their `idleSeconds`, with their deliveries. */
  private expire(now: number): void {
    for (const hook of [...this.hooks.values()]) {
      if (hook.idleSeconds !== undefined && hook.lastReadAt < now - hook.idleSeconds * 1000) void this.deleteHook(hook.token);
    }
  }
}
