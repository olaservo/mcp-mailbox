/**
 * The mailbox's HTTP surface, independent of where it runs.
 *
 * The mailbox holds no signing secret. It does not verify signatures or check timestamps. It records
 * when each delivery arrived, and the reader verifies against that time and deduplicates.
 */

export const MAX_BODY_BYTES = 256 * 1024;
export const MAX_STORED = 10_000;
export const RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const MAX_VERIFICATION_BYTES = 4096;
const MAX_FETCH = 500;
const MAX_WAIT_MS = 25_000;
const MIN_KEY_LENGTH = 32;
const TOKEN = /^[A-Za-z0-9_-]{43}$/;

/** Headers kept with each delivery. The reader needs them to verify. */
const KEPT_HEADERS = ["content-type", "webhook-id", "webhook-timestamp", "webhook-signature", "x-mcp-subscription-id"];

export interface Hook {
  token: string;
  label: string;
  createdAt: number;
}

export interface Delivery {
  seq: number;
  hook: string;
  /** Epoch ms when the mailbox received the POST. */
  receivedAt: number;
  headers: Record<string, string>;
  /** The request body exactly as received. */
  body: Uint8Array;
}

export interface Store {
  createHook(token: string, label: string, now: number): Promise<void>;
  deleteHook(token: string): Promise<boolean>;
  hasHook(token: string): Promise<boolean>;
  listHooks(): Promise<Hook[]>;
  /** Stores a delivery and returns its sequence number. Also drops deliveries older than `RETENTION_MS`. */
  insert(token: string, receivedAt: number, headers: Record<string, string>, body: Uint8Array): Promise<number | "unknown" | "full">;
  /** Deliveries with `seq > after`, oldest first, for one hook when `hook` is given. With none and `waitMs > 0`, waits up to that long for one. */
  list(after: number, limit: number, waitMs: number, hook?: string): Promise<Delivery[]>;
  /** Deletes deliveries with `seq <= upTo`, for one hook when `hook` is given. Returns how many. */
  ack(upTo: number, hook?: string): Promise<number>;
}

export interface MailboxOptions {
  /** The reader's bearer key. Reader routes answer 503 without one of at least 32 characters. */
  key: string | undefined;
  now?: () => number;
}

/** Lets a `list` call wait for the next `insert`. */
export class Waiters {
  private readonly waiting = new Set<() => void>();

  wake(): void {
    for (const done of [...this.waiting]) done();
  }

  wait(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(timer);
        this.waiting.delete(done);
        resolve();
      };
      const timer = setTimeout(done, ms);
      this.waiting.add(done);
    });
  }
}

const json = (status: number, value: unknown) => Response.json(value, { status });
const error = (status: number, message: string) => json(status, { error: message });

function base64(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i += 0x8000) binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(binary);
}

function newToken(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return base64(bytes).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}

/** Compares digests, so the time taken does not depend on where the strings differ. */
async function sameString(a: string, b: string): Promise<boolean> {
  const encoder = new TextEncoder();
  const [x, y] = await Promise.all([a, b].map(async (s) => new Uint8Array(await crypto.subtle.digest("SHA-256", encoder.encode(s)))));
  let diff = 0;
  for (let i = 0; i < x!.length; i++) diff |= x![i]! ^ y![i]!;
  return diff === 0;
}

/** The challenge, if this body is a SEP-3415 verification envelope. */
function verificationChallenge(body: Uint8Array): string | undefined {
  if (body.length > MAX_VERIFICATION_BYTES) return undefined;
  try {
    const parsed: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(body));
    if (parsed !== null && typeof parsed === "object") {
      const { type, challenge } = parsed as Record<string, unknown>;
      if (type === "verification" && typeof challenge === "string") return challenge;
    }
  } catch {}
  return undefined;
}

function wholeNumber(value: string | null, fallback: number, max: number): number | undefined {
  if (value === null) return fallback;
  if (!/^\d{1,15}$/.test(value)) return undefined;
  return Math.min(Number(value), max);
}

/** A POST from an MCP server to a hook path. Not authenticated: the unguessable path is the address. */
async function receive(request: Request, store: Store, token: string, receivedAt: number): Promise<Response> {
  if (Number(request.headers.get("content-length") ?? 0) > MAX_BODY_BYTES) return error(413, "body too large");
  const body = new Uint8Array(await request.arrayBuffer());
  if (body.length > MAX_BODY_BYTES) return error(413, "body too large");

  // Echoed for any registered path. The reader registered it, and that is the consent the handshake asks for.
  const challenge = verificationChallenge(body);
  if (challenge !== undefined) {
    return (await store.hasHook(token)) ? json(200, { challenge }) : error(404, "not found");
  }

  const headers: Record<string, string> = {};
  for (const name of KEPT_HEADERS) {
    const value = request.headers.get(name);
    if (value !== null) headers[name] = value;
  }
  const seq = await store.insert(token, receivedAt, headers, body);
  if (seq === "unknown") return error(404, "not found");
  if (seq === "full") return error(503, "mailbox full");
  return json(200, { seq });
}

export async function handle(request: Request, store: Store, opts: MailboxOptions): Promise<Response> {
  const now = (opts.now ?? Date.now)();
  const url = new URL(request.url);
  const parts = url.pathname.split("/").filter(Boolean);
  const route = `${request.method} /${parts[0] ?? ""}${parts.length > 1 ? "/:token" : ""}`;
  if (parts.length > 2) return error(404, "not found");
  const token = parts[1];
  if (token !== undefined && !TOKEN.test(token)) return error(404, "not found");

  if (route === "POST /hooks/:token") return receive(request, store, token!, now);

  if (!["POST /hooks", "GET /hooks", "DELETE /hooks/:token", "GET /deliveries", "POST /ack"].includes(route)) {
    return error(404, "not found");
  }
  if (opts.key === undefined || opts.key.length < MIN_KEY_LENGTH) return error(503, "mailbox key is not configured");
  const given = /^Bearer (.+)$/.exec(request.headers.get("authorization") ?? "")?.[1];
  if (given === undefined || !(await sameString(given, opts.key))) return error(401, "unauthorized");

  switch (route) {
    case "POST /hooks": {
      const input = (await request.json().catch(() => ({}))) as { label?: unknown } | null;
      const label = typeof input?.label === "string" ? input.label.slice(0, 200) : "";
      const created = newToken();
      await store.createHook(created, label, now);
      return json(201, { token: created, url: `${url.origin}/hooks/${created}`, label });
    }
    case "GET /hooks":
      return json(200, { hooks: await store.listHooks() });
    case "DELETE /hooks/:token":
      return (await store.deleteHook(token!)) ? json(200, {}) : error(404, "not found");
    case "GET /deliveries": {
      const after = wholeNumber(url.searchParams.get("after"), 0, Number.MAX_SAFE_INTEGER);
      const limit = wholeNumber(url.searchParams.get("limit"), 100, MAX_FETCH);
      const wait = wholeNumber(url.searchParams.get("wait"), 0, MAX_WAIT_MS / 1000);
      if (after === undefined || limit === undefined || wait === undefined) return error(400, "after, limit and wait are whole numbers");
      const hook = url.searchParams.get("hook") ?? undefined;
      if (hook !== undefined && !TOKEN.test(hook)) return error(400, "hook is a hook token");
      const deliveries = await store.list(after, Math.max(1, limit), wait * 1000, hook);
      return json(200, { deliveries: deliveries.map((d) => ({ ...d, body: base64(d.body) })) });
    }
    default: {
      const input = (await request.json().catch(() => null)) as { upTo?: unknown; hook?: unknown } | null;
      if (typeof input?.upTo !== "number" || !Number.isSafeInteger(input.upTo) || input.upTo < 0) return error(400, "upTo is a whole number");
      if (input.hook !== undefined && (typeof input.hook !== "string" || !TOKEN.test(input.hook))) return error(400, "hook is a hook token");
      return json(200, { deleted: await store.ack(input.upTo, input.hook) });
    }
  }
}
