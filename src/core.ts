/**
 * The mailbox's HTTP surface, independent of where it runs.
 *
 * The mailbox holds no signing secret. It does not verify signatures or check timestamps. It records
 * when each delivery arrived, and the reader verifies against that time and deduplicates.
 */

const MAX_BODY_BYTES = 256 * 1024;
export const MAX_STORED = 10_000;
export const MAX_PER_HOOK = 2_000;
export const RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const MIN_IDLE_SECONDS = 60;
const MAX_IDLE_SECONDS = RETENTION_MS / 1000;
const MAX_VERIFICATION_BYTES = 4096;
const MAX_FETCH = 500;
const MAX_WAIT_MS = 25_000;
const MIN_KEY_LENGTH = 32;
/** Hooks a mailbox with open hook creation holds before it refuses another without the admin key. */
export const MAX_OPEN_HOOKS = 1_000;
const TOKEN = /^[A-Za-z0-9_-]{43}$/;
const RECEIVER_DOCUMENT = "/.well-known/mcp-webhook-receiver.json";

/** Headers kept with each delivery. The reader needs them to verify. */
const KEPT_HEADERS = ["content-type", "webhook-id", "webhook-timestamp", "webhook-signature", "x-mcp-subscription-id"];

export interface Hook {
  token: string;
  label: string;
  createdAt: number;
  /** Epoch ms of the last fetch for this hook, or its creation. */
  lastReadAt: number;
  /** A hook not fetched for this long is removed with its deliveries. Absent: it is kept. */
  idleSeconds?: number;
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

/** A hook with the SHA-256 of its read key, in base64url. */
export interface StoredHook extends Hook {
  readKeyHash?: string;
}

export interface Store {
  /** Returns false, and creates nothing, when `maxHooks` is given and that many hooks are held. */
  createHook(token: string, label: string, now: number, idleSeconds: number | undefined, readKeyHash: string, maxHooks?: number): Promise<boolean>;
  /** Removes the hook and its deliveries. */
  deleteHook(token: string): Promise<boolean>;
  getHook(token: string): Promise<StoredHook | undefined>;
  listHooks(): Promise<Hook[]>;
  /** Stores a delivery and returns its sequence number. Also drops old deliveries and idle hooks. */
  insert(token: string, receivedAt: number, headers: Record<string, string>, body: Uint8Array): Promise<number | "unknown" | "full" | "hook full">;
  /** Deliveries with `seq > after`, oldest first, for one hook when `hook` is given, which also counts as that hook's read. With none and `waitMs > 0`, waits up to that long for one. */
  list(after: number, limit: number, waitMs: number, hook: string | undefined, now: number): Promise<Delivery[]>;
  /** Deletes deliveries with `seq <= upTo`, for one hook when `hook` is given. Returns how many. */
  ack(upTo: number, hook?: string): Promise<number>;
}

export interface MailboxOptions {
  /** The admin key, for every reader route on every hook. Ignored when shorter than 32 characters. */
  key: string | undefined;
  /** Anyone may create a hook, up to `maxHooks`. Each hook's read key is the only way to read it. */
  openHooks?: boolean;
  maxHooks?: number;
  /** Asked before a hook is created without the admin key. False answers 429. */
  allowOpenHook?: (request: Request) => Promise<boolean>;
  now?: () => number;
}

/** Lets a `list` call wait for the next `insert`, for one hook or for any. */
export class Waiters {
  private readonly waiting = new Set<{ hook: string | undefined; done: () => void }>();

  wake(hook: string): void {
    for (const waiter of [...this.waiting]) if (waiter.hook === undefined || waiter.hook === hook) waiter.done();
  }

  wait(ms: number, hook?: string): Promise<void> {
    return new Promise((resolve) => {
      const waiter = {
        hook,
        done: () => {
          clearTimeout(timer);
          this.waiting.delete(waiter);
          resolve();
        },
      };
      const timer = setTimeout(waiter.done, ms);
      this.waiting.add(waiter);
    });
  }
}

/**
 * A store that makes a read or an ack once more, on a new store from `open`, when it fails with an error marked
 * `retryable` and not `overloaded`. `createHook`, `deleteHook` and `insert` are made once: the first call may have taken effect.
 */
export function retrying(open: () => Store, waitMs = 100): Store {
  const twice = async <T>(call: (store: Store) => Promise<T>): Promise<T> => {
    try {
      return await call(open());
    } catch (err) {
      const { retryable, overloaded } = err as { retryable?: unknown; overloaded?: unknown };
      if (retryable !== true || overloaded === true) throw err;
      await new Promise((resolve) => setTimeout(resolve, waitMs));
      return call(open());
    }
  };
  return {
    createHook: (...args) => open().createHook(...args),
    deleteHook: (...args) => open().deleteHook(...args),
    insert: (...args) => open().insert(...args),
    getHook: (...args) => twice((store) => store.getHook(...args)),
    listHooks: () => twice((store) => store.listHooks()),
    list: (...args) => twice((store) => store.list(...args)),
    ack: (...args) => twice((store) => store.ack(...args)),
  };
}

const json = (status: number, value: unknown) => Response.json(value, { status });
const error = (status: number, message: string) => json(status, { error: message });

/** The answer when the store throws. The reason is logged, and sent only to a caller that gave the admin key. */
function unavailable(route: string, err: unknown, withReason: boolean): Response {
  const reason = err instanceof Error ? err.message : String(err);
  console.error(`${route}: ${reason}`);
  return Response.json({ error: withReason ? `mailbox unavailable: ${reason}` : "mailbox unavailable" }, { status: 503, headers: { "retry-after": "1" } });
}

function base64(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i += 0x8000) binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(binary);
}

function newToken(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return base64(bytes).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}

const sha256 = async (s: string) => new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s)));
const hashKey = async (key: string) => base64(await sha256(key)).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");

/** Compares digests, so the time taken does not depend on where the strings differ. */
async function sameString(a: string, b: string): Promise<boolean> {
  const [x, y] = await Promise.all([sha256(a), sha256(b)]);
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
    return (await store.getHook(token)) ? json(200, { challenge }) : error(404, "not found");
  }

  const headers: Record<string, string> = {};
  for (const name of KEPT_HEADERS) {
    const value = request.headers.get(name);
    if (value !== null) headers[name] = value;
  }
  const seq = await store.insert(token, receivedAt, headers, body);
  if (seq === "unknown") return error(404, "not found");
  if (seq === "full") return error(503, "mailbox full");
  if (seq === "hook full") return error(503, "hook full");
  return json(200, { seq });
}

export async function handle(request: Request, store: Store, opts: MailboxOptions): Promise<Response> {
  const now = (opts.now ?? Date.now)();
  const url = new URL(request.url);
  // SEP-3415 endpoint verification, option 4: the origin consents to deliveries under /hooks/.
  if (request.method === "GET" && url.pathname === RECEIVER_DOCUMENT) {
    return Response.json({ receivers: ["/hooks/"] }, { headers: { "cache-control": "public, max-age=3600" } });
  }
  const parts = url.pathname.split("/").filter(Boolean);
  const route = `${request.method} /${parts[0] ?? ""}${parts.length > 1 ? "/:token" : ""}`;
  if (parts.length > 2) return error(404, "not found");
  const token = parts[1];
  if (token !== undefined && !TOKEN.test(token)) return error(404, "not found");

  if (route === "POST /hooks/:token") return receive(request, store, token!, now).catch((err: unknown) => unavailable(route, err, false));

  if (!["POST /hooks", "GET /hooks", "GET /hooks/:token", "DELETE /hooks/:token", "GET /deliveries", "POST /ack"].includes(route)) {
    return error(404, "not found");
  }
  const adminKey = opts.key !== undefined && opts.key.length >= MIN_KEY_LENGTH ? opts.key : undefined;
  if (adminKey === undefined && !opts.openHooks && route === "POST /hooks") return error(503, "mailbox key is not configured");
  const given = /^Bearer (.+)$/.exec(request.headers.get("authorization") ?? "")?.[1];
  const admin = given !== undefined && adminKey !== undefined && (await sameString(given, adminKey));
  const unauthorized = () => error(401, "unauthorized");
  // The admin key reads every hook. A read key reads only the hook it was made with. A hook that is gone is
  // 404, so a reader can tell it from a wrong key. Its URL already tells anyone that.
  const refused = async (hook: unknown): Promise<Response | undefined> => {
    if (admin) return undefined;
    if (given === undefined || typeof hook !== "string" || !TOKEN.test(hook)) return unauthorized();
    const found = await store.getHook(hook);
    if (!found) return error(404, "not found");
    return found.readKeyHash !== undefined && (await sameString(await hashKey(given), found.readKeyHash)) ? undefined : unauthorized();
  };

  try {
    switch (route) {
      case "POST /hooks": {
        if (!admin && !opts.openHooks) return unauthorized();
        if (!admin && opts.allowOpenHook && !(await opts.allowOpenHook(request))) {
          return Response.json({ error: "too many hooks created from here; try again in a minute" }, { status: 429, headers: { "retry-after": "60" } });
        }
        const input = (await request.json().catch(() => ({}))) as { label?: unknown; idleSeconds?: unknown } | null;
        const label = typeof input?.label === "string" ? input.label.slice(0, 200) : "";
        const asked = input?.idleSeconds;
        if (asked !== undefined && (!Number.isSafeInteger(asked) || (asked as number) < MIN_IDLE_SECONDS || (asked as number) > MAX_IDLE_SECONDS)) {
          return error(400, `idleSeconds is a whole number from ${MIN_IDLE_SECONDS} to ${MAX_IDLE_SECONDS}`);
        }
        // A hook made without the admin key always expires, so an abandoned one frees its place.
        const idleSeconds = (asked as number | undefined) ?? (admin ? undefined : MAX_IDLE_SECONDS);
        const created = newToken();
        const readKey = newToken();
        if (!(await store.createHook(created, label, now, idleSeconds, await hashKey(readKey), admin ? undefined : (opts.maxHooks ?? MAX_OPEN_HOOKS)))) {
          return error(503, "too many hooks");
        }
        return json(201, { token: created, url: `${url.origin}/hooks/${created}`, readKey, label, ...(idleSeconds !== undefined ? { idleSeconds } : {}) });
      }
      case "GET /hooks":
        if (!admin) return unauthorized();
        return json(200, { hooks: await store.listHooks() });
      case "GET /hooks/:token": {
        const no = await refused(token);
        if (no) return no;
        const found = await store.getHook(token!);
        if (!found) return error(404, "not found");
        const { readKeyHash: _hash, ...hook } = found;
        return json(200, hook);
      }
      case "DELETE /hooks/:token":
        return (await refused(token)) ?? ((await store.deleteHook(token!)) ? json(200, {}) : error(404, "not found"));
      case "GET /deliveries": {
        const after = wholeNumber(url.searchParams.get("after"), 0, Number.MAX_SAFE_INTEGER);
        const limit = wholeNumber(url.searchParams.get("limit"), 100, MAX_FETCH);
        const wait = wholeNumber(url.searchParams.get("wait"), 0, MAX_WAIT_MS / 1000);
        if (after === undefined || limit === undefined || wait === undefined) return error(400, "after, limit and wait are whole numbers");
        const hook = url.searchParams.get("hook") ?? undefined;
        const no = await refused(hook);
        if (no) return no;
        if (hook !== undefined && !TOKEN.test(hook)) return error(400, "hook is a hook token");
        const deliveries = await store.list(after, Math.max(1, limit), wait * 1000, hook, now);
        return json(200, { deliveries: deliveries.map((d) => ({ ...d, body: base64(d.body) })) });
      }
      default: {
        const input = (await request.json().catch(() => null)) as { upTo?: unknown; hook?: unknown } | null;
        const no = await refused(input?.hook);
        if (no) return no;
        if (typeof input?.upTo !== "number" || !Number.isSafeInteger(input.upTo) || input.upTo < 0) return error(400, "upTo is a whole number");
        if (input.hook !== undefined && (typeof input.hook !== "string" || !TOKEN.test(input.hook))) return error(400, "hook is a hook token");
        return json(200, { deleted: await store.ack(input.upTo, input.hook) });
      }
    }
  } catch (err) {
    return unavailable(route, err, admin);
  }
}
