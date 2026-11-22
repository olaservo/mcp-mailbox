import assert from "node:assert/strict";
import { createHmac, randomBytes } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { PollEndpoint } from "../src/reader/poll-endpoint.ts";
import { Reader, type Received, type ReaderOptions } from "../src/reader/reader.ts";
import { reader as mailboxClient, startMailbox, type Target } from "./harness.ts";

let target: Target;
let server: Awaited<ReturnType<typeof fakeServer>>;

before(async () => {
  target = await startMailbox();
  server = await fakeServer();
});
after(async () => {
  await server.close();
  await target.close();
});

interface Subscribed {
  id: string;
  name: string;
  url: string;
  secret: string;
  headers: Record<string, string | string[] | undefined>;
  /** The cursor the request carried. */
  cursor: string | null;
}

/** Stands in for an MCP server: answers `events/subscribe` after a challenge to the callback, and records the request. */
async function fakeServer() {
  const subscribed: Subscribed[] = [];
  const unsubscribed: string[] = [];
  const http = createServer((req, res) => {
    let text = "";
    req.on("data", (chunk) => (text += chunk));
    req.on("end", async () => {
      const { id, method, params } = JSON.parse(text) as { id: number; method: string; params: { name: string; cursor: string | null; delivery: { url: string; secret: string } } };
      const reply = (body: object) => res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ jsonrpc: "2.0", id, ...body }));
      if (method === "events/unsubscribe") {
        unsubscribed.push(params.name);
        return reply({ result: {} });
      }
      const challenge = randomBytes(12).toString("hex");
      const echoed = await fetch(params.delivery.url, { method: "POST", body: JSON.stringify({ type: "verification", challenge }) });
      if (((await echoed.json().catch(() => ({}))) as { challenge?: string }).challenge !== challenge) {
        return reply({ error: { code: -32027, message: "CallbackEndpointError" } });
      }
      const sub = { id: `sub_${params.name}`, name: params.name, url: params.delivery.url, secret: params.delivery.secret, headers: req.headers, cursor: params.cursor };
      subscribed.push(sub);
      reply({ result: { id: sub.id, refreshBefore: new Date(Date.now() + 3600_000).toISOString(), cursor: `c:${subscribed.length}`, truncated: false } });
    });
  });
  await new Promise<void>((resolve) => http.listen(0, "127.0.0.1", resolve));
  return {
    url: `http://127.0.0.1:${(http.address() as AddressInfo).port}/mcp`,
    subscribed,
    unsubscribed,
    close: () => new Promise<void>((resolve) => { http.closeAllConnections(); http.close(() => resolve()); }),
  };
}

interface Send {
  secret?: string;
  /** Seconds since the epoch in `webhook-timestamp`. */
  timestamp?: number;
  subscription?: string;
  /** Changes the body after signing. */
  alter?: (body: string) => string;
}

/** POSTs a signed event to the subscription's callback, as the server would. */
async function send(sub: Subscribed, eventId: string, data: unknown, opts: Send = {}): Promise<void> {
  const body = JSON.stringify({ eventId, name: sub.name, timestamp: new Date().toISOString(), data, cursor: null });
  const timestamp = opts.timestamp ?? Math.floor(Date.now() / 1000);
  const key = Buffer.from((opts.secret ?? sub.secret).slice(6), "base64");
  const signature = createHmac("sha256", key).update(`${eventId}.${timestamp}.${body}`).digest("base64");
  const res = await fetch(sub.url, {
    method: "POST",
    headers: { "content-type": "application/json", "webhook-id": eventId, "webhook-timestamp": String(timestamp), "webhook-signature": `v1,${signature}`, "x-mcp-subscription-id": opts.subscription ?? sub.id },
    body: opts.alter ? opts.alter(body) : body,
  });
  assert.equal(res.status, 200);
}

function setUp(events: string[], extra: Partial<ReaderOptions> = {}) {
  const got: Received[] = [];
  const lines: string[] = [];
  const options: ReaderOptions = {
    mailbox: { url: target.base, key: target.key },
    server: { url: server.url, token: "s3cret" },
    events,
    stateFile: join(mkdtempSync(join(tmpdir(), "reader-")), "state.json"),
    settleMs: 50,
    handle: (received) => void got.push(received),
    log: (line) => lines.push(line),
    ...extra,
  };
  const sub = (name: string) => server.subscribed.findLast((s) => s.name === name && s.url === reader.hookUrl)!;
  const reader = new Reader(options);
  return { reader, got, lines, options, sub, ids: () => got.map((r) => r.event.eventId) };
}

test("subscribes with a mailbox hook as the callback", async () => {
  const { reader, sub } = setUp(["a.moved"]);
  await reader.subscribe();
  const made = sub("a.moved");
  assert.ok(made.url.startsWith(`${target.base}/hooks/`));
  assert.match(made.secret, /^whsec_/);
  assert.equal(made.headers.authorization, "Bearer s3cret");
  assert.equal(made.headers["mcp-method"], "events/subscribe");
  assert.equal(made.headers["mcp-name"], "a.moved");
  assert.equal(made.headers["mcp-protocol-version"], "2026-07-28");
});

test("events that arrive out of order are handed on in order, then acked", async () => {
  const { reader, ids, sub } = setUp(["a.moved", "a.chat"]);
  await reader.subscribe();
  await send(sub("a.moved"), "ep-3", { n: 3 });
  await send(sub("a.chat"), "ep-2", { n: 2 });
  await send(sub("a.moved"), "ep-1", { n: 1 });
  assert.equal(await reader.poll(), 3);
  assert.deepEqual(ids(), ["ep-1", "ep-2", "ep-3"]);
  const token = reader.hookUrl!.split("/").at(-1)!;
  assert.deepEqual(await mailboxClient(target).deliveries(0, `&hook=${token}`), [], "the mailbox holds nothing for the hook");
  assert.equal(await reader.poll(), 0);
});

test("a delivery that was altered, forged, stale, repeated or moved is refused", async () => {
  const { reader, ids, lines, sub } = setUp(["a.moved", "a.chat"]);
  await reader.subscribe();
  const moved = sub("a.moved");
  await send(moved, "ep-1", { ok: true });
  await send(moved, "ep-2", {}, { alter: (body) => body.replace("ep-2", "ep-9") });
  await send(moved, "ep-3", {}, { secret: `whsec_${randomBytes(32).toString("base64")}` });
  await send(moved, "ep-4", {}, { timestamp: Math.floor(Date.now() / 1000) - 600 });
  await send(moved, "ep-1", { ok: true });
  await send(moved, "ep-5", {}, { subscription: sub("a.chat").id });
  await send(moved, "ep-6", {}, { subscription: "sub_other" });
  assert.equal(await reader.poll(), 1);
  assert.deepEqual(ids(), ["ep-1"]);
  const reasons = lines.filter((line) => line.startsWith("skipped")).map((line) => line.split(": ")[1]);
  assert.deepEqual(reasons, ["bad signature", "bad signature", "timestamp is not close to the time of receipt", "repeat", "event a.moved on a subscription for a.chat", "unknown subscription"]);

  // The mailbox hands the same delivery out again after it was acked.
  await send(moved, "ep-1", { ok: true });
  assert.equal(await reader.poll(), 0);
  assert.deepEqual(ids(), ["ep-1"]);
});

test("a delivery held in the mailbox is accepted against its time of receipt", async () => {
  const hour = 60 * 60 * 1000;
  const { reader, ids, sub } = setUp(["a.moved"], { now: () => Date.now() + 6 * hour });
  await reader.subscribe();
  await send(sub("a.moved"), "ep-1", {});
  assert.equal(await reader.poll(), 1);
  assert.deepEqual(ids(), ["ep-1"]);

  const late = setUp(["a.moved"], { now: () => Date.now() + 9 * 24 * hour });
  await late.reader.subscribe();
  await send(late.sub("a.moved"), "ep-1", {});
  assert.equal(await late.reader.poll(), 0, "older than the mailbox keeps anything");
});

test("a restarted reader keeps its hook, secret and position", async () => {
  const first = setUp(["a.moved"]);
  await first.reader.subscribe();
  await send(first.sub("a.moved"), "ep-1", {});
  assert.equal(await first.reader.poll(), 1);

  const second = setUp(["a.moved"], { stateFile: first.options.stateFile });
  assert.equal(second.reader.hookUrl, first.reader.hookUrl);
  await second.reader.subscribe();
  assert.equal(second.sub("a.moved").secret, first.sub("a.moved").secret);
  await send(second.sub("a.moved"), "ep-1", {});
  await send(second.sub("a.moved"), "ep-2", {});
  assert.equal(await second.reader.poll(), 1);
  assert.deepEqual(second.ids(), ["ep-2"]);
});

test("a refresh sends back the cursor the server returned, also after a restart", async () => {
  const first = setUp(["a.moved"]);
  await first.reader.subscribe();
  const made = first.sub("a.moved");
  assert.equal(made.cursor, null);
  const returned = `c:${server.subscribed.indexOf(made) + 1}`;
  await first.reader.subscribe();
  assert.equal(first.sub("a.moved").cursor, returned);

  const second = setUp(["a.moved"], { stateFile: first.options.stateFile });
  const again = `c:${server.subscribed.length}`;
  await second.reader.subscribe();
  assert.equal(second.sub("a.moved").cursor, again);
});

test("two readers on one mailbox each get their own events", async () => {
  const one = setUp(["a.moved"]);
  const two = setUp(["a.moved"]);
  await one.reader.subscribe();
  await two.reader.subscribe();
  await send(one.sub("a.moved"), "ep-1", {});
  await send(two.sub("a.moved"), "ep-2", {});
  assert.equal(await one.reader.poll(), 1);
  assert.equal(await two.reader.poll(), 1);
  assert.deepEqual([one.ids(), two.ids()], [["ep-1"], ["ep-2"]]);
  assert.deepEqual([...one.lines, ...two.lines].filter((line) => line.startsWith("skipped")), [], "neither fetched the other's delivery");
});

test("a failing handler does not stop the events after it", async () => {
  const seen: string[] = [];
  const { reader, lines, sub } = setUp(["a.moved"], {
    handle: ({ event }) => {
      if (event.eventId === "ep-1") throw new Error("no");
      seen.push(event.eventId);
    },
  });
  await reader.subscribe();
  await send(sub("a.moved"), "ep-1", {});
  await send(sub("a.moved"), "ep-2", {});
  await reader.poll();
  assert.deepEqual(seen, ["ep-2"]);
  assert.ok(lines.includes("handler failed on ep-1: no"));
});

test("run subscribes, hands on an event that arrives while it waits, and stops", async () => {
  const { reader, ids, sub } = setUp(["a.moved"], { waitSeconds: 5 });
  const stop = new AbortController();
  const running = reader.run(stop.signal);
  while (!reader.hookUrl || !sub("a.moved")) await new Promise((resolve) => setTimeout(resolve, 20));
  await new Promise((resolve) => setTimeout(resolve, 200));
  await send(sub("a.moved"), "ep-1", {});
  const started = Date.now();
  while (ids().length === 0 && Date.now() - started < 5000) await new Promise((resolve) => setTimeout(resolve, 20));
  assert.deepEqual(ids(), ["ep-1"]);
  // Long enough for the next waiting fetch to be in flight.
  await new Promise((resolve) => setTimeout(resolve, 500));
  const stopped = Date.now();
  stop.abort();
  await running;
  assert.ok(Date.now() - stopped < 2000, "stops without waiting out the fetch");
});

test("a hook the mailbox no longer has is replaced at the next refresh", async () => {
  const { reader, ids, sub } = setUp(["a.moved"]);
  await reader.subscribe();
  const old = reader.hookUrl!;
  await mailboxClient(target).deleteHook(old.split("/").at(-1)!);
  await reader.subscribe();
  assert.notEqual(reader.hookUrl, old);
  await send(sub("a.moved"), "ep-1", {});
  assert.equal(await reader.poll(), 1);
  assert.deepEqual(ids(), ["ep-1"]);
});

test("unsubscribe tells the server and removes the hook", async () => {
  const { reader, sub } = setUp(["a.gone"]);
  await reader.subscribe();
  const url = sub("a.gone").url;
  await reader.unsubscribe();
  assert.ok(server.unsubscribed.includes("a.gone"));
  assert.equal(reader.hookUrl, undefined);
  assert.equal((await fetch(url, { method: "POST", body: "{}" })).status, 404);
});

test("a polling client gets the reader's events from the poll endpoint", async () => {
  const endpoint = new PollEndpoint(["a.moved", "a.chat"]);
  const { reader, sub } = setUp(["a.moved", "a.chat"], { handle: ({ event }) => endpoint.add(event) });
  const served = await endpoint.listen(0);
  // The request the mcp-events mod sends.
  const rpc = async (method: string, params: object, headers: Record<string, string> = {}) => {
    const res = await fetch(`http://127.0.0.1:${served.port}/mcp`, {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json, text/event-stream", "mcp-method": method, ...headers },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
    });
    return { status: res.status, ...((await res.json()) as { result?: any; error?: { code: number } }) };
  };
  const poll = async (name: string, cursor: string | null, extra: object = {}) => (await rpc("events/poll", { name, arguments: {}, cursor, ...extra })).result;
  try {
    await reader.subscribe();
    assert.deepEqual((await rpc("events/list", {})).result.events, [{ name: "a.moved", delivery: ["poll"] }, { name: "a.chat", delivery: ["poll"] }]);

    await send(sub("a.moved"), "ep-1", { before: true });
    await reader.poll();
    const start = await poll("a.moved", null);
    assert.deepEqual([start.events, start.truncated], [[], false], "no cursor starts from now");

    await send(sub("a.moved"), "ep-3", { n: 3 });
    await send(sub("a.chat"), "ep-4", { n: 4 });
    await send(sub("a.moved"), "ep-2", { n: 2 });
    await reader.poll();
    const one = await poll("a.moved", start.cursor, { maxEvents: 1 });
    assert.deepEqual([one.events.map((e: { eventId: string }) => e.eventId), one.hasMore], [["ep-2"], true]);
    const two = await poll("a.moved", one.cursor);
    assert.deepEqual(two.events, [{ eventId: "ep-3", name: "a.moved", timestamp: two.events[0].timestamp, data: { n: 3 } }]);
    assert.deepEqual((await poll("a.moved", two.cursor)).events, []);
    assert.deepEqual((await poll("a.chat", start.cursor)).events.map((e: { eventId: string }) => e.eventId), ["ep-4"]);

    await new Promise((resolve) => setTimeout(resolve, 30));
    const aged = await poll("a.moved", start.cursor, { maxAgeMs: 10 });
    assert.deepEqual([aged.events, aged.truncated], [[], true], "events older than maxAgeMs are reported as a gap");
    assert.equal((await poll("a.moved", "00000000:1")).truncated, true, "a cursor from another run");
    assert.equal((await rpc("events/poll", { name: "a.other", cursor: null })).error?.code, -32602);
    assert.equal((await rpc("events/subscribe", { name: "a.moved" })).error?.code, -32601);
    assert.equal((await rpc("events/list", {}, { origin: "https://example.com" })).status, 403);
  } finally {
    await served.close();
  }
});

test("servers in the directory are each subscribed, with the token from the file", async () => {
  const other = await fakeServer();
  const dir = mkdtempSync(join(tmpdir(), "servers-"));
  // The first file names the server the reader already has.
  writeFileSync(join(dir, "1.json"), JSON.stringify({ url: server.url }));
  writeFileSync(join(dir, "2.json"), JSON.stringify({ url: other.url, token: "t2" }));
  // A file still being written has another name. Subscribing to this one would fail.
  writeFileSync(join(dir, "3.json.tmp"), JSON.stringify({ url: "http://127.0.0.1:9/mcp" }));
  const { reader, ids, lines, sub } = setUp(["a.moved"], { servers: { dir, token: "unused" } });
  const fromOther = () => other.subscribed.filter((s) => s.url === reader.hookUrl);
  try {
    await reader.subscribe();
    assert.equal(server.subscribed.filter((s) => s.url === reader.hookUrl).length, 1, "the server named twice is subscribed once");
    assert.equal(sub("a.moved").headers.authorization, "Bearer s3cret");
    assert.equal(fromOther().length, 1);
    assert.equal(fromOther()[0]!.headers.authorization, "Bearer t2");

    // Both servers grant the same subscription id, as two forge-mcp-server processes with one token do.
    await send(sub("a.moved"), "one-1", {});
    await send(fromOther()[0]!, "two-1", {});
    assert.equal(await reader.poll(), 2);
    assert.deepEqual(ids().sort(), ["one-1", "two-1"]);
    assert.deepEqual(lines.filter((line) => line.startsWith("skipped")), []);

    rmSync(join(dir, "2.json"));
    await reader.subscribe();
    assert.equal(fromOther().length, 1, "a server whose file is gone is not refreshed");
    assert.equal(sub("a.moved").cursor !== null, true, "the remaining server is");
  } finally {
    await other.close();
  }
});

test("run subscribes to a server that appears in the directory, and goes on when one is down", async () => {
  const other = await fakeServer();
  const dir = join(mkdtempSync(join(tmpdir(), "servers-")), "not-yet");
  const { reader, ids, lines } = setUp(["a.moved"], { server: undefined, servers: { dir }, scanMs: 50, waitSeconds: 1 });
  const stop = new AbortController();
  const running = reader.run(stop.signal);
  const until = async (done: () => boolean) => {
    for (const started = Date.now(); !done() && Date.now() - started < 5000; ) await new Promise((resolve) => setTimeout(resolve, 20));
    assert.ok(done());
  };
  try {
    await new Promise((resolve) => setTimeout(resolve, 200));
    assert.equal(reader.hookUrl, undefined, "nothing to do before a server appears");
    mkdirSync(dir);
    writeFileSync(join(dir, "down.json"), JSON.stringify({ url: "http://127.0.0.1:9/mcp" }));
    writeFileSync(join(dir, "up.json"), JSON.stringify({ url: other.url }));
    const made = () => other.subscribed.find((s) => s.url === reader.hookUrl);
    await until(() => made() !== undefined);
    await send(made()!, "two-1", {});
    await until(() => ids().length === 1);
    await new Promise((resolve) => setTimeout(resolve, 300));
    assert.equal(lines.filter((line) => line.startsWith("subscribe failed at http://127.0.0.1:9/mcp")).length, 1, "the server that is down is logged and does not stop the other");
  } finally {
    stop.abort();
    await running;
    await other.close();
  }
});

test("a state file from before several servers keeps its subscriptions and cursors", async () => {
  const first = setUp(["a.moved"]);
  await first.reader.subscribe();
  const made = first.sub("a.moved");
  const state = JSON.parse(readFileSync(first.options.stateFile, "utf8"));
  const { servers, ...rest } = state;
  writeFileSync(first.options.stateFile, JSON.stringify({ ...rest, ...servers[server.url] }));

  const second = setUp(["a.moved"], { stateFile: first.options.stateFile });
  await send(made, "ep-1", {});
  assert.equal(await second.reader.poll(), 1, "the subscription id is still known");
  await second.reader.subscribe();
  assert.equal(second.sub("a.moved").cursor, servers[server.url].cursors["a.moved"]);
});
