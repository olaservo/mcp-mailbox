import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { handle, retrying } from "../src/core.ts";
import { MemoryStore } from "../src/memory.ts";
import { reader, startMailbox, type Target } from "./harness.ts";

let target: Target;
let mailbox: ReturnType<typeof reader>;

before(async () => {
  target = await startMailbox();
  mailbox = reader(target);
});
after(() => target.close());

const unknownToken = "A".repeat(43);
const post = (url: string, body: string | Buffer, headers: Record<string, string> = {}) => fetch(url, { method: "POST", headers, body });

test("reader routes need the key", async () => {
  const { token } = await mailbox.createHook("keyed");
  for (const [method, path] of [["POST", "/hooks"], ["GET", "/hooks"], ["GET", `/hooks/${token}`], ["GET", "/deliveries"], ["POST", "/ack"], ["DELETE", `/hooks/${token}`]] as const) {
    const none = await fetch(`${target.base}${path}`, { method });
    assert.equal(none.status, 401, `${method} ${path} without a key`);
    const wrong = await fetch(`${target.base}${path}`, { method, headers: { authorization: `Bearer ${"x".repeat(43)}` } });
    assert.equal(wrong.status, 401, `${method} ${path} with a wrong key`);
  }
  await mailbox.deleteHook(token);
});

test("the well-known receiver document covers /hooks/ and needs no key", async () => {
  const res = await fetch(`${target.base}/.well-known/mcp-webhook-receiver.json`);
  assert.equal(res.status, 200);
  assert.deepEqual(await res.json(), { receivers: ["/hooks/"] });
});

test("a path that was not registered is not found", async () => {
  const challenge = await post(`${target.base}/hooks/${unknownToken}`, JSON.stringify({ type: "verification", challenge: "abc" }));
  assert.equal(challenge.status, 404);
  const delivery = await post(`${target.base}/hooks/${unknownToken}`, JSON.stringify({ eventId: "1" }));
  assert.equal(delivery.status, 404);
  assert.equal((await post(`${target.base}/hooks/short`, "{}")).status, 404);
});

test("the verification challenge is echoed and not stored", async () => {
  const hook = await mailbox.createHook("challenge");
  const tip = await mailbox.tip();
  const res = await post(hook.url, JSON.stringify({ type: "verification", challenge: "nonce-123" }));
  assert.equal(res.status, 200);
  assert.deepEqual(await res.json(), { challenge: "nonce-123" });
  assert.deepEqual(await mailbox.deliveries(tip), []);
});

test("a delivery comes back byte for byte with its headers, and ack removes it", async () => {
  const hook = await mailbox.createHook("bytes");
  const tip = await mailbox.tip();
  // Not valid UTF-8, and JSON-like text whose spacing a re-serialization would change.
  const first = Buffer.concat([Buffer.from('{ "a" : 1 ,\r\n "b":"é" }'), Buffer.from([0xff, 0x00, 0xfe])]);
  const second = Buffer.from('{"eventId":"2"}');
  const headers = { "content-type": "application/json", "webhook-id": "evt_1", "webhook-timestamp": "1763740800", "webhook-signature": "v1,AAAA v1,BBBB", "x-mcp-subscription-id": "sub_1", "x-other": "dropped" };
  const sent = Date.now();
  assert.equal((await post(hook.url, first, headers)).status, 200);
  assert.equal((await post(hook.url, second, { ...headers, "webhook-id": "evt_2" })).status, 200);

  const got = await mailbox.deliveries(tip, `&hook=${hook.token}`);
  assert.equal(got.length, 2);
  assert.deepEqual(Buffer.from(got[0]!.body, "base64"), first);
  assert.deepEqual(Buffer.from(got[1]!.body, "base64"), second);
  const { "x-other": _dropped, ...kept } = headers;
  assert.deepEqual(got[0]!.headers, kept);
  assert.equal(got[0]!.hook, hook.token);
  assert.ok(got[1]!.seq > got[0]!.seq);
  assert.ok(Math.abs(got[0]!.receivedAt - sent) < 5000, "receivedAt is the arrival time");

  assert.equal((await mailbox.deliveries(got[0]!.seq, `&hook=${hook.token}`)).length, 1, "after skips what was read");
  assert.equal((await mailbox.deliveries(tip, `&limit=1&hook=${hook.token}`)).length, 1);
  assert.equal(await mailbox.ack(got[1]!.seq, hook.token), 2);
  assert.deepEqual(await mailbox.deliveries(0, `&hook=${hook.token}`), []);
});

test("a body over 256 KiB is refused with 413", async () => {
  const hook = await mailbox.createHook("large");
  assert.equal((await post(hook.url, Buffer.alloc(256 * 1024 + 1, "a"))).status, 413);
  assert.equal((await post(hook.url, Buffer.alloc(256 * 1024, "a"))).status, 200);
  await mailbox.deleteHook(hook.token);
});

test("a waiting fetch returns when a delivery arrives", async () => {
  const hook = await mailbox.createHook("wait");
  const tip = await mailbox.tip();
  const started = Date.now();
  const waiting = mailbox.deliveries(tip, `&wait=10&hook=${hook.token}`);
  setTimeout(() => void post(hook.url, '{"eventId":"late"}'), 300);
  const got = await waiting;
  assert.equal(got.length, 1);
  assert.ok(Date.now() - started < 5000, `returned after ${Date.now() - started} ms`);
  await mailbox.ack(got[0]!.seq, hook.token);
});

test("a waiting fetch returns empty when nothing arrives", async () => {
  const hook = await mailbox.createHook("quiet");
  const tip = await mailbox.tip();
  const started = Date.now();
  assert.deepEqual(await mailbox.deliveries(tip, `&wait=1&hook=${hook.token}`), []);
  assert.ok(Date.now() - started >= 900);
});

test("a deleted hook stops accepting deliveries, and its deliveries go with it", async () => {
  const hook = await mailbox.createHook("gone");
  const tip = await mailbox.tip();
  assert.equal((await post(hook.url, "left behind")).status, 200);
  assert.equal((await mailbox.deleteHook(hook.token)).status, 200);
  assert.equal((await post(hook.url, "{}")).status, 404);
  assert.equal((await mailbox.deleteHook(hook.token)).status, 404);
  assert.deepEqual((await mailbox.deliveries(tip)).filter((d) => d.hook === hook.token), []);
});

test("a waiting fetch for one hook is not woken by another hook's delivery", async () => {
  const mine = await mailbox.createHook("mine");
  const other = await mailbox.createHook("other");
  const tip = await mailbox.tip();
  const started = Date.now();
  const waiting = mailbox.deliveries(tip, `&wait=2&hook=${mine.token}`);
  setTimeout(() => void post(other.url, "not yours"), 300);
  assert.deepEqual(await waiting, []);
  assert.ok(Date.now() - started >= 1900, `returned after ${Date.now() - started} ms`);
  const late = mailbox.deliveries(tip, `&wait=10&hook=${mine.token}`);
  setTimeout(() => void post(mine.url, "yours"), 300);
  assert.equal((await late).length, 1);
  await mailbox.deleteHook(mine.token);
  await mailbox.deleteHook(other.token);
});

test("a hook at its own limit answers 503 and the others still accept", async (t) => {
  if (target.remote) return t.skip("needs a small limit, set only on the in-memory mailbox");
  const small = await startMailbox({ maxPerHook: 2 });
  try {
    const own = reader(small);
    const stuck = await own.createHook("stuck");
    const live = await own.createHook("live");
    assert.equal((await post(stuck.url, "1")).status, 200);
    assert.equal((await post(stuck.url, "2")).status, 200);
    const refused = await post(stuck.url, "3");
    assert.equal(refused.status, 503);
    assert.deepEqual(await refused.json(), { error: "hook full" });
    assert.equal((await post(live.url, "a")).status, 200);
    await own.ack(1, stuck.token);
    assert.equal((await post(stuck.url, "3")).status, 200);
  } finally {
    await small.close();
  }
});

test("a hook with idleSeconds is removed with its deliveries when not fetched for that long, and one without is kept", async (t) => {
  if (target.remote) return t.skip("needs a clock the test can move, set only on the in-memory mailbox");
  let clock = 1_000_000;
  const timed = await startMailbox({ now: () => clock });
  try {
    const own = reader(timed);
    const session = await own.createHook("session", 60);
    const kept = await own.createHook("reader");
    assert.equal((await post(session.url, "unread")).status, 200);
    clock += 50_000;
    await own.deliveries(0, `&hook=${session.token}`); // a fetch counts as a read
    clock += 50_000;
    assert.equal((await post(session.url, "still here")).status, 200);
    clock += 61_000;
    assert.equal((await post(kept.url, "x")).status, 200); // any request sweeps
    assert.equal((await post(session.url, "late")).status, 404);
    const left = await own.deliveries(0);
    assert.deepEqual(left.map((d) => d.hook), [kept.token]);
    clock += 8 * 24 * 3600_000;
    assert.equal((await post(kept.url, "y")).status, 200);
  } finally {
    await timed.close();
  }
});

test("idleSeconds outside 60 seconds to 7 days is refused", async () => {
  for (const idleSeconds of [59, 7 * 24 * 3600 + 1, 1.5, "60"]) {
    const res = await fetch(`${target.base}/hooks`, { method: "POST", headers: { authorization: `Bearer ${target.key}` }, body: JSON.stringify({ idleSeconds }) });
    assert.equal(res.status, 400, `idleSeconds ${JSON.stringify(idleSeconds)}`);
  }
});

test("a full mailbox answers 503 until the reader acks", async (t) => {
  if (target.remote) return t.skip("needs a small limit, set only on the in-memory mailbox");
  const small = await startMailbox({ maxStored: 2 });
  try {
    const own = reader(small);
    const hook = await own.createHook();
    assert.equal((await post(hook.url, "1")).status, 200);
    assert.equal((await post(hook.url, "2")).status, 200);
    assert.equal((await post(hook.url, "3")).status, 503);
    await own.ack(1);
    assert.equal((await post(hook.url, "3")).status, 200);
  } finally {
    await small.close();
  }
});

test("fetch and ack can be limited to one hook", async () => {
  const one = await mailbox.createHook("one");
  const two = await mailbox.createHook("two");
  const tip = await mailbox.tip();
  await post(one.url, "a");
  await post(two.url, "b");
  await post(one.url, "c");
  const mine = await mailbox.deliveries(tip, `&hook=${one.token}`);
  assert.deepEqual(mine.map((d) => Buffer.from(d.body, "base64").toString()), ["a", "c"]);
  assert.equal(await mailbox.ack(mine.at(-1)!.seq, one.token), 2);
  const left = (await mailbox.deliveries(tip)).filter((d) => d.hook === one.token || d.hook === two.token);
  assert.deepEqual(left.map((d) => d.hook), [two.token]);
  assert.equal((await fetch(`${target.base}/deliveries?hook=short`, { headers: { authorization: `Bearer ${target.key}` } })).status, 400);
  await mailbox.ack(left[0]!.seq, two.token);
});

test("a hook's read key reads, acks and deletes that hook and no other", async () => {
  const mine = await mailbox.createHook("mine");
  const other = await mailbox.createHook("other");
  assert.match(mine.readKey, /^[A-Za-z0-9_-]{43}$/);
  assert.notEqual(mine.readKey, mine.token);
  const tip = await mailbox.tip();
  await post(mine.url, "a");
  await post(other.url, "b");
  const asMine = reader(target, mine.readKey);

  const got = await asMine.deliveries(tip, `&hook=${mine.token}`);
  assert.deepEqual(got.map((d) => Buffer.from(d.body, "base64").toString()), ["a"]);
  assert.equal(await asMine.status("GET", `/deliveries?after=${tip}`), 401, "without hook");
  assert.equal(await asMine.status("GET", `/deliveries?after=${tip}&hook=${other.token}`), 401);
  assert.equal(await asMine.status("GET", "/hooks"), 401);
  assert.equal(await asMine.status("GET", `/hooks/${other.token}`), 401);
  assert.equal(await asMine.status("POST", "/ack", { upTo: Number.MAX_SAFE_INTEGER, hook: other.token }), 401);
  assert.equal(await asMine.status("DELETE", `/hooks/${other.token}`), 401);
  assert.equal(await asMine.status("GET", `/hooks/${mine.token}`), 200);
  const shown = (await (await fetch(`${target.base}/hooks/${mine.token}`, { headers: { authorization: `Bearer ${mine.readKey}` } })).json()) as Record<string, unknown>;
  assert.equal(shown.token, mine.token);
  assert.equal("readKeyHash" in shown, false);
  const listed = (await (await fetch(`${target.base}/hooks`, { headers: { authorization: `Bearer ${target.key}` } })).json()) as { hooks: Record<string, unknown>[] };
  assert.equal(listed.hooks.some((h) => "readKeyHash" in h), false);

  assert.equal(await asMine.ack(got[0]!.seq, mine.token), 1);
  assert.equal(await asMine.status("DELETE", `/hooks/${mine.token}`), 200);
  assert.equal(await asMine.status("GET", `/hooks/${mine.token}`), 404, "a hook that is gone is not found, so the reader makes another");
  assert.equal(await reader(target, "").status("GET", `/hooks/${other.token}`), 401, "without a key");
  assert.equal((await mailbox.deliveries(tip, `&hook=${other.token}`)).length, 1, "the other hook is untouched");
  await mailbox.deleteHook(other.token);
});

test("with open hooks anyone can create a hook, which expires, up to the cap that the admin key passes", async (t) => {
  if (target.remote) return t.skip("needs open hooks and a small cap, set only on the in-memory mailbox");
  const open = await startMailbox({ openHooks: true, maxHooks: 2 });
  try {
    const anyone = reader(open, "");
    const first = await anyone.createHook("first");
    assert.equal(first.idleSeconds, 7 * 24 * 3600, "a hook made without the admin key always expires");
    assert.equal((await anyone.createHook("second", 60)).idleSeconds, 60);
    assert.equal(await anyone.status("POST", "/hooks", {}), 503);
    assert.equal((await reader(open).createHook("admin")).idleSeconds, undefined, "the admin key passes the cap");

    assert.equal(await anyone.status("GET", "/hooks"), 401);
    assert.equal(await anyone.status("GET", `/deliveries?hook=${first.token}`), 401);
    assert.equal(await reader(open, first.readKey).status("GET", "/hooks"), 401);
    await post(first.url, "x");
    assert.equal((await reader(open, first.readKey).deliveries(0, `&hook=${first.token}`)).length, 1);
  } finally {
    await open.close();
  }
  const closed = await startMailbox({ key: null });
  try {
    assert.equal(await reader(closed, "").status("POST", "/hooks", {}), 503, "no admin key and no open hooks");
  } finally {
    await closed.close();
  }
});

const failing = (err: Error) => {
  const opened = { count: 0 };
  const fail = async (): Promise<never> => {
    throw err;
  };
  return { opened, open: () => (opened.count++, { createHook: fail, deleteHook: fail, getHook: fail, listHooks: fail, insert: fail, list: fail, ack: fail }) };
};

test("a store that throws answers 503, and names the reason only to a caller with the key", async (t) => {
  const logged = t.mock.method(console, "error", () => {});
  const key = "k".repeat(43);
  const store = failing(new Error("storage is down")).open();
  const fetched = await handle(new Request("http://mailbox.test/deliveries", { headers: { authorization: `Bearer ${key}` } }), store, { key });
  assert.equal(fetched.status, 503);
  assert.equal(fetched.headers.get("retry-after"), "1");
  assert.deepEqual(await fetched.json(), { error: "mailbox unavailable: storage is down" });
  const posted = await handle(new Request(`http://mailbox.test/hooks/${unknownToken}`, { method: "POST", body: JSON.stringify({ eventId: "1" }) }), store, { key });
  assert.equal(posted.status, 503);
  assert.deepEqual(await posted.json(), { error: "mailbox unavailable" });
  assert.deepEqual(logged.mock.calls.map((call) => call.arguments[0]), ["GET /deliveries: storage is down", "POST /hooks/:token: storage is down"]);
});

test("a read that fails with a retryable error is made once more on a new store", async () => {
  const retryable = Object.assign(new Error("reset"), { retryable: true });
  const once = failing(retryable);
  const good = new MemoryStore();
  const store = retrying(() => (once.opened.count === 0 ? once.open() : (once.opened.count++, good)), 0);
  assert.deepEqual(await store.listHooks(), []);
  assert.equal(once.opened.count, 2);

  const always = failing(retryable);
  await assert.rejects(retrying(always.open, 0).listHooks(), /reset/);
  assert.equal(always.opened.count, 2);
});

test("a write, an overloaded store and an error not marked retryable are not tried again", async () => {
  const write = failing(Object.assign(new Error("reset"), { retryable: true }));
  await assert.rejects(retrying(write.open, 0).insert(unknownToken, 0, {}, new Uint8Array()), /reset/);
  await assert.rejects(retrying(write.open, 0).createHook(unknownToken, "", 0, undefined, ""), /reset/);
  await assert.rejects(retrying(write.open, 0).deleteHook(unknownToken), /reset/);
  assert.equal(write.opened.count, 3);

  const overloaded = failing(Object.assign(new Error("overloaded"), { retryable: true, overloaded: true }));
  await assert.rejects(retrying(overloaded.open, 0).listHooks(), /overloaded/);
  assert.equal(overloaded.opened.count, 1);

  const plain = failing(new Error("broken"));
  await assert.rejects(retrying(plain.open, 0).ack(1), /broken/);
  assert.equal(plain.opened.count, 1);
});
