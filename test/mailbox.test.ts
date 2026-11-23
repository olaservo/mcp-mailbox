import assert from "node:assert/strict";
import { after, before, test } from "node:test";
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
  for (const [method, path] of [["POST", "/hooks"], ["GET", "/hooks"], ["GET", "/deliveries"], ["POST", "/ack"], ["DELETE", `/hooks/${unknownToken}`]] as const) {
    const none = await fetch(`${target.base}${path}`, { method });
    assert.equal(none.status, 401, `${method} ${path} without a key`);
    const wrong = await fetch(`${target.base}${path}`, { method, headers: { authorization: `Bearer ${"x".repeat(43)}` } });
    assert.equal(wrong.status, 401, `${method} ${path} with a wrong key`);
  }
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

  const got = await mailbox.deliveries(tip);
  assert.equal(got.length, 2);
  assert.deepEqual(Buffer.from(got[0]!.body, "base64"), first);
  assert.deepEqual(Buffer.from(got[1]!.body, "base64"), second);
  const { "x-other": _dropped, ...kept } = headers;
  assert.deepEqual(got[0]!.headers, kept);
  assert.equal(got[0]!.hook, hook.token);
  assert.ok(got[1]!.seq > got[0]!.seq);
  assert.ok(Math.abs(got[0]!.receivedAt - sent) < 5000, "receivedAt is the arrival time");

  assert.equal((await mailbox.deliveries(got[0]!.seq)).length, 1, "after skips what was read");
  assert.equal((await mailbox.deliveries(tip, "&limit=1")).length, 1);
  assert.ok((await mailbox.ack(got[1]!.seq)) >= 2);
  assert.deepEqual(await mailbox.deliveries(0), []);
});

test("a body over 256 KiB is refused with 413", async () => {
  const hook = await mailbox.createHook("large");
  assert.equal((await post(hook.url, Buffer.alloc(256 * 1024 + 1, "a"))).status, 413);
  assert.equal((await post(hook.url, Buffer.alloc(256 * 1024, "a"))).status, 200);
  await mailbox.ack(await mailbox.tip());
});

test("a waiting fetch returns when a delivery arrives", async () => {
  const hook = await mailbox.createHook("wait");
  const tip = await mailbox.tip();
  const started = Date.now();
  const waiting = mailbox.deliveries(tip, "&wait=10");
  setTimeout(() => void post(hook.url, '{"eventId":"late"}'), 300);
  const got = await waiting;
  assert.equal(got.length, 1);
  assert.ok(Date.now() - started < 5000, `returned after ${Date.now() - started} ms`);
  await mailbox.ack(got[0]!.seq);
});

test("a waiting fetch returns empty when nothing arrives", async () => {
  const tip = await mailbox.tip();
  const started = Date.now();
  assert.deepEqual(await mailbox.deliveries(tip, "&wait=1"), []);
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
  const left = await mailbox.deliveries(tip);
  assert.deepEqual(left.map((d) => d.hook), [two.token]);
  assert.equal((await fetch(`${target.base}/deliveries?hook=short`, { headers: { authorization: `Bearer ${target.key}` } })).status, 400);
  await mailbox.ack(left[0]!.seq);
});
