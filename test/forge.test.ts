import assert from "node:assert/strict";
import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { pathToFileURL } from "node:url";
import { Reader, type Received } from "../src/reader/reader.ts";
import { reader, startMailbox, type Fetched } from "./harness.ts";

// Runs forge-mcp-server's WebhookHub against the mailbox. Set FORGE_MCP_SERVER to a built clone.
const repo = process.env.FORGE_MCP_SERVER;

function signatureOk(secret: string, delivery: Fetched): boolean {
  const key = Buffer.from(secret.slice(6), "base64");
  const signed = `${delivery.headers["webhook-id"]}.${delivery.headers["webhook-timestamp"]}.`;
  const expected = createHmac("sha256", key).update(signed).update(Buffer.from(delivery.body, "base64")).digest();
  return delivery.headers["webhook-signature"]!.split(" ").some((part) => {
    const given = Buffer.from(part.replace(/^v1,/, ""), "base64");
    return given.length === expected.length && timingSafeEqual(given, expected);
  });
}

test("forge-mcp-server delivers through the mailbox and the reader verifies", { skip: repo ? false : "FORGE_MCP_SERVER is not set" }, async () => {
  const { WebhookHub } = await import(pathToFileURL(join(repo!, "dist/webhooks.js")).href);
  const { EventLog } = await import(pathToFileURL(join(repo!, "dist/event-log.js")).href);
  const target = await startMailbox();
  try {
    const mailbox = reader(target);
    const hook = await mailbox.createHook("forge");
    const tip = await mailbox.tip();
    const secret = `whsec_${randomBytes(32).toString("base64")}`;
    const log = new EventLog();
    // Against an https mailbox the hub keeps its checks: https only, public addresses only.
    const hub = new WebhookHub(log, { insecure: !hook.url.startsWith("https:"), retryDelaysMs: [50, 50, 50] });

    // The handshake is answered by a mailbox that does not have the secret.
    const move = await hub.subscribe("test:reader", { name: "forge.your_move", delivery: { url: hook.url, secret } });
    const chat = await hub.subscribe("test:reader", { name: "forge.chat", delivery: { url: hook.url, secret } });
    assert.notEqual(move.id, chat.id);

    for (let i = 0; i < 3; i++) log.emit("forge.your_move", { prompt: `play a land ${i}`, revision: i });
    log.emit("forge.chat", { text: "good luck" });
    await hub.idle();

    const got = await mailbox.deliveries(tip);
    assert.equal(got.length, 4);
    for (const delivery of got) {
      assert.ok(signatureOk(secret, delivery), "verifies with the reader's secret");
      assert.ok(Math.abs(delivery.receivedAt / 1000 - Number(delivery.headers["webhook-timestamp"])) <= 300, "fresh against receivedAt");
    }
    // The hub POSTs events concurrently, so over a network they arrive in any order.
    assert.deepEqual(got.map((d) => d.headers["x-mcp-subscription-id"]).sort(), [move.id, move.id, move.id, chat.id].sort());
    assert.equal(new Set(got.map((d) => `${d.headers["x-mcp-subscription-id"]} ${d.headers["webhook-id"]}`)).size, 4, "subscription id and webhook-id make a dedup key");
    const prompts = got.filter((d) => d.headers["x-mcp-subscription-id"] === move.id).map((d) => JSON.parse(Buffer.from(d.body, "base64").toString("utf8")).data.prompt);
    assert.deepEqual(prompts.sort(), ["play a land 0", "play a land 1", "play a land 2"]);

    assert.ok(!signatureOk(`whsec_${randomBytes(32).toString("base64")}`, got[0]!), "a different secret does not verify");
    await mailbox.ack(got.at(-1)!.seq);
  } finally {
    await target.close();
  }
});

test("the reader subscribes to forge-mcp-server over HTTP and hands on its events in order", { skip: repo ? false : "FORGE_MCP_SERVER is not set" }, async () => {
  const load = (file: string) => import(pathToFileURL(join(repo!, "dist", file)).href);
  const [{ WebhookHub }, { EventLog }, { listenHttp }, { Bridge }, { skillCatalog }] = await Promise.all(["webhooks.js", "event-log.js", "transports/http.js", "bridge.js", "server.js"].map(load));
  const target = await startMailbox();
  const log = new EventLog();
  const hub = new WebhookHub(log, { insecure: !target.base.startsWith("https:"), retryDelaysMs: [50, 50, 50] });
  const bridge = new Bridge({ baseUrl: "http://bridge.test", fetch: async () => new Response("{}", { status: 400 }) });
  const server = listenHttp({ bridge, skills: await skillCatalog(), events: log, port: 0, token: "s3cret-for-the-reader", webhooks: hub });
  assert.equal(await server.listening, true);
  const got: Received[] = [];
  const lines: string[] = [];
  const reader = new Reader({
    mailbox: { url: target.base, key: target.key },
    server: { url: `http://127.0.0.1:${server.port()}/mcp`, token: "s3cret-for-the-reader" },
    events: ["forge.your_move", "forge.chat"],
    stateFile: join(mkdtempSync(join(tmpdir(), "reader-forge-")), "state.json"),
    label: "forge test",
    handle: (received) => void got.push(received),
    log: (line) => lines.push(line),
  });
  try {
    await reader.subscribe();
    assert.equal(hub.size, 2);
    const sent = [
      log.emit("forge.your_move", { kind: "priority", prompt: "play a land" }),
      log.emit("forge.chat", { text: "good luck" }),
      log.emit("forge.your_move", { kind: "decision", prompt: "choose a target" }),
      log.emit("forge.your_move", { kind: "game_over", prompt: "Game over" }),
    ];
    await hub.idle();
    assert.equal(await reader.poll(5), 4);
    assert.deepEqual(got.map((r) => r.event.eventId), sent.map((e: { eventId: string }) => e.eventId));
    assert.deepEqual(got.map((r) => r.event.name), ["forge.your_move", "forge.chat", "forge.your_move", "forge.your_move"]);
    assert.deepEqual(lines.filter((line) => line.startsWith("refused")), []);

    // A refresh keeps the subscriptions and reports no failed delivery.
    await reader.subscribe();
    assert.equal(hub.size, 2);
    await reader.unsubscribe();
    assert.equal(hub.size, 0);
  } finally {
    server.close();
    await target.close();
  }
});
