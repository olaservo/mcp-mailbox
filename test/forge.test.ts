import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { pathToFileURL } from "node:url";
import { Reader, type Received } from "../src/reader/reader.ts";
import { reader as mailboxClient, startMailbox, TEST_HOOK_IDLE_SECONDS } from "./harness.ts";

// Runs the reader against forge-mcp-server's HTTP endpoint and webhook hub. Set FORGE_MCP_SERVER to a built clone.
const repo = process.env.FORGE_MCP_SERVER;
const skip = repo ? false : "FORGE_MCP_SERVER is not set";
const load = (file: string) => import(pathToFileURL(join(repo!, "dist", file)).href);

for (const encrypt of [false, true]) {
  test(`the reader subscribes to forge-mcp-server over HTTP and hands on its events in order${encrypt ? ", encrypted" : ""}`, { skip }, async (t) => {
    const [{ WebhookHub, ENCRYPTION_SCHEME }, { EventLog }, { listenHttp }, { Bridge }, { skillCatalog }] = await Promise.all(["webhooks.js", "event-log.js", "transports/http.js", "bridge.js", "server.js"].map(load));
    if (encrypt && !ENCRYPTION_SCHEME) return t.skip("this build of forge-mcp-server does not encrypt");
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
      encrypt,
      stateFile: join(mkdtempSync(join(tmpdir(), "reader-forge-")), "state.json"),
      label: "forge test",
      hookIdleSeconds: TEST_HOOK_IDLE_SECONDS,
      handle: (received) => void got.push(received),
      log: (line) => lines.push(line),
    });
    try {
      await reader.subscribe();
      assert.equal(hub.size, 2);
      const sent: { eventId: string; data: unknown }[] = [
        log.emit("forge.your_move", { kind: "priority", prompt: "play a land" }),
        log.emit("forge.chat", { text: "good luck" }),
        log.emit("forge.your_move", { kind: "decision", prompt: "choose a target" }),
        log.emit("forge.your_move", { kind: "game_over", prompt: "Game over" }),
      ];
      await hub.idle();
      const held = await mailboxClient(target).deliveries(0, `&limit=500&hook=${reader.hookUrl!.split("/").at(-1)}`);
      assert.equal(held.some((delivery) => Buffer.from(delivery.body, "base64").includes("play a land")), !encrypt, "what the mailbox holds");
      assert.equal(await reader.poll(5), 4);
      assert.deepEqual(got.map((r) => r.event.eventId), sent.map((e) => e.eventId));
      assert.deepEqual(got.map((r) => r.event.data), sent.map((e) => e.data));
      assert.deepEqual(got.map((r) => r.event.name), ["forge.your_move", "forge.chat", "forge.your_move", "forge.your_move"]);

      // A refresh keeps the subscriptions. Nothing was skipped and the server reports no failed delivery.
      await reader.subscribe();
      assert.equal(hub.size, 2);
      assert.deepEqual(lines.filter((line) => !line.startsWith("subscribed")), []);
      await reader.unsubscribe();
      assert.equal(hub.size, 0);
    } finally {
      server.close();
      await target.close();
    }
  });
}
