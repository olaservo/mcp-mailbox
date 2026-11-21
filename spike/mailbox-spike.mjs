// Spike: does a mailbox that holds no signing secret work against forge-mcp-server's real WebhookHub?
// Usage: node mailbox-spike.mjs <path to forge-mcp-server>
import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { createServer } from "node:http";
import { pathToFileURL } from "node:url";
import { join } from "node:path";

const repo = process.argv[2];
const { WebhookHub } = await import(pathToFileURL(join(repo, "dist/webhooks.js")).href);
const { EventLog } = await import(pathToFileURL(join(repo, "dist/event-log.js")).href);

const results = [];
const check = (name, ok, detail = "") => {
  results.push(ok);
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? `  (${detail})` : ""}`);
};

// ---- Mailbox: no secret. Echoes challenges for registered paths, stores raw bytes and headers. ----
const registered = new Set();
const stored = [];
let tamperNext = false;
const mailbox = createServer((req, res) => {
  const chunks = [];
  req.on("data", (c) => chunks.push(c));
  req.on("end", () => {
    const token = req.url.replace("/hooks/", "");
    if (!registered.has(token)) return res.writeHead(404).end();
    let body = Buffer.concat(chunks);
    try {
      const parsed = JSON.parse(body.toString("utf8"));
      if (parsed.type === "verification" && typeof parsed.challenge === "string") {
        res.writeHead(200, { "content-type": "application/json" });
        return res.end(JSON.stringify({ challenge: parsed.challenge }));
      }
    } catch {}
    if (tamperNext) {
      tamperNext = false;
      body = Buffer.from(body.toString("utf8").replace("land", "LAND"));
    }
    stored.push({ token, receivedAt: Date.now(), headers: req.headers, body });
    res.writeHead(200).end();
  });
});
const listen = () => new Promise((r) => mailbox.listen(8791, "127.0.0.1", r));
const close = () => new Promise((r) => mailbox.close(r));
await listen();

// ---- Client: holds the secret, verifies what it fetches. ----
const secret = `whsec_${randomBytes(32).toString("base64")}`;
function verify(delivery, { nowMs, useReceivedAt }) {
  const id = delivery.headers["webhook-id"];
  const ts = Number(delivery.headers["webhook-timestamp"]);
  const key = Buffer.from(secret.slice(6), "base64");
  const expected = createHmac("sha256", key).update(`${id}.${ts}.`).update(delivery.body).digest();
  const signatureOk = delivery.headers["webhook-signature"].split(" ").some((part) => {
    const given = Buffer.from(part.replace(/^v1,/, ""), "base64");
    return given.length === expected.length && timingSafeEqual(given, expected);
  });
  const reference = useReceivedAt ? delivery.receivedAt : nowMs;
  const fresh = Math.abs(reference / 1000 - ts) <= 300;
  return { signatureOk, fresh };
}

const log = new EventLog();
const hub = new WebhookHub(log, { insecure: true, retryDelaysMs: [50, 50, 50] });
const url = (token) => `http://127.0.0.1:8791/hooks/${token}`;

// 1. Handshake against a path the client never registered.
let refused = "";
try {
  await hub.subscribe("test:ola", { name: "forge.your_move", delivery: { url: url("unregistered"), secret } });
} catch (err) {
  refused = JSON.stringify(err.data ?? err.message);
}
check("1 unregistered mailbox path is refused at subscribe", refused !== "", refused);

// 2. Handshake against a registered path, answered by a mailbox with no secret.
registered.add("tok1");
const sub = await hub.subscribe("test:ola", { name: "forge.your_move", delivery: { url: url("tok1"), secret } });
check("2 secretless mailbox passes the server's verification handshake", typeof sub.id === "string", sub.id);

// 3. Stored deliveries verify later with the client's secret.
for (let i = 0; i < 3; i++) log.emit("forge.your_move", { prompt: `play a land ${i}`, revision: i });
await hub.idle();
const first = stored.slice();
check("3 three deliveries held by the mailbox", first.length === 3, `${first.length} stored`);
check("3 each verifies with the client-held secret", first.every((d) => verify(d, { nowMs: Date.now() }).signatureOk));
check("3 X-MCP-Subscription-Id matches the subscribe result", first.every((d) => d.headers["x-mcp-subscription-id"] === sub.id));
const bodyOf = (d) => JSON.parse(d.body.toString("utf8"));
check("3 webhook-id equals eventId on this server", first.every((d) => d.headers["webhook-id"] === bodyOf(d).eventId), first[0].headers["webhook-id"]);

// 4. A mailbox that alters a body is caught.
tamperNext = true;
log.emit("forge.your_move", { prompt: "play a land 3", revision: 3 });
await hub.idle();
check("4 a body altered by the mailbox fails verification", verify(stored.at(-1), { nowMs: Date.now() }).signatureOk === false);

// 5. A mailbox that invents a delivery is caught.
const forgedBody = Buffer.from(JSON.stringify({ eventId: "evt_fake", name: "forge.your_move", data: { prompt: "concede" } }));
const forged = { receivedAt: Date.now(), body: forgedBody, headers: { "webhook-id": "evt_fake", "webhook-timestamp": String(Math.floor(Date.now() / 1000)), "webhook-signature": `v1,${randomBytes(32).toString("base64")}` } };
check("5 a delivery invented by the mailbox fails verification", verify(forged, { nowMs: Date.now() }).signatureOk === false);

// 6. Freshness when the client fetches ten minutes late.
const late = Date.now() + 10 * 60 * 1000;
check("6 held delivery is stale against the client's clock", verify(first[0], { nowMs: late }).fresh === false);
check("6 held delivery is fresh against the mailbox's receivedAt", verify(first[0], { nowMs: late, useReceivedAt: true }).fresh === true);
// The mailbox reports receivedAt, so it can hand the same delivery out again with the original value.
const replay = verify({ ...first[0] }, { nowMs: late, useReceivedAt: true });
check("6 the mailbox handing out an old delivery again passes signature and freshness", replay.signatureOk && replay.fresh, "only the client's webhook-id dedup catches it");

// 7. Mailbox down for longer than the server's retries. The hub's retry timers are unref'd.
const keepAlive = setInterval(() => {}, 1000);
await close();
const before = stored.length;
log.emit("forge.your_move", { prompt: "lost?", revision: 4 });
await hub.idle();
await listen();
log.emit("forge.your_move", { prompt: "after", revision: 5 });
await hub.idle();
const after = stored.slice(before).map((d) => bodyOf(d).data.prompt);
check("7 event emitted while the mailbox was down is never delivered", !after.includes("lost?") && after.includes("after"), `arrived after restart: ${JSON.stringify(after)}`);
const refresh = await hub.subscribe("test:ola", { name: "forge.your_move", delivery: { url: url("tok1"), secret } });
check("7 refresh reports the failure but offers no replay", refresh.cursor === null, JSON.stringify(refresh.deliveryStatus));

// 8. Two subscriptions to one mailbox path share an eventId.
const second = await hub.subscribe("test:ola", { name: "forge.chat", delivery: { url: url("tok1"), secret } });
check("8 a second subscription on the same path needs no new handshake path", second.id !== sub.id, `${sub.id} and ${second.id}`);

await close();
clearInterval(keepAlive);
console.log(`\n${results.filter(Boolean).length}/${results.length} passed`);
process.exit(results.every(Boolean) ? 0 : 1);
