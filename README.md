# mcp-mailbox

A mailbox for MCP Events webhook delivery (SEP-3415). An MCP server POSTs signed events to the mailbox. A client with no public address fetches them from the mailbox and verifies them with a secret the mailbox never holds.

Status: the mailbox is built and deployed at `https://mcp-mailbox.olahungerford.workers.dev` (2026-11-21). The reader is built and tested against forge-mcp-server. No game has run through it. The encryption experiment is not started.

## Parts

- Mailbox (built): a public `https` endpoint that answers the verification challenge for registered paths and stores each delivery as raw bytes and headers. It runs as a Cloudflare Worker with one SQLite Durable Object. `src/core.ts` holds the HTTP surface and does not depend on Workers.
- Reader (built): a Node process that subscribes with a mailbox hook as the callback, fetches, verifies, deduplicates, orders and hands each event to a command. `src/reader/`. It has no dependencies.
- Encryption (not started): the server encrypts the payload to a key the client supplies, so the mailbox cannot read events.

## What the mailbox can and cannot do

It has no signing secret, so it cannot forge or alter a delivery without the reader's verification failing. It can read events, drop them, and hand an old one out again. The reader therefore has to:

- verify `webhook-signature` over the stored bytes with its own secret;
- check `webhook-timestamp` against the delivery's `receivedAt`, since a held delivery is older than 5 minutes by the reader's clock;
- deduplicate on `x-mcp-subscription-id` plus `webhook-id`;
- order events by their own content. `seq` is arrival order. Observed against the deployed mailbox: forge-mcp-server POSTs events concurrently, and four events emitted in order arrived in another order.

A server that does not replay over webhook loses any event it emits while the mailbox is down past its retries.

## API

Reader routes take `Authorization: Bearer <MAILBOX_KEY>`.

| Route | Who | What |
|---|---|---|
| `POST /hooks` with optional `{"label"}` | reader | Creates a callback path. Returns `{token, url, label}`. Use `url` as `delivery.url` in `events/subscribe`. |
| `GET /hooks` | reader | Lists callback paths. |
| `DELETE /hooks/<token>` | reader | Removes a path. Later POSTs to it get 404. |
| `POST /hooks/<token>` | MCP server | A verification envelope is echoed. Anything else is stored. 404 for an unknown path, 413 over 256 KiB, 503 when 10,000 deliveries are held. |
| `GET /deliveries?after=<seq>&limit=<n>&wait=<seconds>&hook=<token>` | reader | Deliveries after `seq`, oldest first, as `{seq, hook, receivedAt, headers, body}` with `body` in base64. `wait` (up to 25) holds the request until one arrives. `hook` limits the answer to one callback path. |
| `POST /ack` with `{"upTo": <seq>, "hook": <token>}` | reader | Deletes deliveries up to `seq`, for one callback path when `hook` is given. |

Deliveries not acked are dropped after 7 days.

## Reader

```bash
node --env-file=<file> src/reader/cli.ts            # run until stopped
node --env-file=<file> src/reader/cli.ts unsubscribe  # unsubscribe and remove the hook
```

| Variable | What |
|---|---|
| `MAILBOX_URL`, `MAILBOX_KEY` | The mailbox and the reader's key. |
| `READER_SERVER_URL` | The MCP server's Streamable HTTP endpoint, such as `https://host/mcp`. |
| `READER_SERVER_TOKEN` | Its bearer token. For forge-mcp-server this is `FORGE_MCP_TOKEN`. |
| `READER_EVENTS` | Event names, comma separated, such as `forge.your_move,forge.chat`. |
| `READER_EXEC` | A command run once per event through the shell, with the event as JSON on stdin and `MCP_EVENT_NAME`, `MCP_EVENT_ID` and `MCP_SUBSCRIPTION_ID` set. Without it, each event is one JSON line on stdout. |
| `READER_STATE` | State file, default `~/.mcp-mailbox/reader-state.json`. It holds the signing secret. |
| `READER_LABEL` | The hook's label in the mailbox. |

What it does:

- Creates one hook in the mailbox and a `whsec_` secret on first run, and keeps both in the state file.
- Sends `events/subscribe` for each event as a single 2026-07-28 request with no session, and again every 5 minutes. The repeat is the refresh, and it restores a subscription the server lost in a restart.
- Fetches with `wait=25` and `hook=<its token>`, so several readers can share one mailbox. After a fetch returns deliveries it fetches again 500 ms later, so events POSTed together are ordered together.
- Refuses a delivery whose signature does not verify, whose timestamp is more than 5 minutes from `receivedAt`, whose `receivedAt` is over 8 days old or in the future, whose subscription id it does not hold, whose event name is not the subscription's, or that it has handled before. The subscription id header is not signed and the event name is, which is the reason for the name check.
- Orders a batch by the number at the end of `eventId` when the ids share a prefix, otherwise by `timestamp`, then by arrival.
- Hands events on one at a time, saves its position, then acks. A reader stopped between handing on and saving hands the batch on again at the next start.

Limits:

- A handler that fails is logged and the event is not tried again.
- Ordering holds within a batch. An event that arrives after its batch was handed on is handed on late.
- It does not detect a dropped event. forge-mcp-server sends `cursor: null` over webhook.
- The secret is not rotated.
- `gap` and `terminated` envelopes are logged and nothing else is done.

## Develop

Node 24 or later. The tests run TypeScript directly with `node --test`.

```bash
npm install
npm test
npm run typecheck
```

`npm test` starts an in-memory mailbox. To run the same tests against the Worker, copy `.dev.vars.example` to `.dev.vars`, set a key, run `npx wrangler dev --port 8917`, and then:

```bash
MAILBOX_URL=http://127.0.0.1:8917 MAILBOX_KEY=<the key> npm test
```

`FORGE_MCP_SERVER=<path to a built forge-mcp-server>` adds a test that runs that server's `WebhookHub` through the mailbox.

## Deploy

```bash
npx wrangler deploy
npx wrangler secret put MAILBOX_KEY
```

The key is at least 32 characters. Reader routes answer 503 without it.

## Tested

2026-11-21, with forge-mcp-server's `events-push` branch at `692526b`:

- 10 of 10 tests pass against the in-memory mailbox.
- 9 pass against the Worker under `wrangler dev` (workerd, SQLite Durable Object). The tenth sets a storage limit and only runs in memory.
- The same 9 pass against the deployed Worker.
- In all three, forge-mcp-server's hub passes its verification handshake against the mailbox, and four deliveries from two subscriptions verify with the reader's secret. Against the deployed Worker the hub ran with `insecure` off, so its `https` and public-address checks applied.

Reader, 2026-11-21, with forge-mcp-server checked out at `c2fa1e4` and its `dist` as built that day:

- 9 reader tests pass against the in-memory mailbox and against the Worker under `wrangler dev`, with a stand-in MCP server.
- With `FORGE_MCP_SERVER` set, the reader subscribes to forge-mcp-server's HTTP endpoint with a bearer token, and four events emitted in order are handed on in order. Passes in memory, under `wrangler dev` and against the deployed Worker.
- The CLI was run once against the in-memory mailbox and forge-mcp-server, printing events and with `READER_EXEC`.
- After the Worker was deployed with `hook` on `/deliveries` and `/ack` (version `ba05efcf`), all 20 tests that apply pass against it, including two readers on one mailbox.

Not tested: a real game through the mailbox, a reader left running for hours, and what a held `wait` request costs in Durable Object duration.

`spike/mailbox-spike.mjs` is the first check of the idea, run before the mailbox was written. It also shows the cases a reader has to handle: a mailbox that alters, invents or repeats a delivery, and a mailbox that is down.
