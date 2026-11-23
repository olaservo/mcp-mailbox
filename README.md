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
| `POST /hooks` with optional `{"label", "idleSeconds"}` | reader | Creates a callback path. Returns `{token, url, label}`, and `idleSeconds` when given. Use `url` as `delivery.url` in `events/subscribe`. With `idleSeconds` (60 to 604800), a path not fetched with `hook=<token>` for that long is removed with its deliveries. Without it the path is kept. |
| `GET /hooks` | reader | Lists callback paths with `createdAt`, `lastReadAt` and `idleSeconds`. |
| `DELETE /hooks/<token>` | reader | Removes a path and its deliveries. Later POSTs to it get 404. |
| `POST /hooks/<token>` | MCP server | A verification envelope is echoed. Anything else is stored. 404 for an unknown path, 413 over 256 KiB, 503 when 10,000 deliveries are held in all, or 2,000 for this path. |
| `GET /deliveries?after=<seq>&limit=<n>&wait=<seconds>&hook=<token>` | reader | Deliveries after `seq`, oldest first, as `{seq, hook, receivedAt, headers, body}` with `body` in base64. `wait` (up to 25) holds the request until one arrives. `hook` limits the answer to one callback path, and a waiting fetch with `hook` returns only for that path's deliveries. |
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
| `READER_SERVERS_DIR` | A directory where servers announce themselves. Takes the place of `READER_SERVER_URL`, or adds to it. See below. |
| `READER_EVENTS` | Event names, comma separated, such as `forge.your_move,forge.chat`. |
| `READER_EXEC` | A command run once per event through the shell, with the event as JSON on stdin and `MCP_EVENT_NAME`, `MCP_EVENT_ID` and `MCP_SUBSCRIPTION_ID` set. Without it, each event is one JSON line on stdout. |
| `READER_POLL_PORT` | Serves the events as MCP Events poll delivery on `127.0.0.1:<port>`. Takes the place of `READER_EXEC`. See below. |
| `READER_STATE` | State file, default `~/.mcp-mailbox/reader-state.json`. It holds the signing secret. |
| `READER_LABEL` | The hook's label in the mailbox. |

What it does:

- Creates one hook in the mailbox and a `whsec_` secret on first run, and keeps both in the state file.
- Sends `events/subscribe` for each event as a single 2026-07-28 request with no session. It repeats this every minute, or at half the granted lifetime when that is sooner. The repeat is the refresh, and it restores a subscription the server lost in a restart.
- Keeps the cursor each `events/subscribe` returns and sends it with the next one, so a server that replays from a cursor resends what a lapsed subscription missed. A repeat is dropped by the check below.
- Fetches with `wait=25` and `hook=<its token>`, so several readers can share one mailbox. After a fetch returns deliveries it fetches again 500 ms later, so events POSTed together are ordered together.
- Skips a delivery, with a log line, when:
  - the signature does not verify;
  - the timestamp is more than 5 minutes from `receivedAt`;
  - `receivedAt` is over 8 days old or in the future;
  - it does not hold the subscription id;
  - the event name is not the subscription's (the subscription id header is not signed, the name is);
  - it has handled the delivery before.
- Orders a batch by the number at the end of `eventId` when the ids share a prefix, otherwise by `timestamp`, then by arrival.
- Hands events on one at a time, saves its position, then acks. A reader stopped between handing on and saving hands the batch on again at the next start.

### Several servers

With `READER_SERVERS_DIR` set, the reader subscribes to every server that has a file in that directory. One hook and one poll endpoint serve them all.

- A server writes one `*.json` file with `url`, and `token` when its bearer token is not `READER_SERVER_TOKEN`. It removes the file when it exits. A file being written needs a name that does not end in `.json`.
- The directory is read every 5 seconds. A new file is subscribed at the next read. A server whose file is gone is no longer refreshed, and it is not sent `events/unsubscribe`.
- Servers are told apart by `url`, so one named in a file and in `READER_SERVER_URL` is subscribed once.
- A server that cannot be reached is tried again every 15 seconds, and the failure is logged once until it changes.
- The reader sends its token, or the file's, to whatever `url` a file names. Keep the directory writable by the user only.
- Events from every server are in one log on the poll endpoint. A client that wants one server's events picks them by event id, as the `mcp-events` mod does with `ownOnly`.

### Poll endpoint

With `READER_POLL_PORT` set, the reader answers `events/list` and `events/poll` as JSON-RPC POSTs on that port, on any path. A client that polls and has no webhook support can then follow a server through the mailbox. For the `mcp-events` Claude Code mod in forge-mcp-server, set its `url` option to `http://127.0.0.1:<port>/mcp`.

- Events are kept in memory, the newest 1000. A poll with no cursor starts from now. A cursor from before a restart of the reader polls as `truncated`.
- On either of those polls the reader also sends each server a poll with no cursor for that event, and returns what they answer. A server that keeps an ask waiting can give it to a client that starts late this way, as forge-mcp-server does from `5933c34`. The answers are not verified by signature, since they come from the server itself and not through the mailbox.
- `maxAgeMs` and `maxEvents` are honoured. `nextPollMs` is 1000.
- There is no authentication. It listens on loopback and refuses a request with an `Origin` header.
- The client's tool calls still go to the MCP server itself. Only events take this path.

Limits:

- A handler that fails is logged and the event is not tried again.
- Ordering holds within a batch. An event that arrives after its batch was handed on is handed on late.
- Events a server emits after a restart and before the next refresh are not delivered, for up to a minute. A cursor from before the restart does not bring them back on forge-mcp-server, whose cursors are per process. forge-mcp-server sends a new subscription the ask that is still pending (`76c7b3f`), and can keep subscriptions across restarts in a file (`FORGE_MCP_SUBSCRIPTIONS`, not tried with the reader).
- A gap the server reports (`truncated`) is logged and not passed on to the handler or the poll endpoint.
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

- The reader tests pass against the in-memory mailbox and against the Worker under `wrangler dev`, with a stand-in MCP server.
- With `FORGE_MCP_SERVER` set, the reader subscribes to forge-mcp-server's HTTP endpoint with a bearer token, and four events emitted in order are handed on in order. Passes in memory, under `wrangler dev` and against the deployed Worker.
- The CLI was run once against the in-memory mailbox and forge-mcp-server, printing events and with `READER_EXEC`.
- The CLI was run once with `READER_POLL_PORT` against the same two. Three events emitted by forge-mcp-server were read by a client polling once a second with the mod's request, 1.1 s after they were emitted. The mod's `register.ts` (forge-mcp-server `c2fa1e4`) was then run in Node against it, with a stand-in for the engine that does real HTTP and timers: an event between turns was submitted as a prompt once, and three events during a turn were held and the newest of each name submitted when the turn ended. Not run: the mod in a Claude Code session.
- After the Worker was deployed with `hook` on `/deliveries` and `/ack` (version `ba05efcf`), every test that applies passes against it, including two readers on one mailbox.

Game, 2026-11-22, forge-mcp-server `main` with the `mcp-events` mod polling the reader's endpoint, through the deployed mailbox. Counts are from the server's activity log as read by the session that ran the game, not counted line by line:

- A Claude Code seat played 23 game turns between 18:18Z and 18:58Z. The server emitted 153 `forge.your_move` and 3 `forge.chat`. Every delivery line read was 200 on the first attempt, in 50 to 170 ms. The mod started about 45 turns and reported no missed events.
- Before the game the server restarted at about 17:25Z. The reader, then refreshing every 5 minutes, subscribed again at 17:29:35Z. The four events emitted in between (`3c794fbd-1` to `-4`, the last one the coin-toss ask) were never delivered, and the seat waited on the coin toss until a person asked. The refresh is now every minute, and the server now sends a new subscription the pending ask. Neither has been run through a restart.
- The reader ran for 17 hours before the game. It logged three failed mailbox fetches, one of them a 500 from the mailbox, and went on after each.

Several servers, 2026-11-22, with forge-mcp-server branch `session-ports` at `bcd8437`: two server processes started 4 seconds apart, the second on a free port, each wrote its file, and one reader with a 1 second directory read subscribed to each about a second after it started. No event was sent through them.

Two more games the same day, on LEGION7 through the deployed mailbox, with counts from the server's activity log as reported by the session that ran them:

- After a restart of the server on port 3080 (reader at `f4d9ad1`, refresh every minute): the reader subscribed to the new process 7 seconds after it started, having been retrying every 15 seconds while the port was down. The server then emitted 163 events and logged 163 deliveries, all 200 on the first attempt. The reader logged no skip. No ask was pending at the restart, so the server's replay of one was not exercised.
- With two servers (reader at `8a46e9e`, forge-mcp-server `bcd8437`): a stand-in held port 3080 and the playing session's server took a free port. The reader found it in the registry and subscribed 4 seconds after it started. That server emitted 118 events and logged 118 deliveries, all 200 on the first attempt. The reader's state holds 118 handled deliveries from it and its cursor reads 118. The mod, with `relay` on, started turns in that session from the reader's endpoint. The stand-in emitted nothing, so two servers did not send events at the same time.

The first-poll request, 2026-11-22: accepted by forge-mcp-server's HTTP endpoint at `59864fd`, which answered with no events. Not run against `5933c34`, the build that answers with the waiting ask.

Per-path limits, 2026-11-23, branch `hook-limits`, not deployed: the mailbox tests pass in memory, and those that do not need a small limit or a movable clock pass under `wrangler dev`. Under `wrangler dev`, a hook and delivery written by the `main` Worker were read by this one with `lastReadAt` set and no `idleSeconds`, and the hook still accepted a POST. A hook with `idleSeconds: 60` and not fetched for 62 seconds was removed with its delivery at the next request, and a hook without it was kept.

Not tested: two servers emitting during the same game, a server restart with an ask pending, the cursor against a subscription that lapsed, and what a held `wait` request costs in Durable Object duration.

`spike/mailbox-spike.mjs` is the first check of the idea, run before the mailbox was written. It also shows the cases a reader has to handle: a mailbox that alters, invents or repeats a delivery, and a mailbox that is down.
