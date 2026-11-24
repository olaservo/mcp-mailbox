# mcp-mailbox

A mailbox for MCP Events webhook delivery (SEP-3415). An MCP server POSTs signed events to the mailbox. A client with no public address fetches them from the mailbox and verifies them with a secret the mailbox never holds.

Status: the mailbox runs as a Cloudflare Worker, and games of a Forge MCP server have run through the reader since 2026-11-22. Encryption is not started.

```
               events/subscribe (callback: hook URL, secret)
  MCP server  ◀─────────────────────────────────────────────  reader
      │                                                        ▲   │
      │ POST signed event                   GET /deliveries    │   │ events/poll
      ▼                                                        │   ▼  on 127.0.0.1:<port>
  mailbox (Worker, holds no secret)  ──────────────────────────┘  mcp-events mod
```

## For implementers

SEP-3415 allows a webhook callback that is not the client: "A common deployment is a forward proxy that receives webhooks and serves events to clients by poll or push." This is one such relay, and it needs no spec change. The client generates `delivery.secret` and keeps it. The relay stores and hands out deliveries, and cannot forge or alter one without the client's check failing.

The relay can still read events, drop them, and hand an old one out again. A client reading through it has to:

- verify `webhook-signature` over the stored bytes with its own secret;
- check `webhook-timestamp` against the time the relay received the delivery (`receivedAt`), since a held delivery is older than 5 minutes by the client's clock;
- deduplicate on `x-mcp-subscription-id` plus `webhook-id`;
- check that the event name in the signed body matches the subscription, since the subscription id header is not signed;
- order events by their content. Observed with forge-mcp-server: it POSTs events concurrently, and four events emitted in order arrived in another order.

Endpoint verification: the mailbox serves `/.well-known/mcp-webhook-receiver.json` with `{"receivers": ["/hooks/"]}`, the SEP's option for a gateway that fronts many subscriptions. A server that supports it treats every hook URL as verified without a challenge. forge-mcp-server does from 0.3.0. A server that only does the verification handshake still gets an echo for any registered path, without a signature check, since the mailbox has no secret. The SEP says the receiver verifies before echoing, so this part departs from it. Either way, anyone who has a hook URL can point a server at it, and the reader refuses what it receives. Those deliveries still count toward the hook's limit of 2,000.

Where the SEP leaves a gap:

- A server that gives up retrying loses that event, and the client is not told while its subscription stays live. The SEP says such an event can be recovered by cursor replay, and also that a cursor on the refresh of a live subscription is a no-op. The workaround is to unsubscribe and subscribe again with the saved cursor. The reader does not do this. See [ext-events#9](https://github.com/modelcontextprotocol/ext-events/issues/9).
- The relay is the only durability between server and client. An event the server emits while the relay is down past the server's retries is lost.
- The relay can read every event. Hiding them from it needs the server to encrypt `data` to a key the client supplies, which the SEP does not define. That part is not built here.

## Parts

- Mailbox (built): a public `https` endpoint that answers the verification challenge for registered paths and stores each delivery as raw bytes and headers. It runs as a Cloudflare Worker with one SQLite Durable Object. `src/core.ts` holds the HTTP surface and does not depend on Workers.
- Reader (built): a Node process that subscribes with a mailbox hook as the callback, fetches, verifies, deduplicates, orders and hands each event on: to stdout, a command, or a local poll endpoint. `src/reader/`. It has no dependencies.
- Encryption (not started): the server encrypts the payload to a key the client supplies, so the mailbox cannot read events.

## API

Reader routes take `Authorization: Bearer <MAILBOX_KEY>`.

| Route | Who | What |
|---|---|---|
| `POST /hooks` with optional `{"label", "idleSeconds"}` | reader | Creates a callback path. Returns `{token, url, label}`, and `idleSeconds` when given. Use `url` as `delivery.url` in `events/subscribe`. With `idleSeconds` (60 to 604800), a path not fetched with `hook=<token>` for that long is removed with its deliveries. Without it the path is kept. |
| `GET /hooks` | reader | Lists callback paths with `createdAt`, `lastReadAt` and `idleSeconds`. |
| `DELETE /hooks/<token>` | reader | Removes a path and its deliveries. Later POSTs to it get 404. |
| `GET /.well-known/mcp-webhook-receiver.json` | MCP server | `{"receivers": ["/hooks/"]}`, so a server that supports SEP-3415's well-known option skips the challenge. No key. |
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
- Events a server emits after a restart and before the next refresh, up to a minute, are not delivered. forge-mcp-server's cursors are per process, so a cursor does not bring them back. It does send a new subscription the ask that is still pending.
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

Hooks the tests make on a remote mailbox have `idleSeconds` 3600, so the mailbox removes them an hour after their last fetch. Some tests ack without `hook`, which deletes every hook's deliveries up to that point. Do not run them against a mailbox a reader is using.

`FORGE_MCP_SERVER=<path to a built forge-mcp-server>` adds a test that runs the reader against that server's HTTP endpoint and webhook hub.

## Deploy

```bash
npx wrangler deploy
npx wrangler secret put MAILBOX_KEY
```

The key is at least 32 characters. Reader routes answer 503 without it.

## Tested

- The tests pass in memory, under `wrangler dev` and against the deployed Worker.
- Endpoint verification: forge-mcp-server's handshake passes against the mailbox with its `https` and public-address checks on. From 0.3.0 it reads the well-known document and sends no challenge, and the games on 2026-11-24 ran 0.3.1.
- Games, 2026-11-22 to 24, through the deployed mailbox: seven games with every event the server emitted delivered (156, 163, 118, 108, 96, 136 and 102). In the last three the reader handed on every event once, with no gaps in the event ids. A poll with no cursor returned each pending ask once (23 asks). After a server restart the reader subscribed to the new process 7 seconds later.
- A Worker deploy reaches an active Durable Object up to 5 minutes late. Observed on 2026-11-23.

Not tested: two servers emitting during the same game, a server restart with an ask pending, the cursor against a subscription that lapsed, and what a held `wait` request costs in Durable Object duration.
