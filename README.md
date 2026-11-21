# mcp-mailbox

A mailbox for MCP Events webhook delivery (SEP-3415). An MCP server POSTs signed events to the mailbox. A client with no public address fetches them from the mailbox and verifies them with a secret the mailbox never holds.

Status: not built. The repo holds one spike.

## Plan

- Mailbox: a public `https` endpoint that answers the verification challenge for registered paths and stores each delivery as raw bytes and headers. Target: a Cloudflare Worker.
- Reader: a daemon that subscribes with the mailbox URL as the callback, fetches, verifies, deduplicates and hands events to an agent.
- Experiment: the server encrypts the payload to a key the client supplies, so the mailbox cannot read events.

## Spike

`spike/mailbox-spike.mjs` runs a throwaway mailbox with no secret against the real `WebhookHub` from [forge-mcp-server](https://github.com/olaservo/forge-mcp-server), on loopback with the server's `insecure` option.

```bash
node spike/mailbox-spike.mjs <path to a built forge-mcp-server>
```

Observed on 2026-11-21 against the `events-push` branch at `692526b`, 14 of 14 checks passed:

- A mailbox with no secret passes the server's verification handshake. A path the client did not register is refused at subscribe.
- Deliveries held as raw bytes and headers verify later with the client's secret.
- A body altered by the mailbox fails verification. A delivery invented by the mailbox fails verification.
- A delivery fetched 10 minutes late fails the 5 minute freshness check against the client's clock and passes against the mailbox's `receivedAt`.
- The mailbox can hand out an old delivery again and it passes signature and freshness. The client has to deduplicate on subscription id and `webhook-id`.
- An event emitted while the mailbox was down past the server's retries was never delivered, and the next successful delivery cleared `lastError`.

Not tested: a public `https` mailbox, how the reader authenticates to the server, encryption, and a mailbox that drops deliveries.
