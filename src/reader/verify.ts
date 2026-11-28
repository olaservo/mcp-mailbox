import { createHmac, timingSafeEqual } from "node:crypto";

/** A delivery as `GET /deliveries` returns it. `body` is base64. */
export interface Fetched {
  seq: number;
  hook: string;
  receivedAt: number;
  headers: Record<string, string>;
  body: string;
}

/** Standard Webhooks allows a timestamp this far from the time of receipt. */
const TOLERANCE_MS = 5 * 60 * 1000;
/** The mailbox drops a delivery after 7 days. One older than this by the reader's clock is refused. */
export const MAX_HELD_MS = 8 * 24 * 60 * 60 * 1000;

/** Standard Webhooks `v1`: HMAC-SHA256 over `<id>.<timestamp>.<body>`, with the body as received. */
function signatureOk(secret: string, delivery: Fetched): boolean {
  const key = Buffer.from(secret.slice(6), "base64");
  const signed = `${delivery.headers["webhook-id"]}.${delivery.headers["webhook-timestamp"]}.`;
  const expected = createHmac("sha256", key).update(signed).update(Buffer.from(delivery.body, "base64")).digest();
  return (delivery.headers["webhook-signature"] ?? "").split(" ").some((part) => {
    if (!part.startsWith("v1,")) return false;
    const given = Buffer.from(part.slice(3), "base64");
    return given.length === expected.length && timingSafeEqual(given, expected);
  });
}

/**
 * Why a delivery is refused, or undefined when it verifies. The signed timestamp is compared with the
 * mailbox's `receivedAt`, since a held delivery is old by the reader's clock. The mailbox could state
 * any `receivedAt`, so that is held to the mailbox's retention against the reader's clock.
 */
export function refusal(secret: string, delivery: Fetched, now: number): string | undefined {
  const { headers, receivedAt } = delivery;
  if (!headers["webhook-id"] || !headers["x-mcp-subscription-id"]) return "missing headers";
  const timestamp = Number(headers["webhook-timestamp"]);
  if (!Number.isFinite(timestamp)) return "missing timestamp";
  if (!signatureOk(secret, delivery)) return "bad signature";
  if (Math.abs(receivedAt - timestamp * 1000) > TOLERANCE_MS) return "timestamp is not close to the time of receipt";
  if (receivedAt > now + TOLERANCE_MS || receivedAt < now - MAX_HELD_MS) return "time of receipt is outside the mailbox's retention";
  return undefined;
}
