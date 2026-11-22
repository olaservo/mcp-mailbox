import { spawn } from "node:child_process";
import { homedir } from "node:os";
import { join } from "node:path";
import { PollEndpoint } from "./poll-endpoint.ts";
import { Reader, type Received } from "./reader.ts";

const USAGE = `Usage: node --env-file=<file> src/reader/cli.ts [run|unsubscribe]

  MAILBOX_URL, MAILBOX_KEY   the mailbox and the reader's key
  READER_SERVER_URL          the MCP server's Streamable HTTP endpoint, such as https://host/mcp
  READER_SERVER_TOKEN        its bearer token
  READER_SERVERS_DIR         a directory where servers announce themselves, one *.json file each with
                             "url" and optionally "token". Each one is subscribed. Takes the place of
                             READER_SERVER_URL, or adds to it.
  READER_EVENTS              event names, comma separated
  READER_EXEC                a command run once per event, with the event as JSON on stdin.
                             Without it, each event is one JSON line on stdout.
  READER_POLL_PORT           serve the events as MCP Events poll delivery on 127.0.0.1:<port>, for a client
                             that polls. Takes the place of READER_EXEC.
  READER_STATE               state file (default ~/.mcp-mailbox/reader-state.json). It holds the signing secret.
  READER_LABEL               the hook's label in the mailbox`;

const env = process.env;
const missing = ["MAILBOX_URL", "MAILBOX_KEY", "READER_EVENTS"].filter((name) => !env[name]);
if (!env.READER_SERVER_URL && !env.READER_SERVERS_DIR) missing.push("READER_SERVER_URL or READER_SERVERS_DIR");
const command = process.argv[2] ?? "run";
if (missing.length > 0 || !["run", "unsubscribe"].includes(command)) {
  console.error(missing.length > 0 ? `Not set: ${missing.join(", ")}\n\n${USAGE}` : USAGE);
  process.exit(2);
}

const log = (line: string) => console.error(`${new Date().toISOString()} ${line}`);

/** Runs `READER_EXEC` through the shell with the event on stdin, and waits for it to exit. */
function exec(commandLine: string, received: Received): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(commandLine, {
      shell: true,
      stdio: ["pipe", "inherit", "inherit"],
      env: { ...env, MCP_EVENT_NAME: received.event.name, MCP_EVENT_ID: received.event.eventId, MCP_SUBSCRIPTION_ID: received.subscriptionId },
    });
    child.on("error", reject);
    child.on("exit", (code) => (code === 0 ? resolve() : reject(new Error(`exit ${code}`))));
    child.stdin.on("error", () => {});
    child.stdin.end(JSON.stringify(received));
  });
}

const events = env.READER_EVENTS!.split(",").map((name) => name.trim()).filter(Boolean);
const endpoint = env.READER_POLL_PORT ? new PollEndpoint(events) : undefined;

const reader = new Reader({
  mailbox: { url: env.MAILBOX_URL!, key: env.MAILBOX_KEY! },
  server: env.READER_SERVER_URL ? { url: env.READER_SERVER_URL, token: env.READER_SERVER_TOKEN } : undefined,
  servers: env.READER_SERVERS_DIR ? { dir: env.READER_SERVERS_DIR, token: env.READER_SERVER_TOKEN } : undefined,
  events,
  stateFile: env.READER_STATE ?? join(homedir(), ".mcp-mailbox", "reader-state.json"),
  label: env.READER_LABEL,
  log,
  handle: (received) => {
    if (endpoint) return endpoint.add(received.event);
    if (env.READER_EXEC) return exec(env.READER_EXEC, received);
    console.log(JSON.stringify(received));
  },
});

if (command === "unsubscribe") {
  await reader.unsubscribe();
  log("unsubscribed and removed the hook");
} else {
  const stop = new AbortController();
  for (const signal of ["SIGINT", "SIGTERM"] as const) process.on(signal, () => stop.abort());
  if (endpoint) log(`serving events/poll on http://127.0.0.1:${(await endpoint.listen(Number(env.READER_POLL_PORT))).port}`);
  log(`reading ${env.MAILBOX_URL} for ${env.READER_EVENTS} from ${[env.READER_SERVER_URL, env.READER_SERVERS_DIR].filter(Boolean).join(" and ")}`);
  await reader.run(stop.signal);
}
