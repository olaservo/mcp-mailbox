import { spawn } from "node:child_process";
import { homedir } from "node:os";
import { join } from "node:path";
import { Reader, type Received } from "./reader.ts";

const USAGE = `Usage: node --env-file=<file> src/reader/cli.ts [run|unsubscribe]

  MAILBOX_URL, MAILBOX_KEY   the mailbox and the reader's key
  READER_SERVER_URL          the MCP server's Streamable HTTP endpoint, such as https://host/mcp
  READER_SERVER_TOKEN        its bearer token
  READER_EVENTS              event names, comma separated
  READER_EXEC                a command run once per event, with the event as JSON on stdin.
                             Without it, each event is one JSON line on stdout.
  READER_STATE               state file (default ~/.mcp-mailbox/reader-state.json). It holds the signing secret.
  READER_LABEL               the hook's label in the mailbox`;

const env = process.env;
const missing = ["MAILBOX_URL", "MAILBOX_KEY", "READER_SERVER_URL", "READER_EVENTS"].filter((name) => !env[name]);
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

const reader = new Reader({
  mailbox: { url: env.MAILBOX_URL!, key: env.MAILBOX_KEY! },
  server: { url: env.READER_SERVER_URL!, token: env.READER_SERVER_TOKEN },
  events: env.READER_EVENTS!.split(",").map((name) => name.trim()).filter(Boolean),
  stateFile: env.READER_STATE ?? join(homedir(), ".mcp-mailbox", "reader-state.json"),
  label: env.READER_LABEL,
  log,
  handle: (received) => {
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
  log(`reading ${env.MAILBOX_URL} for ${env.READER_EVENTS} from ${env.READER_SERVER_URL}`);
  await reader.run(stop.signal);
}
