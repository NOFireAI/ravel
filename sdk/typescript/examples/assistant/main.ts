/**
 * Ask the assistant one question against a running server and print the
 * answer: `npm run example:assistant` from `sdk/typescript`.
 *
 * Reads RAVEL_URL (default http://127.0.0.1:4318), RAVEL_AGENTS_TOKEN (a
 * credential with the `agents` capability) and RAVEL_WORKER_TOKEN (one with
 * `agents_worker`), and the question from the command line.
 *
 * `main.ts --worker <policy>` is worker A of the kill-and-recover scenario
 * (`scenario.ts`) instead: it serves the assistant only, appends every tool
 * call to the file RAVEL_COUNTERS names, and holds the step the policy kills
 * in (`kb.draft` for at_least_once, `desk.ticket` for manual) after the tool
 * ran, printing `HELD <tool> <attempt> <effect>` for the scenario to SIGKILL
 * it on. It prints `WORKER <owner id>` once serving.
 */
import { randomUUID } from "node:crypto";
import { createAssistant, type Assistant } from "./assistant.js";
import { AssistantFixture } from "./fixture.js";
import { KILLED_TOOL } from "./scenario.js";
import { RavelAgentsError } from "@ravel/agents";
import { createClient } from "@ravel/agents/client";
import { createHttpTransport, serveAgents } from "@ravel/agents/worker";

/** The error's code, when it carries one, and its message, on one line. */
function oneLine(error: unknown): string {
  const text = error instanceof RavelAgentsError ? `${error.code}: ${error.message}` : String(error);
  return text.replace(/\s*\n\s*/g, " ");
}

function required(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === "") {
    console.error(`${name} is not set`);
    process.exit(64);
  }
  return value;
}

const url = process.env.RAVEL_URL ?? "http://127.0.0.1:4318";
const workerToken = required("RAVEL_WORKER_TOKEN");
const workerStopped = (error: unknown): never => {
  console.error(`worker stopped: ${oneLine(error)}`);
  process.exit(1);
};

if (process.argv[2] === "--worker") {
  const policy = process.argv[3];
  if (policy !== "at_least_once" && policy !== "manual") {
    console.error("usage: main.ts --worker at_least_once|manual");
    process.exit(64);
  }
  const fixture = new AssistantFixture(required("RAVEL_COUNTERS"));
  fixture.holdAt({
    tool: KILLED_TOOL[policy],
    attempt: 1,
    release: new Promise<void>(() => undefined),
    onHeld: (call) => {
      console.log(`HELD ${call.tool} ${String(call.attempt)} ${call.effectId}`);
    },
  });
  const { assistant, tools } = createAssistant(fixture);
  const worker = await serveAgents({
    transport: createHttpTransport({ url, token: workerToken }),
    agents: [assistant],
    tools,
  }).catch(workerStopped);
  console.log(`WORKER ${worker.ownerId}`);
  await worker.done.catch(workerStopped);
} else {
  const clientToken = required("RAVEL_AGENTS_TOKEN");
  const question = process.argv.slice(2).join(" ") || "How does a worker recover a step after a crash?";
  const fixture = new AssistantFixture();
  const { assistant, tools } = createAssistant(fixture);
  // A token refused at register rejects serveAgents itself.
  const worker = await serveAgents({
    transport: createHttpTransport({ url, token: workerToken }),
    agents: [assistant],
    tools,
  }).catch(workerStopped);
  // A refused poll (a revoked worker token, say) stops the worker and rejects
  // `done`: end the run there rather than wait out the result's deadline.
  const stopped = worker.done.then(() => undefined, workerStopped);
  try {
    const agent = createClient({ url, token: clientToken }).agent<Assistant>("assistant", `example-${randomUUID()}`);
    await agent.create();
    const handle = await agent.send("ask", { question, escalate: false });
    const result = await handle.result({ deadlineMs: 60_000 });
    console.log(`operation ${handle.operationId ?? "unknown"}`);
    console.log(`articles  ${result.articles.join(", ") || "none"}`);
    console.log(`answer    ${result.answer}`);
    console.log(`tool runs ${fixture.calls.map((c) => c.tool).join(", ")}`);
  } finally {
    await worker.close();
  }
  await stopped;
}
