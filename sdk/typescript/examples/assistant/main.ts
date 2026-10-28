/**
 * Ask the assistant one question against a running server and print the
 * answer: `npm run example:assistant` from `sdk/typescript`.
 *
 * Reads RAVEL_URL (default http://127.0.0.1:4318), RAVEL_AGENTS_TOKEN (a
 * credential with the `agents` capability) and RAVEL_WORKER_TOKEN (one with
 * `agents_worker`), and the question from the command line.
 */
import { randomUUID } from "node:crypto";
import { createAssistant, type Assistant } from "./assistant.js";
import { AssistantFixture } from "./fixture.js";
import { createClient } from "@ravel/agents/client";
import { createHttpTransport, serveAgents } from "@ravel/agents/worker";

function required(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === "") {
    console.error(`${name} is not set`);
    process.exit(64);
  }
  return value;
}

const url = process.env.RAVEL_URL ?? "http://127.0.0.1:4318";
const clientToken = required("RAVEL_AGENTS_TOKEN");
const workerToken = required("RAVEL_WORKER_TOKEN");
const question = process.argv.slice(2).join(" ") || "How does a worker recover a step after a crash?";

const fixture = new AssistantFixture();
const { assistant, tools } = createAssistant(fixture);
const worker = await serveAgents({
  transport: createHttpTransport({ url, token: workerToken }),
  agents: [assistant],
  tools,
});
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
