/**
 * The demo driver behind `demo/agents-kill-and-recover.sh`: runs the
 * kill-and-recover scenario under the policy named on the command line and
 * prints one `KEY=value` line per figure for the script to assert on.
 * Progress goes to stderr.
 */
import { randomUUID } from "node:crypto";
import { runKillAndRecover, type ScenarioPolicy } from "./scenario.js";

const policy = process.argv[2];
if (policy !== "at_least_once" && policy !== "manual") {
  console.error("usage: kill-and-recover.ts at_least_once|manual");
  process.exit(64);
}
const env = (name: string): string => {
  const value = process.env[name];
  if (value === undefined || value === "") {
    console.error(`${name} is not set`);
    process.exit(64);
  }
  return value;
};

const run = await runKillAndRecover({
  url: env("RAVEL_URL"),
  clientToken: env("RAVEL_AGENTS_TOKEN"),
  workerToken: env("RAVEL_WORKER_TOKEN"),
  policy: policy satisfies ScenarioPolicy,
  key: `demo-${policy}-${randomUUID()}`,
  requestId: `demo-request-${randomUUID()}`,
  log: (line) => {
    console.error(`[${policy}] ${line}`);
  },
});

const draftCalls = run.calls.filter((c) => c.tool === "kb.draft");
const ticketCalls = run.calls.filter((c) => c.tool === "desk.ticket");
const lines: Record<string, string | number> = {
  OPERATION_ID: run.operationId,
  RETRIED_SAME_OPERATION: String(run.retriedOperationId === run.operationId),
  WORKERS_DIFFER: String(run.workerA !== run.workerB),
  SEARCH_RUNS: run.counts.search,
  DRAFT_RUNS: run.counts.draft,
  TICKET_RUNS: run.counts.ticket,
  DRAFT_EFFECTS_DISTINCT: new Set(draftCalls.map((c) => c.effectId)).size,
  DRAFT_ATTEMPTS: draftCalls.map((c) => c.attempt).join(","),
  TICKET_ATTEMPTS: ticketCalls.map((c) => c.attempt).join(","),
  EFFECT_STATES: run.effects.map((e) => `${e.tool_name}:${e.status}`).join(","),
  RESULT_ARTICLES: run.result.articles.join(","),
  RESULT_TICKET: run.result.ticket ?? "null",
  RESULT_ANSWER: run.result.answer,
  ANSWERED: run.answered,
  IN_DOUBT_ERROR: run.inDoubt?.error.split(":")[0] ?? "none",
  IN_DOUBT_TICKET_RUNS: run.inDoubt?.ticketRunsBeforeReconcile ?? "none",
  IN_DOUBT_SERVER_STATUS: run.inDoubt?.serverStatus ?? "none",
  RECOVERY_MS: run.recoveryMs,
};
for (const [key, value] of Object.entries(lines)) console.log(`${key}=${String(value)}`);
