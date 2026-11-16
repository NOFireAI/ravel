/**
 * The demo driver behind `demo/agents-kill-and-recover.sh`: runs the
 * kill-and-recover scenario under the policy named on the command line and
 * prints one `KEY=value` line per figure for the script to assert on.
 * Progress goes to stderr.
 */
import { randomUUID } from "node:crypto";
import { runKillAndRecover, type ScenarioPolicy } from "./scenario.js";
import { RavelAgentsError } from "@ravel/agents";

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

// A worker stopped by a refused poll rejects the run with that error.
const run = await runKillAndRecover({
  url: env("RAVEL_URL"),
  clientToken: env("RAVEL_AGENTS_TOKEN"),
  workerToken: env("RAVEL_WORKER_TOKEN"),
  adminToken: env("RAVEL_ADMIN_TOKEN"),
  policy: policy satisfies ScenarioPolicy,
  key: `demo-${policy}-${randomUUID()}`,
  requestId: `demo-request-${randomUUID()}`,
  log: (line) => {
    console.error(`[${policy}] ${line}`);
  },
}).catch((error: unknown) => {
  if (!(error instanceof RavelAgentsError)) throw error;
  const reason = `${error.code}: ${error.message}`.replace(/\s*\n\s*/g, " ");
  console.error(`[${policy}] stopped: ${reason}`);
  process.exit(1);
});

const draftCalls = run.calls.filter((c) => c.tool === "kb.draft");
const ticketCalls = run.calls.filter((c) => c.tool === "desk.ticket");
const draftEffect = run.effects.find((e) => e.tool_name === "kb.draft");
const lines: Record<string, string | number> = {
  OPERATION_ID: run.operationId,
  RETRIED_OPERATION_ID: run.retriedOperationId ?? "none",
  OP_SAME: String(run.retriedOperationId === run.operationId),
  WORKERS_DIFFER: String(run.workerA !== run.workerB),
  WORKER_A_SIGNAL: run.workerASignal ?? "none",
  SEARCH_RUNS: run.counts.search,
  DRAFT_RUNS: run.counts.draft,
  TICKET_RUNS: run.counts.ticket,
  DRAFT_EFFECTS_DISTINCT: new Set(draftCalls.map((c) => c.effectId)).size,
  DRAFT_CALL_ATTEMPTS: draftCalls.map((c) => c.attempt).join(","),
  DRAFT_ATTEMPTS: draftEffect?.attempt ?? "none",
  TICKET_ATTEMPTS: ticketCalls.map((c) => c.attempt).join(","),
  EFFECT_STATES: run.effects.map((e) => `${e.tool_name}:${e.status}`).join(","),
  RESULT_ERROR: run.resultError ?? "none",
  RESULT_ARTICLES: run.result?.articles.join(",") ?? "none",
  RESULT_TICKET: run.result === null ? "none" : (run.result.ticket ?? "null"),
  RESULT_ANSWER: run.result?.answer ?? "none",
  ANSWERED: run.answered,
  IN_DOUBT_ERROR: run.inDoubt?.error.split(":")[0] ?? "none",
  TICKET_RUNS_BEFORE_RECONCILE: run.inDoubt?.ticketRunsBeforeReconcile ?? "none",
  STATUS_IN_DOUBT: run.inDoubt?.serverStatus ?? "none",
  BLOCKED_EFFECT_IS_TICKET: String(
    run.inDoubt !== null && run.inDoubt.blockedEffect === run.inDoubt.effect.effect,
  ),
  RECOVERY_MS: run.recoveryMs,
};
for (const [key, value] of Object.entries(lines)) console.log(`${key}=${String(value)}`);
