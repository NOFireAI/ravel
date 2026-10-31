/**
 * Kill a worker inside a step and recover the operation on a new worker,
 * against a real server. Shared by the e2e test (`e2e/recover.test.ts`) and
 * the demo (`demo/agents-kill-and-recover.sh`), so both exercise the same
 * path.
 *
 * Worker A is a child process (`main.ts --worker <policy>`). It holds the
 * killed step after the tool's side effect and before `completeEffect`, says
 * so on stdout, and is sent SIGKILL there. Worker B runs in this process and
 * takes the turn over once worker A's lease lapses, from the steps and
 * effects the server recorded. Both workers append their tool calls to one
 * counters file; the effects are read from the server.
 */
import { type ChildProcessByStdio, spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { createInterface } from "node:readline";
import type { Readable } from "node:stream";
import { fileURLToPath, pathToFileURL } from "node:url";
import { type Assistant, createAssistant, DRAFT, SEARCH, TICKET } from "./assistant.js";
import { AssistantFixture, readCounters, type ToolCall } from "./fixture.js";
import { EffectInDoubt } from "@ravel/agents";
import { createClient, type EffectAttemptBody, type EffectBody } from "@ravel/agents/client";
import { createHttpTransport, serveAgents, type TurnReport, type WorkerHandle } from "@ravel/agents/worker";

export type ScenarioPolicy = "at_least_once" | "manual";

/** The tool whose step worker A is killed in, per policy. */
export const KILLED_TOOL: Readonly<Record<ScenarioPolicy, string>> = {
  at_least_once: DRAFT,
  manual: TICKET,
};

export interface ScenarioOptions {
  readonly url: string;
  /** A credential with the `agents` capability. */
  readonly clientToken: string;
  /** A credential with the `agents_worker` capability. */
  readonly workerToken: string;
  /** A credential with the `agents_admin` capability, for the reconcile under `manual`. */
  readonly adminToken: string;
  /** `at_least_once` kills worker A inside the draft step; `manual` inside the ticket step. */
  readonly policy: ScenarioPolicy;
  /** The agent key; one per run, so runs against one store do not meet. */
  readonly key: string;
  readonly requestId: string;
  /** Bound on each wait: the child's start, the kill, the takeover, the result. */
  readonly waitMs?: number;
  readonly log?: (line: string) => void;
}

export interface ScenarioResult {
  readonly operationId: string;
  /** The operation the second send under the same request id resolved to. */
  readonly retriedOperationId: string | undefined;
  readonly workerA: string;
  readonly workerB: string;
  /** The signal worker A's process exited by: `SIGKILL` when the kill landed. */
  readonly workerASignal: NodeJS.Signals | null;
  /** The operation's result, or null when it did not arrive within `waitMs`. */
  readonly result: { answer: string; articles: string[]; ticket: string | null } | null;
  /** Why `result` is null. */
  readonly resultError: string | null;
  /** Every tool run by either worker, in order, from the counters file. */
  readonly calls: readonly ToolCall[];
  readonly counts: { readonly search: number; readonly draft: number; readonly ticket: number };
  /** The agent's effects as the server lists them once the operation finished. */
  readonly effects: readonly EffectBody[];
  readonly answered: number;
  /** Set under `manual`: what the server and worker B showed before the reconcile. */
  readonly inDoubt: {
    readonly error: string;
    /** The ticket effect as the server's effects list showed it. */
    readonly effect: EffectBody;
    /** The ticket effect's attempts as the server's effect inspect route showed them. */
    readonly attempts: readonly EffectAttemptBody[];
    /** The server's status for the operation while the effect was in doubt. */
    readonly serverStatus: string;
    /** The effect the server named as blocking the operation. */
    readonly blockedEffect: string | null;
    readonly ticketRunsBeforeReconcile: number;
  } | null;
  /** Milliseconds from the kill to the result, mostly worker A's lease running out. */
  readonly recoveryMs: number;
}

const PAYLOAD = { question: "How does a worker recover a step after a crash?" } as const;
const HERE = dirname(fileURLToPath(import.meta.url));

async function waitFor(
  what: string,
  ms: number,
  condition: () => boolean,
  stopped: Promise<never>,
): Promise<void> {
  const deadline = Date.now() + ms;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`${what}: not reached within ${String(ms)} ms`);
    await Promise.race([new Promise((resolve) => setTimeout(resolve, 25)), stopped]);
  }
}

/** Worker A: `main.ts --worker <policy>` under the same node and loader hook as this process. */
function spawnWorkerA(options: ScenarioOptions, counters: string): ChildProcessByStdio<null, Readable, Readable> {
  return spawn(
    process.execPath,
    [
      "--experimental-transform-types",
      "--disable-warning=ExperimentalWarning",
      "--import",
      pathToFileURL(join(HERE, "..", "ts-hooks.js")).href,
      join(HERE, "main.ts"),
      "--worker",
      options.policy,
    ],
    {
      env: {
        ...process.env,
        RAVEL_URL: options.url,
        RAVEL_WORKER_TOKEN: options.workerToken,
        RAVEL_COUNTERS: counters,
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
}

export async function runKillAndRecover(options: ScenarioOptions): Promise<ScenarioResult> {
  const waitMs = options.waitMs ?? 120_000;
  const log = options.log ?? (() => undefined);
  const scratch = mkdtempSync(join(tmpdir(), "ravel-kill-and-recover-"));
  const counters = join(scratch, "counters.jsonl");
  const fixture = new AssistantFixture(counters);
  const { assistant, tools } = createAssistant(fixture);
  const client = createClient({ url: options.url, token: options.clientToken });
  const admin = createClient({ url: options.url, token: options.adminToken });
  const agent = client.agent<Assistant>("assistant", options.key);
  const payload = { ...PAYLOAD, escalate: options.policy === "manual" };
  const killedTool = KILLED_TOOL[options.policy];
  const workers: WorkerHandle[] = [];
  // A stopping error on worker B (a refused poll) or worker A exiting before
  // it was killed rejects `stopped`, and every wait below races it, so the
  // run rejects with that error instead of running into a deadline.
  let stop: (error: unknown) => void = () => undefined;
  const stopped = new Promise<never>((_, reject) => {
    stop = reject;
  });
  stopped.catch(() => undefined);

  const child = spawnWorkerA(options, counters);
  let killing = false;
  const stderr: string[] = [];
  child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk.toString("utf8")));
  const exited = new Promise<NodeJS.Signals | null>((resolve) => {
    child.once("exit", (code, signal) => {
      if (!killing) {
        stop(new Error(`worker A exited ${String(signal ?? code)} before it was killed:\n${stderr.join("")}`));
      }
      resolve(signal);
    });
  });
  let workerA = "";
  let held = false;
  createInterface({ input: child.stdout }).on("line", (line) => {
    const [tag, value] = line.split(" ");
    if (tag === "WORKER") workerA = value ?? "";
    if (tag === "HELD") held = true;
  });

  try {
    await agent.create();
    await waitFor("worker A serving", waitMs, () => workerA !== "", stopped);
    log(`worker A ${workerA} serving as process ${String(child.pid)}`);

    const first = await agent.send("ask", payload, { requestId: options.requestId });
    const operationId = first.operationId ?? "";
    log(`sent request ${options.requestId}: operation ${operationId}`);
    await waitFor(`worker A inside ${killedTool}`, waitMs, () => held, stopped);
    killing = true;
    const killedAt = Date.now();
    child.kill("SIGKILL");
    const workerASignal = await exited;
    log(`worker A killed by ${String(workerASignal)} inside ${killedTool} attempt 1`);

    // A client that lost the first answer sends again under the same request id.
    const retried = await agent.send("ask", payload, { requestId: options.requestId });
    log(`resent request ${options.requestId}: operation ${retried.operationId ?? "none"}`);

    const reports: TurnReport[] = [];
    const workerB = await serveAgents({
      transport: createHttpTransport({ url: options.url, token: options.workerToken }),
      agents: [assistant],
      tools,
      onTurn: (report) => reports.push(report),
    });
    workers.push(workerB);
    workerB.done.catch(stop);
    log(`worker B ${workerB.ownerId} serving; waiting for worker A's lease to lapse`);

    let inDoubt: ScenarioResult["inDoubt"] = null;
    if (options.policy === "manual") {
      const isBlocked = (r: TurnReport) => r.outcome === "abandoned" && r.error instanceof EffectInDoubt;
      await waitFor("worker B blocked on the effect in doubt", waitMs, () => reports.some(isBlocked), stopped);
      const blocked = reports.find(isBlocked);
      const effect = (await agent.effects()).find((e) => e.tool_name === TICKET);
      if (blocked?.outcome !== "abandoned" || effect === undefined) throw new Error("no effect in doubt");
      const inspected = await client.effect(effect.effect);
      const info = await first.status();
      const ticketCalls = readCounters(counters).filter((c) => c.tool === TICKET);
      inDoubt = {
        error: String(blocked.error),
        effect,
        attempts: inspected.attempts,
        serverStatus: info.status,
        blockedEffect: info.blockedEffect,
        ticketRunsBeforeReconcile: ticketCalls.length,
      };
      log(`worker B blocked: ${inDoubt.error}; the server reads the operation ${info.status}`);
      // The operator looks at the desk, finds the ticket the killed run filed,
      // and records it as the effect's result rather than filing another.
      const filed = (ticketCalls[0]?.output as { ticket?: string } | undefined)?.ticket ?? "";
      await admin.reconcile({
        effect: effect.effect,
        attempt: effect.attempt,
        decision: "record_result",
        result: { ticket: filed },
      });
      log(`reconciled ${effect.effect} attempt ${String(effect.attempt)} with ticket ${filed}`);
    }

    // A miss is reported with the tool counts rather than thrown, so a
    // caller can see which tool ran again when the operation never finished.
    // A stopped worker is not a miss: it rejects the run.
    const settled = await Promise.race([
      first.result({ deadlineMs: waitMs }).then(
        (value) => ({ value, error: null }),
        (error: unknown) => ({ value: null, error: String(error) }),
      ),
      stopped,
    ]);
    const recoveryMs = Date.now() - killedAt;
    const state = await agent.state();
    const calls: ToolCall[] = readCounters(counters).map(({ tool, effectId, attempt }) => ({
      tool,
      effectId,
      attempt,
    }));
    const count = (tool: string) => calls.filter((c) => c.tool === tool).length;
    return {
      operationId,
      retriedOperationId: retried.operationId,
      workerA,
      workerB: workerB.ownerId,
      workerASignal,
      result: settled.value,
      resultError: settled.error,
      calls,
      counts: { search: count(SEARCH), draft: count(DRAFT), ticket: count(TICKET) },
      effects: await agent.effects(),
      answered: state.state?.answered ?? 0,
      inDoubt,
      recoveryMs,
    };
  } finally {
    killing = true;
    child.kill("SIGKILL");
    await Promise.all(workers.map((w) => w.close()));
    rmSync(scratch, { recursive: true, force: true });
  }
}
