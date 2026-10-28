/**
 * Kill a worker inside a step and recover the operation on a new worker,
 * against a real server. Shared by the e2e test (`e2e/recover.test.ts`) and
 * the demo (`demo/agents-kill-and-recover.sh`), so both exercise the same
 * path.
 *
 * The kill is an in-process abort, not a process kill: the step journal is
 * the SDK's provisional in-memory one until the server mounts step and effect
 * routes, so worker A and worker B share it inside one process. Worker A is
 * closed from inside the tool, after the tool's side effect happened and
 * before its result is recorded, which is the instant a process kill would
 * leave the effect without a result. The server knows nothing of the abort:
 * it hands the turn to worker B only once worker A's lease expires.
 */
import { type Assistant, createAssistant, DRAFT, TICKET } from "./assistant.js";
import { AssistantFixture, type ToolCall } from "./fixture.js";
import { EffectInDoubt } from "@ravel/agents";
import { createClient } from "@ravel/agents/client";
import {
  createHttpTransport,
  MemoryStepJournal,
  serveAgents,
  type TurnReport,
  type WorkerHandle,
} from "@ravel/agents/worker";

type EffectBody = Awaited<ReturnType<MemoryStepJournal["effects"]>>[number];

export type ScenarioPolicy = "at_least_once" | "manual";

export interface ScenarioOptions {
  readonly url: string;
  /** A credential with the `agents` capability. */
  readonly clientToken: string;
  /** A credential with the `agents_worker` capability. */
  readonly workerToken: string;
  /** `at_least_once` kills the worker inside the draft step; `manual` inside the ticket step. */
  readonly policy: ScenarioPolicy;
  /** The agent key; one per run, so runs against one store do not meet. */
  readonly key: string;
  readonly requestId: string;
  /** Bound on each wait: the kill, the takeover, the result. */
  readonly waitMs?: number;
  readonly log?: (line: string) => void;
}

export interface ScenarioResult {
  readonly operationId: string;
  /** The operation the second send under the same request id resolved to. */
  readonly retriedOperationId: string | undefined;
  readonly workerA: string;
  readonly workerB: string;
  /** The operation's result, or null when it did not arrive within `waitMs`. */
  readonly result: { answer: string; articles: string[]; ticket: string | null } | null;
  /** Why `result` is null. */
  readonly resultError: string | null;
  readonly calls: readonly ToolCall[];
  readonly counts: { readonly search: number; readonly draft: number; readonly ticket: number };
  /** The journal's effects once the operation completed. */
  readonly effects: readonly EffectBody[];
  readonly answered: number;
  /** Set under `manual`: what worker B saw before the reconcile. */
  readonly inDoubt: {
    readonly error: string;
    readonly effect: EffectBody;
    /** The server's status for the operation while the effect was in doubt. */
    readonly serverStatus: string;
    readonly ticketRunsBeforeReconcile: number;
  } | null;
  /** Milliseconds from the kill to the result, mostly worker A's lease running out. */
  readonly recoveryMs: number;
}

const PAYLOAD = { question: "How does a worker recover a step after a crash?" } as const;

async function within<T>(what: string, ms: number, work: Promise<T>): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`${what}: not reached within ${String(ms)} ms`));
    }, ms);
  });
  try {
    return await Promise.race([work, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

async function waitFor(what: string, ms: number, condition: () => boolean): Promise<void> {
  const deadline = Date.now() + ms;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`${what}: not reached within ${String(ms)} ms`);
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}

export async function runKillAndRecover(options: ScenarioOptions): Promise<ScenarioResult> {
  const waitMs = options.waitMs ?? 120_000;
  const log = options.log ?? (() => undefined);
  const fixture = new AssistantFixture();
  const { assistant, tools } = createAssistant(fixture);
  const journal = new MemoryStepJournal();
  const client = createClient({ url: options.url, token: options.clientToken });
  const agent = client.agent<Assistant>("assistant", options.key);
  const payload = { ...PAYLOAD, escalate: options.policy === "manual" };
  const workers: WorkerHandle[] = [];
  const serve = (onTurn?: (report: TurnReport) => void) =>
    serveAgents({
      transport: createHttpTransport({ url: options.url, token: options.workerToken, journal }),
      agents: [assistant],
      tools,
      ...(onTurn === undefined ? {} : { onTurn }),
    }).then((worker) => {
      workers.push(worker);
      return worker;
    });

  try {
    await agent.create();
    let killed: Promise<void> | undefined;
    let killedAt = 0;
    const workerA = await serve();
    fixture.interruptAt({
      tool: options.policy === "manual" ? TICKET : DRAFT,
      attempt: 1,
      fire: () => {
        killedAt = Date.now();
        killed = workerA.close();
        return new Error("worker killed inside the step");
      },
    });
    log(`worker A ${workerA.ownerId} serving`);

    const first = await agent.send("ask", payload, { requestId: options.requestId });
    const operationId = first.operationId ?? "";
    log(`sent request ${options.requestId}: operation ${operationId}`);
    await waitFor("worker A killed inside the step", waitMs, () => killed !== undefined);
    await within("worker A closed", waitMs, killed ?? Promise.resolve());
    log(`worker A killed inside ${options.policy === "manual" ? TICKET : DRAFT} attempt 1`);

    // A client that lost the first answer sends again under the same request id.
    const retried = await agent.send("ask", payload, { requestId: options.requestId });
    log(`resent request ${options.requestId}: operation ${retried.operationId ?? "none"}`);

    const reports: TurnReport[] = [];
    const workerB = await serve((report) => reports.push(report));
    log(`worker B ${workerB.ownerId} serving; waiting for worker A's lease to lapse`);

    let inDoubt: ScenarioResult["inDoubt"] = null;
    if (options.policy === "manual") {
      await waitFor("worker B blocked on the effect in doubt", waitMs, () =>
        reports.some((r) => r.outcome === "abandoned" && r.error instanceof EffectInDoubt),
      );
      const blocked = reports.find((r) => r.outcome === "abandoned" && r.error instanceof EffectInDoubt);
      const effects = await journal.effects(operationId);
      const effect = effects.find((e) => e.tool_name === TICKET);
      if (blocked?.outcome !== "abandoned" || effect === undefined) throw new Error("no effect in doubt");
      const serverStatus = (await first.status()).status;
      const ticketRunsBeforeReconcile = fixture.count(TICKET);
      inDoubt = { error: String(blocked.error), effect, serverStatus, ticketRunsBeforeReconcile };
      log(`worker B blocked: ${inDoubt.error}`);
      // The operator looks at the desk, finds the ticket the killed run filed,
      // and records it as the effect's result rather than filing another.
      const filed = fixture.tickets[0] ?? "";
      await journal.reconcile({
        effect: effect.effect,
        attempt: effect.attempt,
        decision: "record_result",
        result: { ticket: filed },
      });
      log(`reconciled ${effect.effect} attempt ${String(effect.attempt)} with ticket ${filed}`);
    }

    // A miss is reported with the tool counts rather than thrown, so a
    // caller can see which tool ran again when the operation never finished.
    const settled = await first.result({ deadlineMs: waitMs }).then(
      (value) => ({ value, error: null }),
      (error: unknown) => ({ value: null, error: String(error) }),
    );
    const recoveryMs = Date.now() - killedAt;
    const state = await agent.state();
    return {
      operationId,
      retriedOperationId: retried.operationId,
      workerA: workerA.ownerId,
      workerB: workerB.ownerId,
      result: settled.value,
      resultError: settled.error,
      calls: [...fixture.calls],
      counts: {
        search: fixture.count("kb.search"),
        draft: fixture.count(DRAFT),
        ticket: fixture.count(TICKET),
      },
      effects: await journal.effects(operationId),
      // The server reads a never-committed agent's state as null, which the
      // client's type does not admit.
      answered: (state.state as { answered: number } | null)?.answered ?? 0,
      inDoubt,
      recoveryMs,
    };
  } finally {
    await Promise.all(workers.map((w) => w.close()));
  }
}
