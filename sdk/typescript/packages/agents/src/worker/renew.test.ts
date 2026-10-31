import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { z } from "zod";
import { AgentGone, OutcomeUnknown, OwnershipLost } from "../errors.js";
import { defineTool } from "../tools/index.js";
import { decodeWire, StepRecordResponse, WorkerPollResponse, type TurnAssignment } from "../wire/index.js";
import {
  DEFAULT_LEASE_MS,
  defineAgent,
  handlerDigest,
  InvalidWorkerOption,
  serveAgents,
  type ServeOptions,
  type Transport,
  type TurnReport,
} from "./index.js";
import { LeaseExpired, MIN_RENEW_INTERVAL_MS, renewInterval } from "./runner.js";

const fixtureDir = resolve(dirname(fileURLToPath(import.meta.url)), "../../../../../fixtures");
const pollFixture = JSON.parse(readFileSync(resolve(fixtureDir, "worker_poll_response.json"), "utf8")) as {
  assignment: Record<string, unknown> & { lease: Record<string, unknown> };
};
/** A step journaled with no result: the answer authorizes an attempt. */
const startedStep = decodeWire(
  StepRecordResponse,
  JSON.parse(readFileSync(resolve(fixtureDir, "step_record_response.json"), "utf8")),
);

/** The scripted coordinator's lease and grace: a turn idle past 8 s is taken over. */
const LEASE_MS = 6_000;
const GRACE_MS = 2_000;
/** How long the handler runs: longer than the lease plus its grace. */
const HANDLER_MS = 9_000;

let handlerMs = HANDLER_MS;
/** How long the `charge` handler works without a step, ignoring its signal, before its step. */
let beforeStepMs = 0;
const signals: AbortSignal[] = [];
/** The fake clock at each abort of a handler's signal. */
const abortedAt: number[] = [];
/** The fake clock at each run of the `charge` tool, and what each step rejected with. */
const toolRunAt: number[] = [];
const stepErrors: unknown[] = [];

const charge = defineTool({
  name: "card.charge",
  version: "1",
  input: z.object({}),
  output: z.object({}),
  recovery: "at_least_once",
  run: () => {
    toolRunAt.push(Date.now());
    return Promise.resolve({});
  },
});

const agent = defineAgent({
  name: "order",
  version: "1.0.0",
  state: z.unknown(),
  initialState: null,
  messages: (message) => ({
    // Runs no step, so nothing but a background renewal keeps its lease.
    think: message({
      payload: z.unknown(),
      result: z.null(),
      handler: async (ctx) => {
        signals.push(ctx.signal);
        ctx.signal.addEventListener("abort", () => abortedAt.push(Date.now()));
        await new Promise((done) => setTimeout(done, handlerMs));
        return null;
      },
    }),
    // Works past the lease without looking at its signal, then runs a step.
    charge: message({
      payload: z.unknown(),
      result: z.null(),
      handler: async (ctx) => {
        signals.push(ctx.signal);
        ctx.signal.addEventListener("abort", () => abortedAt.push(Date.now()));
        await new Promise((done) => setTimeout(done, beforeStepMs));
        await ctx.step("charge", charge, {}).catch((error: unknown) => stepErrors.push(error));
        return null;
      },
    }),
  }),
});

function assignment(handler: string): TurnAssignment {
  const body = {
    assignment: {
      ...pollFixture.assignment,
      handler,
      lease: { ...pollFixture.assignment.lease, handler_digest: handlerDigest(agent, handler) },
      steps: [],
    },
  };
  const decoded = decodeWire(WorkerPollResponse, body).assignment;
  if (decoded === null) throw new Error("fixture has no assignment");
  return decoded;
}

interface Leasing {
  /** The coordinator's lease. */
  readonly leaseMs?: number;
  /** How long each heartbeat's answer travels back after the coordinator processed it. */
  readonly answerMs?: number;
  /** How long a step record's answer takes. */
  readonly recordMs?: number;
  /** The handler the turn runs. */
  readonly handler?: string;
}

const after = (ms: number): Promise<void> =>
  ms === 0 ? Promise.resolve() : new Promise((done) => setTimeout(done, ms));

/**
 * A coordinator that leases one turn at its first poll for `leaseMs` and
 * renews it by `leaseMs` past the fake clock on each heartbeat, counted from
 * when it processed the request, as the server does. The n-th heartbeat
 * rejects with `beats[n]` (or `beats(n)`) when that is set. Like the server,
 * it refuses a commit once the clock passes the last expiry plus `GRACE_MS`,
 * since a poll by another worker would have taken the turn over by then. A
 * step is journaled with no result, which authorizes an attempt.
 */
function leasingTransport(
  beats: (Error | undefined)[] | ((n: number) => Error | undefined) = [],
  { leaseMs = LEASE_MS, answerMs = 0, recordMs = 0, handler = "think" }: Leasing = {},
) {
  const heartbeatAt: number[] = [];
  const commitAt: number[] = [];
  const recordStepAt: number[] = [];
  const effectAt: number[] = [];
  let polled = false;
  let expiresMs = 0;
  const transport: Transport = {
    register: () => Promise.resolve({ worker_id: "w-0000000000000001" }),
    heartbeat: async () => {
      const n = heartbeatAt.length;
      heartbeatAt.push(Date.now());
      const refusal = typeof beats === "function" ? beats(n) : beats[n];
      if (refusal === undefined) expiresMs = Date.now() + leaseMs;
      const granted = expiresMs;
      await after(answerMs);
      if (refusal !== undefined) throw refusal;
      return { lease_expires_unix_ns: BigInt(granted) * 1_000_000n };
    },
    poll: (_request, signal) => {
      if (!polled) {
        polled = true;
        expiresMs = Date.now() + leaseMs;
        return Promise.resolve({ assignment: assignment(handler) });
      }
      return new Promise((done) => {
        signal.addEventListener("abort", () => {
          done(null);
        });
      });
    },
    recordStep: async () => {
      recordStepAt.push(Date.now());
      await after(recordMs);
      return startedStep;
    },
    completeEffect: (request) => {
      effectAt.push(Date.now());
      return Promise.resolve({ effect: request.effect, status: "completed", late: false });
    },
    failEffect: (request) => {
      effectAt.push(Date.now());
      return Promise.resolve({ effect: request.effect, status: "failed", late: false });
    },
    commit: (request) => {
      commitAt.push(Date.now());
      if (Date.now() > expiresMs + GRACE_MS) {
        return Promise.reject(new OwnershipLost("the turn was taken over after its lease and grace ran out"));
      }
      return Promise.resolve({
        operation: request.lease.turn_id,
        status: "completed",
        state_version: 1,
        rebased: false,
        effects: [],
      });
    },
  };
  return { transport, heartbeatAt, commitAt, recordStepAt, effectAt };
}

/**
 * Serve `transport` under fake timers from time 0 for `serveMs`, with the
 * worker told the coordinator's lease is `leaseMs` (left unset when `null`),
 * and report each turn.
 */
async function serveFor(
  transport: Transport,
  leaseMs: number | null = LEASE_MS,
  serveMs = 20_000,
): Promise<TurnReport[]> {
  vi.useFakeTimers({ now: 0 });
  signals.length = 0;
  abortedAt.length = 0;
  toolRunAt.length = 0;
  stepErrors.length = 0;
  try {
    const reports: TurnReport[] = [];
    const options: ServeOptions = {
      transport,
      agents: [agent],
      workerId: "w-0000000000000001",
      onTurn: (report) => reports.push(report),
    };
    const worker = await serveAgents(leaseMs === null ? options : { ...options, leaseMs });
    await vi.advanceTimersByTimeAsync(serveMs);
    await worker.close();
    return reports;
  } finally {
    vi.useRealTimers();
  }
}

describe("lease renewal", () => {
  it("renews a third of the lease left, and never faster than the floor", () => {
    expect(renewInterval(undefined, 0)).toBe(MIN_RENEW_INTERVAL_MS);
    expect(renewInterval(30_000_000_000n, 0)).toBe(10_000);
    expect(renewInterval(30_000_000_000n, 27_000)).toBe(MIN_RENEW_INTERVAL_MS);
    expect(renewInterval(30_000_000_000n, 40_000)).toBe(MIN_RENEW_INTERVAL_MS);
  });

  it("a turn longer than the lease renews it in the background and commits", async () => {
    const { transport, heartbeatAt, commitAt } = leasingTransport();
    const reports = await serveFor(transport);
    // At assignment, then every third of the 6 s lease each answer grants,
    // until the handler returns at 9 s; none after the commit.
    expect(heartbeatAt).toEqual([0, 2_000, 4_000, 6_000, 8_000]);
    expect(commitAt).toEqual([HANDLER_MS]);
    expect(reports.map((r) => r.outcome)).toEqual(["committed"]);
    expect(signals[0]?.aborted).toBe(false);
  });

  it.each([
    ["OwnershipLost", new OwnershipLost("lease no longer names HEAD's owner, epoch and turn")],
    ["AgentGone", new AgentGone("agent is tombstoned")],
  ])("a refused renewal aborts the turn and nothing is committed (%s)", async (_name, refusal) => {
    const { transport, heartbeatAt, commitAt } = leasingTransport([undefined, undefined, refusal]);
    const reports = await serveFor(transport);
    // Refused at 4 s; no renewal after it and no commit.
    expect(heartbeatAt).toEqual([0, 2_000, 4_000]);
    expect(commitAt).toEqual([]);
    expect(signals).toHaveLength(1);
    expect(signals[0]?.aborted).toBe(true);
    expect(signals[0]?.reason).toBe(refusal);
    expect(reports).toHaveLength(1);
    const [report] = reports;
    expect(report?.outcome).toBe("abandoned");
    expect(report?.outcome === "abandoned" && report.error).toBe(refusal);
  });

  it("a transient renewal failure inside the lease does not abort the turn", async () => {
    const unavailable = new OutcomeUnknown("worker/heartbeat: HTTP 503");
    const { transport, heartbeatAt, commitAt } = leasingTransport([undefined, unavailable]);
    const reports = await serveFor(transport);
    // The 503 at 2 s renews nothing, so the next tick is a third of the
    // 4 s left on the expiry granted at 0 s; renewals resume from there.
    expect(heartbeatAt).toEqual([0, 2_000, 3_333, 5_333, 7_333]);
    expect(commitAt).toEqual([HANDLER_MS]);
    expect(reports.map((r) => r.outcome)).toEqual(["committed"]);
    expect(signals[0]?.aborted).toBe(false);
    expect(abortedAt).toEqual([]);
  });

  // Two lease lengths, one not a multiple of the 1 s renewal pace: an abort
  // after a fixed count of failures lands on a whole second and matches
  // neither, while the expiry tracks the lease.
  it.each([6_000, 4_500])(
    "renewals that keep failing past the lease abort the turn and send no commit (lease %i ms)",
    async (leaseMs) => {
      const unavailable = new OutcomeUnknown("worker/heartbeat: HTTP 503");
      const { transport, heartbeatAt, commitAt } = leasingTransport(() => unavailable, { leaseMs });
      const reports = await serveFor(transport, leaseMs);
      // No answer ever names an expiry, so renewals run at the 1 s floor from
      // assignment until the lease the poll granted at 0 s runs out.
      expect(heartbeatAt.every((at) => at < leaseMs)).toBe(true);
      expect(heartbeatAt.length).toBeGreaterThanOrEqual(Math.floor(leaseMs / MIN_RENEW_INTERVAL_MS));
      expect(signals).toHaveLength(1);
      expect(abortedAt).toEqual([leaseMs]);
      expect(signals[0]?.reason).toBeInstanceOf(LeaseExpired);
      // Nothing was committed at the expiry, nor in the 20 s served after it.
      expect(commitAt).toEqual([]);
      expect(reports).toHaveLength(1);
      const [report] = reports;
      expect(report?.outcome).toBe("abandoned");
      expect(report?.outcome === "abandoned" && report.error).toBe(signals[0]?.reason);
    },
  );

  it("an answered renewal restarts the lease the worker measures from its sending", async () => {
    const unavailable = new OutcomeUnknown("worker/heartbeat: HTTP 503");
    // Each answer travels 500 ms. Sent at 0 s and answered at 0.5 s, then
    // sent a third of the 5.5 s left later, at 2.333 s, and answered at
    // 2.833 s; every later one fails. The coordinator counted the last grant
    // from 2.333 s, when it processed the request, so the lease runs out at
    // 8.333 s: 6 s after that renewal was sent, not after its answer came
    // back, nor after the assignment.
    const { transport, heartbeatAt, commitAt } = leasingTransport((n) => (n < 2 ? undefined : unavailable), {
      answerMs: 500,
    });
    const reports = await serveFor(transport);
    expect(heartbeatAt.slice(0, 2)).toEqual([0, 2_333]);
    expect(abortedAt).toEqual([2_333 + LEASE_MS]);
    expect(signals[0]?.reason).toBeInstanceOf(LeaseExpired);
    expect(commitAt).toEqual([]);
    expect(reports.map((r) => r.outcome)).toEqual(["abandoned"]);
  });

  it("leaseMs left unset aborts at the 30 s default", async () => {
    expect(DEFAULT_LEASE_MS).toBe(30_000);
    handlerMs = 40_000;
    try {
      const unavailable = new OutcomeUnknown("worker/heartbeat: HTTP 503");
      const { transport, commitAt } = leasingTransport(() => unavailable, { leaseMs: 30_000 });
      const reports = await serveFor(transport, null, 45_000);
      expect(abortedAt).toEqual([30_000]);
      expect(signals[0]?.reason).toBeInstanceOf(LeaseExpired);
      expect(commitAt).toEqual([]);
      expect(reports.map((r) => r.outcome)).toEqual(["abandoned"]);
    } finally {
      handlerMs = HANDLER_MS;
    }
  });

  it.each([999, 0, -1, 1_500.5, Number.NaN, Number.POSITIVE_INFINITY, 2_147_483_648])(
    "serveAgents refuses leaseMs %d at startup with InvalidWorkerOption",
    async (leaseMs) => {
      const { transport } = leasingTransport();
      const register = vi.spyOn(transport, "register");
      const poll = vi.spyOn(transport, "poll");
      const started = serveAgents({ transport, agents: [agent], leaseMs });
      await expect(started).rejects.toBeInstanceOf(InvalidWorkerOption);
      await expect(started).rejects.toMatchObject({ option: "leaseMs" });
      // Refused before any request, never at turn time.
      expect(register).not.toHaveBeenCalled();
      expect(poll).not.toHaveBeenCalled();
    },
  );

  it.each([1_000, 2_147_483_647])("serveAgents accepts leaseMs %d", async (leaseMs) => {
    const { transport } = leasingTransport();
    const worker = await serveAgents({ transport, agents: [agent], workerId: "w-0000000000000001", leaseMs });
    await worker.close();
  });
});

describe("a step after the turn aborted", () => {
  const unavailable = new OutcomeUnknown("worker/heartbeat: HTTP 503");

  it("a step called after the lease expired is refused locally and never reaches the server", async () => {
    // The handler works until 7 s, past the 6 s lease, without looking at
    // its signal, then runs a step.
    beforeStepMs = LEASE_MS + 1_000;
    const { transport, commitAt, recordStepAt, effectAt } = leasingTransport(() => unavailable, {
      handler: "charge",
    });
    const reports = await serveFor(transport);
    expect(abortedAt).toEqual([LEASE_MS]);
    expect(recordStepAt.filter((at) => at >= LEASE_MS)).toEqual([]);
    expect(recordStepAt).toEqual([]);
    expect(toolRunAt).toEqual([]);
    expect(effectAt).toEqual([]);
    expect(commitAt).toEqual([]);
    expect(stepErrors).toHaveLength(1);
    expect(stepErrors[0]).toBeInstanceOf(LeaseExpired);
    expect(stepErrors[0]).toBe(signals[0]?.reason);
    expect(reports.map((r) => r.outcome)).toEqual(["abandoned"]);
  });

  it("a lease that expires while the step's record is in flight runs no tool", async () => {
    // The step is recorded at 5 s, inside the 6 s lease, and its answer
    // comes back at 7 s, after the expiry.
    beforeStepMs = LEASE_MS - 1_000;
    const { transport, commitAt, recordStepAt, effectAt } = leasingTransport(() => unavailable, {
      handler: "charge",
      recordMs: 2_000,
    });
    const reports = await serveFor(transport);
    expect(abortedAt).toEqual([LEASE_MS]);
    expect(recordStepAt).toEqual([LEASE_MS - 1_000]);
    expect(toolRunAt).toEqual([]);
    expect(effectAt).toEqual([]);
    expect(commitAt).toEqual([]);
    expect(stepErrors).toHaveLength(1);
    expect(stepErrors[0]).toBeInstanceOf(LeaseExpired);
    expect(reports.map((r) => r.outcome)).toEqual(["abandoned"]);
  });

  it("a step inside the lease runs its tool and the turn commits", async () => {
    beforeStepMs = 1_000;
    const { transport, commitAt, recordStepAt, effectAt } = leasingTransport([], { handler: "charge" });
    const reports = await serveFor(transport);
    expect(recordStepAt).toEqual([1_000]);
    expect(toolRunAt).toEqual([1_000]);
    expect(effectAt).toEqual([1_000]);
    expect(stepErrors).toEqual([]);
    expect(commitAt).toEqual([1_000]);
    expect(reports.map((r) => r.outcome)).toEqual(["committed"]);
  });
});
