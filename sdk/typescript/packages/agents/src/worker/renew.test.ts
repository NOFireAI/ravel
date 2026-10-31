import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { z } from "zod";
import { AgentGone, OutcomeUnknown, OwnershipLost } from "../errors.js";
import { decodeWire, WorkerPollResponse, type TurnAssignment } from "../wire/index.js";
import { defineAgent, handlerDigest, serveAgents, type Transport, type TurnReport } from "./index.js";
import { LeaseExpired, MIN_RENEW_INTERVAL_MS, renewInterval } from "./runner.js";

const fixtureDir = resolve(dirname(fileURLToPath(import.meta.url)), "../../../../../fixtures");
const pollFixture = JSON.parse(readFileSync(resolve(fixtureDir, "worker_poll_response.json"), "utf8")) as {
  assignment: Record<string, unknown> & { lease: Record<string, unknown> };
};

/** The scripted coordinator's lease and grace: a turn idle past 8 s is taken over. */
const LEASE_MS = 6_000;
const GRACE_MS = 2_000;
/** How long the handler runs: longer than the lease plus its grace. */
const HANDLER_MS = 9_000;

const signals: AbortSignal[] = [];
/** The fake clock at each abort of a handler's signal. */
const abortedAt: number[] = [];

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
        await new Promise((done) => setTimeout(done, HANDLER_MS));
        return null;
      },
    }),
  }),
});

function assignment(): TurnAssignment {
  const body = {
    assignment: {
      ...pollFixture.assignment,
      handler: "think",
      lease: { ...pollFixture.assignment.lease, handler_digest: handlerDigest(agent, "think") },
      steps: [],
    },
  };
  const decoded = decodeWire(WorkerPollResponse, body).assignment;
  if (decoded === null) throw new Error("fixture has no assignment");
  return decoded;
}

/**
 * A coordinator that leases one turn at its first poll for `leaseMs` and
 * renews it by `leaseMs` past the fake clock on each heartbeat. The n-th
 * heartbeat rejects with `beats[n]` (or `beats(n)`) when that is set. Like the server, it
 * refuses a commit once the clock passes the last expiry plus `GRACE_MS`,
 * since a poll by another worker would have taken the turn over by then.
 */
function leasingTransport(
  beats: (Error | undefined)[] | ((n: number) => Error | undefined) = [],
  leaseMs = LEASE_MS,
) {
  const heartbeatAt: number[] = [];
  const commitAt: number[] = [];
  let polled = false;
  let expiresMs = 0;
  const transport: Transport = {
    register: () => Promise.resolve({ worker_id: "w-0000000000000001" }),
    heartbeat: () => {
      const n = heartbeatAt.length;
      heartbeatAt.push(Date.now());
      const refusal = typeof beats === "function" ? beats(n) : beats[n];
      if (refusal !== undefined) return Promise.reject(refusal);
      expiresMs = Date.now() + leaseMs;
      return Promise.resolve({ lease_expires_unix_ns: BigInt(expiresMs) * 1_000_000n });
    },
    poll: (_request, signal) => {
      if (!polled) {
        polled = true;
        expiresMs = Date.now() + leaseMs;
        return Promise.resolve({ assignment: assignment() });
      }
      return new Promise((done) => {
        signal.addEventListener("abort", () => {
          done(null);
        });
      });
    },
    recordStep: () => Promise.reject(new Error("unused")),
    completeEffect: () => Promise.reject(new Error("unused")),
    failEffect: () => Promise.reject(new Error("unused")),
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
  return { transport, heartbeatAt, commitAt };
}

/**
 * Serve `transport` under fake timers from time 0 for 20 s, with the worker
 * told the coordinator's lease is `leaseMs`, and report each turn.
 */
async function serveFor(transport: Transport, leaseMs = LEASE_MS): Promise<TurnReport[]> {
  vi.useFakeTimers({ now: 0 });
  signals.length = 0;
  abortedAt.length = 0;
  try {
    const reports: TurnReport[] = [];
    const worker = await serveAgents({
      transport,
      agents: [agent],
      workerId: "w-0000000000000001",
      leaseMs,
      onTurn: (report) => reports.push(report),
    });
    await vi.advanceTimersByTimeAsync(20_000);
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
      const { transport, heartbeatAt, commitAt } = leasingTransport(() => unavailable, leaseMs);
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

  it("an answered renewal restarts the lease the worker measures", async () => {
    const unavailable = new OutcomeUnknown("worker/heartbeat: HTTP 503");
    // Answered at 0 s and 2 s, then failing: the lease runs out 6 s after
    // the answer at 2 s, not 6 s after the assignment.
    const { transport, commitAt } = leasingTransport((n) => (n < 2 ? undefined : unavailable));
    const reports = await serveFor(transport);
    expect(abortedAt).toEqual([2_000 + LEASE_MS]);
    expect(signals[0]?.reason).toBeInstanceOf(LeaseExpired);
    expect(commitAt).toEqual([]);
    expect(reports.map((r) => r.outcome)).toEqual(["abandoned"]);
  });
});
