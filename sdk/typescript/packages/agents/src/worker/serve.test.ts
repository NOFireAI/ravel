import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { z } from "zod";
import { AgentGone, EffectInDoubt, Forbidden, OutcomeUnknown, StoreUnavailable, UnsupportedFormat } from "../errors.js";
import { defineTool } from "../tools/index.js";
import { decodeWire, WorkerPollResponse, type TurnAssignment } from "../wire/index.js";
import { Backoff, OperationBackoffs } from "./backoff.js";
import { defineAgent, handlerDigest, serveAgents, type Transport, type TurnReport } from "./index.js";

const fixtureDir = resolve(dirname(fileURLToPath(import.meta.url)), "../../../../../fixtures");
const pollFixture = JSON.parse(readFileSync(resolve(fixtureDir, "worker_poll_response.json"), "utf8")) as {
  assignment: Record<string, unknown> & { lease: Record<string, unknown> };
};

const ticket = defineTool({
  name: "desk.ticket",
  version: "1",
  input: z.object({}),
  output: z.object({}),
  recovery: "manual",
  run: () => Promise.resolve({}),
});

const agent = defineAgent({
  name: "order",
  version: "1.0.0",
  state: z.unknown(),
  initialState: null,
  messages: (message) => ({
    block: message({
      payload: z.unknown(),
      result: z.object({}),
      handler: (ctx) => ctx.step("ticket", ticket, {}),
    }),
    pass: message({ payload: z.unknown(), result: z.null(), handler: () => Promise.resolve(null) }),
  }),
});

/**
 * `block` abandons with the transport's step error (EffectInDoubt unless
 * set), `pass` commits, `incompatible` abandons with IncompatibleHandler. A
 * bare kind runs under the fixture's operation id.
 */
type Kind = "block" | "pass" | "incompatible";
type Script = Kind | { readonly kind: Kind; readonly operation: string } | Error;

function assignment(kind: Kind, operation?: string): TurnAssignment {
  const handler = kind === "incompatible" ? "pass" : kind;
  const digest = kind === "incompatible" ? "00".repeat(32) : handlerDigest(agent, handler);
  const body = {
    assignment: {
      ...pollFixture.assignment,
      ...(operation === undefined ? {} : { operation }),
      handler,
      lease: { ...pollFixture.assignment.lease, handler_digest: digest },
      steps: [],
    },
  };
  const decoded = decodeWire(WorkerPollResponse, body).assignment;
  if (decoded === null) throw new Error("fixture has no assignment");
  return decoded;
}

/**
 * A transport that answers each poll with the next scripted turn or error,
 * stamping the fake clock, and then waits for the worker to close. Every
 * step record rejects with `stepError()`.
 */
function scriptedTransport(
  script: Script[],
  stepError: () => Error = () => new EffectInDoubt("the ticket's attempt has no result"),
) {
  const polledAt: number[] = [];
  const transport: Transport = {
    register: () => Promise.resolve({ worker_id: "w-0000000000000001" }),
    heartbeat: () => Promise.reject(new Error("unused")),
    poll: (_request, signal) => {
      polledAt.push(Date.now());
      const next = script.shift();
      if (next === undefined) {
        return new Promise((done) => {
          signal.addEventListener("abort", () => {
            done(null);
          });
        });
      }
      if (next instanceof Error) return Promise.reject(next);
      if (typeof next === "string") return Promise.resolve({ assignment: assignment(next) });
      return Promise.resolve({ assignment: assignment(next.kind, next.operation) });
    },
    recordStep: () => Promise.reject(stepError()),
    completeEffect: () => Promise.reject(new Error("unused")),
    failEffect: () => Promise.reject(new Error("unused")),
    commit: (request) =>
      Promise.resolve({
        operation: request.lease.turn_id,
        status: "completed",
        state_version: 1,
        rebased: false,
        effects: [],
      }),
  };
  return { transport, polledAt };
}

/** Run `body` under fake timers starting at time 0, restoring real ones after. */
async function withFakeClock(body: () => Promise<void>): Promise<void> {
  vi.useFakeTimers({ now: 0 });
  try {
    await body();
  } finally {
    vi.useRealTimers();
  }
}

describe("worker backoff", () => {
  it("a Backoff doubles from its start to its cap and starts over on reset", () => {
    const backoff = new Backoff(250, 30_000);
    const seen = Array.from({ length: 10 }, () => backoff.next());
    expect(seen).toEqual([250, 500, 1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000, 30_000]);
    backoff.reset();
    expect(backoff.next()).toBe(250);
  });

  it("OperationBackoffs keeps one delay per operation and forgets the least recently used past its capacity", () => {
    const delays = new OperationBackoffs(250, 1_000, 2);
    expect([delays.next("a"), delays.next("a"), delays.next("b"), delays.next("a")]).toEqual([250, 500, 250, 1_000]);
    delays.forget("a");
    expect(delays.next("a")).toBe(250);
    // "b" (next delay 500) is now the least recently used, so a third
    // operation evicts it and "b" starts over at 250, evicting "a" in turn.
    expect(delays.next("c")).toBe(250);
    expect(delays.size).toBe(2);
    expect(delays.next("b")).toBe(250);
    expect(delays.next("c")).toBe(500);
    expect(delays.next("a")).toBe(250);
  });

  it("doubles one operation's pause after each EffectInDoubt abandonment up to the cap and resets it on its commit", async () => {
    await withFakeClock(async () => {
      const { transport, polledAt } = scriptedTransport([
        "block",
        "block",
        "incompatible",
        "block",
        "block",
        "pass",
        "block",
      ]);
      const reports: TurnReport[] = [];
      const worker = await serveAgents({
        transport,
        agents: [agent],
        workerId: "w-0000000000000001",
        abandonDelayMs: 250,
        maxBackoffMs: 1_000,
        onTurn: (report) => reports.push(report),
      });
      await vi.advanceTimersByTimeAsync(10_000);
      await worker.close();
      expect(reports.map((r) => (r.outcome === "committed" ? "committed" : (r.error as Error).name))).toEqual([
        "EffectInDoubt",
        "EffectInDoubt",
        "IncompatibleHandler",
        "EffectInDoubt",
        "EffectInDoubt",
        "committed",
        "EffectInDoubt",
      ]);
      // In-doubt pauses 250, 500, then 1000 twice at the cap; the
      // IncompatibleHandler abandonment keeps the fixed 250 and does not
      // advance them; the commit resets them, so the last block waits 250.
      expect(polledAt).toEqual([0, 250, 750, 1_000, 2_000, 3_000, 3_000, 3_250]);
    });
  });

  it("doubles the pause after each StoreUnavailable heartbeat abandonment, and an IncompatibleHandler abandonment keeps the fixed pause", async () => {
    await withFakeClock(async () => {
      const { transport, polledAt } = scriptedTransport(
        ["block", "block", "block", "incompatible", "block"],
        () => new StoreUnavailable("step/record: object store unavailable"),
      );
      const reports: TurnReport[] = [];
      const worker = await serveAgents({
        transport,
        agents: [agent],
        workerId: "w-0000000000000001",
        abandonDelayMs: 250,
        maxBackoffMs: 1_000,
        onTurn: (report) => reports.push(report),
      });
      await vi.advanceTimersByTimeAsync(10_000);
      await worker.close();
      expect(reports.map((r) => (r.outcome === "committed" ? "committed" : (r.error as Error).name))).toEqual([
        "StoreUnavailable",
        "StoreUnavailable",
        "StoreUnavailable",
        "IncompatibleHandler",
        "StoreUnavailable",
      ]);
      // Pauses 250, 500, 1000 for the store outage, the fixed 250 for the
      // handler mismatch, then 1000 at the cap.
      expect(polledAt).toEqual([0, 250, 750, 1_750, 2_000, 3_000]);
    });
  });

  it("an operation at the backoff cap does not raise an unrelated operation's pause, and the unrelated commit does not reset it", async () => {
    await withFakeClock(async () => {
      const a = { kind: "block", operation: "op-a" } as const;
      const b = { kind: "block", operation: "op-b" } as const;
      const { transport, polledAt } = scriptedTransport([a, a, a, b, a, { kind: "pass", operation: "op-b" }, a]);
      const reports: TurnReport[] = [];
      const worker = await serveAgents({
        transport,
        agents: [agent],
        workerId: "w-0000000000000001",
        abandonDelayMs: 250,
        maxBackoffMs: 1_000,
        onTurn: (report) => reports.push(report),
      });
      await vi.advanceTimersByTimeAsync(10_000);
      await worker.close();
      expect(reports.map((r) => `${r.operation}:${r.outcome}`)).toEqual([
        "op-a:abandoned",
        "op-a:abandoned",
        "op-a:abandoned",
        "op-b:abandoned",
        "op-a:abandoned",
        "op-b:committed",
        "op-a:abandoned",
      ]);
      // op-a pauses 250, 500, then 1000; op-b's abandonment pauses the base
      // 250; op-a stays at 1000; op-b's commit pauses nothing and leaves
      // op-a at the cap.
      expect(polledAt).toEqual([0, 250, 750, 1_750, 2_000, 3_000, 3_000, 4_000]);
    });
  });
});

describe("worker agent_gone", () => {
  it("agent_gone at commit abandons the turn without retry", async () => {
    await withFakeClock(async () => {
      const { transport, polledAt } = scriptedTransport(["pass", "pass"]);
      const gone = new AgentGone("agent destroyed");
      const commits: string[] = [];
      const commit = transport.commit.bind(transport);
      transport.commit = (request) => {
        commits.push(request.lease.turn_id);
        return commits.length === 1 ? Promise.reject(gone) : commit(request);
      };
      const reports: TurnReport[] = [];
      const worker = await serveAgents({
        transport,
        agents: [agent],
        workerId: "w-0000000000000001",
        abandonDelayMs: 250,
        onTurn: (report) => reports.push(report),
      });
      await vi.advanceTimersByTimeAsync(10_000);
      await worker.close();
      const [first, second] = reports;
      expect(first?.outcome).toBe("abandoned");
      expect(first?.outcome === "abandoned" && first.error).toBe(gone);
      // One commit for the abandoned turn and one for the next poll's turn: no retry.
      expect(commits).toHaveLength(2);
      expect(second?.outcome).toBe("committed");
      expect(polledAt).toEqual([0, 250, 250]);
    });
  });
});

describe("worker poll errors", () => {
  it("a Forbidden poll stops the worker and rejects done with it", async () => {
    const refused = new Forbidden(403, "worker/poll: HTTP 403 forbidden: token revoked");
    const { transport, polledAt } = scriptedTransport([refused, "pass"]);
    const worker = await serveAgents({ transport, agents: [agent], workerId: "w-0000000000000001" });
    const outcome = await worker.done.then(
      () => "fulfilled",
      (error: unknown) => error,
    );
    expect(outcome).toBe(refused);
    expect(polledAt).toHaveLength(1);
    await worker.close();
  });

  it("a Forbidden poll goes to onError, stops the worker and fulfils done", async () => {
    const refused = new Forbidden(401, "worker/poll: HTTP 401 unauthorized");
    const { transport, polledAt } = scriptedTransport([refused, "pass"]);
    const errors: unknown[] = [];
    const worker = await serveAgents({
      transport,
      agents: [agent],
      workerId: "w-0000000000000001",
      onError: (error) => errors.push(error),
    });
    await worker.done;
    expect(errors).toEqual([refused]);
    expect(polledAt).toHaveLength(1);
    await worker.close();
  });

  it("a poll answered with UnsupportedFormat is retried and the next poll runs its turn", async () => {
    await withFakeClock(async () => {
      const unreadable = new UnsupportedFormat("worker/poll: HTTP 404 answer does not match the wire schema");
      const { transport, polledAt } = scriptedTransport([unreadable, "pass"]);
      const errors: unknown[] = [];
      const reports: TurnReport[] = [];
      const worker = await serveAgents({
        transport,
        agents: [agent],
        workerId: "w-0000000000000001",
        onError: (error) => errors.push(error),
        onTurn: (report) => reports.push(report),
      });
      let settled = false;
      void worker.done.finally(() => {
        settled = true;
      });
      await vi.advanceTimersByTimeAsync(10_000);
      expect(settled).toBe(false);
      await worker.close();
      expect(errors).toEqual([unreadable]);
      expect(reports.map((r) => r.outcome)).toEqual(["committed"]);
      // One 50 ms pause after the unreadable answer, then the turn, then the
      // poll that waits for close.
      expect(polledAt).toEqual([0, 50, 50]);
    });
  });

  it("a transient OutcomeUnknown poll is retried after a doubling pause and the next poll runs its turn", async () => {
    await withFakeClock(async () => {
      const lost = () => new OutcomeUnknown("worker/poll: request failed: connection reset");
      const { transport, polledAt } = scriptedTransport([lost(), lost(), "pass", lost()]);
      const errors: unknown[] = [];
      const reports: TurnReport[] = [];
      const worker = await serveAgents({
        transport,
        agents: [agent],
        workerId: "w-0000000000000001",
        onError: (error) => errors.push(error),
        onTurn: (report) => reports.push(report),
      });
      let settled = false;
      void worker.done.finally(() => {
        settled = true;
      });
      await vi.advanceTimersByTimeAsync(10_000);
      expect(settled).toBe(false);
      await worker.close();
      expect(errors).toHaveLength(3);
      expect(errors.every((e) => e instanceof OutcomeUnknown)).toBe(true);
      expect(reports.map((r) => r.outcome)).toEqual(["committed"]);
      // Pauses of 50 and 100 ms, none after the poll that answered, and 50
      // again after the next failure.
      expect(polledAt).toEqual([0, 50, 150, 150, 200]);
    });
  });
});
