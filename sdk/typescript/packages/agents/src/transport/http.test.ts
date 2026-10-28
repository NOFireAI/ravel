import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { z } from "zod";
import {
  EffectInDoubt,
  Forbidden,
  NonDeterministicReplay,
  OutcomeUnknown,
  OwnershipLost,
  RavelAgentsError,
  StoreUnavailable,
  UnsupportedFormat,
} from "../errors.js";
import { effectId, stepId } from "../identity.js";
import { defineTool } from "../tools/index.js";
import { gate, until } from "../test-util.js";
import { decodeWire, heldLease, WorkerPollResponse, type TurnAssignment } from "../wire/index.js";
import { defineAgent, handlerDigest, serveAgents, type TurnReport } from "../worker/index.js";
import type { StepRecordRequest } from "../worker/transport.js";
import { createHttpTransport, MAX_POLL_WAIT_MS, MemoryStepJournal } from "./http.js";

const fixtureDir = resolve(dirname(fileURLToPath(import.meta.url)), "../../../../../fixtures");
const pollFixture = JSON.parse(readFileSync(resolve(fixtureDir, "worker_poll_response.json"), "utf8")) as {
  assignment: Record<string, unknown> & { lease: Record<string, unknown>; steps: unknown[] };
};

interface Sent {
  readonly url: string;
  readonly authorization: string | null;
  readonly body: unknown;
}

type Answer = { readonly status: number; readonly body?: unknown; readonly text?: string } | Error;

/** A fetch that records each request and answers from `answer`. */
function fakeFetch(answer: (route: string, body: unknown) => Answer) {
  const sent: Sent[] = [];
  const impl: typeof fetch = (input, init) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    const body: unknown = typeof init?.body === "string" ? JSON.parse(init.body) : undefined;
    const headers = new Headers(init?.headers);
    sent.push({ url, authorization: headers.get("authorization"), body });
    const route = url.slice(url.lastIndexOf("/") + 1);
    const a = answer(route, body);
    if (a instanceof Error) return Promise.reject(a);
    const text = a.text ?? (a.body === undefined ? "" : JSON.stringify(a.body));
    return Promise.resolve(new Response(text, { status: a.status }));
  };
  return { sent, fetch: impl };
}

function transportWith(answer: (route: string, body: unknown) => Answer) {
  const fake = fakeFetch(answer);
  const transport = createHttpTransport({ url: "http://ravel.test/", token: "worker-token", fetch: fake.fetch });
  return { ...fake, transport };
}

const LEASE = heldLease(decodeWire(WorkerPollResponse, pollFixture).assignment?.lease ?? fail());

function fail(): never {
  throw new Error("fixture has no assignment");
}

async function caught(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error("expected a rejection");
}

describe("HttpTransport requests", () => {
  it("posts each worker route with the bearer token", async () => {
    const { transport, sent } = transportWith((route) =>
      route === "register"
        ? { status: 200, body: { worker_id: "w-00000000000000a1" } }
        : { status: 200, body: { lease_expires_unix_ns: "1767225630000000000" } },
    );
    expect(await transport.register({ handlers: [] })).toEqual({ worker_id: "w-00000000000000a1" });
    const beat = await transport.heartbeat({ lease: LEASE });
    expect(beat.lease_expires_unix_ns).toBe(1767225630000000000n);
    expect(sent.map((s) => s.url)).toEqual([
      "http://ravel.test/api/v1/agents/worker/register",
      "http://ravel.test/api/v1/agents/worker/heartbeat",
    ]);
    expect(sent.every((s) => s.authorization === "Bearer worker-token")).toBe(true);
  });

  it("clamps the poll wait to 20 s and leaves a shorter one alone", async () => {
    const { transport, sent } = transportWith(() => ({ status: 200, body: { assignment: null } }));
    const signal = new AbortController().signal;
    expect(await transport.poll({ worker_id: "w", handlers: [], wait_ms: 60_000 }, signal)).toBeNull();
    expect(await transport.poll({ worker_id: "w", handlers: [], wait_ms: 5_000 }, signal)).toBeNull();
    expect(MAX_POLL_WAIT_MS).toBe(20_000);
    expect(sent.map((s) => (s.body as { wait_ms: number }).wait_ms)).toEqual([20_000, 5_000]);
  });

  it("decodes the polled lease's recorded time as a bigint and keeps its seed", async () => {
    const { transport } = transportWith(() => ({ status: 200, body: pollFixture }));
    const polled = await transport.poll({ worker_id: "w", handlers: [], wait_ms: 1 }, new AbortController().signal);
    expect(polled?.assignment.lease.recorded_unix_ns).toBe(1767225600000000000n);
    expect(polled?.assignment.lease.random_seed).toBe("8f8d1b06b1618bd08db0300dbba36423");
  });

  it("rejects with the abort reason when the poll is aborted", async () => {
    const controller = new AbortController();
    const reason = new Error("closing");
    const { transport } = transportWith(() => {
      controller.abort(reason);
      return new Error("aborted");
    });
    expect(await caught(transport.poll({ worker_id: "w", handlers: [], wait_ms: 1 }, controller.signal))).toBe(reason);
  });
});

describe("HttpTransport error mapping", () => {
  it("maps an ErrorBody to the class its code names, with its safe ids", async () => {
    const { transport } = transportWith(() => ({
      status: 409,
      body: { code: "ownership_lost", retry: "never", reason: "fenced", ids: { turn_id: "t-1" } },
    }));
    const error = await caught(transport.heartbeat({ lease: LEASE }));
    expect(error).toBeInstanceOf(OwnershipLost);
    expect((error as OwnershipLost).message).toBe("fenced");
    expect((error as OwnershipLost).ids.turnId).toBe("t-1");
  });

  it("maps a typed 503 to StoreUnavailable rather than OutcomeUnknown", async () => {
    const { transport } = transportWith(() => ({
      status: 503,
      body: { code: "store_unavailable", retry: "backoff", reason: "agents admission limit reached", ids: {} },
    }));
    expect(await caught(transport.register({ handlers: [] }))).toBeInstanceOf(StoreUnavailable);
  });

  it.each([401, 403] as const)("maps the plain %i body to Forbidden with its status", async (code) => {
    const { transport } = transportWith(() => ({
      status: code,
      body: { error: "this route requires the agents_worker capability", errorType: "forbidden", status: "error" },
    }));
    const error = await caught(transport.register({ handlers: [] }));
    expect(error).toBeInstanceOf(Forbidden);
    expect((error as Forbidden).status).toBe(code);
    expect((error as Forbidden).message).toContain("this route requires the agents_worker capability");
  });

  it("maps an untyped 5xx and a request with no answer to OutcomeUnknown", async () => {
    const bare = transportWith(() => ({ status: 502, text: "bad gateway" }));
    expect(await caught(bare.transport.commit({ lease: LEASE, outcome: "failed", error: "x", terminal: false }))).toBeInstanceOf(
      OutcomeUnknown,
    );
    const lost = transportWith(() => new TypeError("fetch failed"));
    expect(await caught(lost.transport.heartbeat({ lease: LEASE }))).toBeInstanceOf(OutcomeUnknown);
  });

  it("maps an off-schema 2xx and an untyped 4xx to UnsupportedFormat", async () => {
    const offSchema = transportWith(() => ({ status: 200, body: { worker: 7 } }));
    expect(await caught(offSchema.transport.register({ handlers: [] }))).toBeInstanceOf(UnsupportedFormat);
    const notFound = transportWith(() => ({ status: 404 }));
    expect(await caught(notFound.transport.register({ handlers: [] }))).toBeInstanceOf(UnsupportedFormat);
  });

  it("maps every failure to a RavelAgentsError", async () => {
    const answers: Answer[] = [{ status: 500 }, { status: 418, text: "{" }, new Error("reset"), { status: 200, text: "nope" }];
    for (const answer of answers) {
      const { transport } = transportWith(() => answer);
      expect(await caught(transport.register({ handlers: [] }))).toBeInstanceOf(RavelAgentsError);
    }
  });
});

describe("HttpTransport steps", () => {
  async function polledTransport(heartbeat: () => Answer) {
    const fake = transportWith((route) =>
      route === "poll" ? { status: 200, body: pollFixture } : route === "heartbeat" ? heartbeat() : { status: 404 },
    );
    const polled = await fake.transport.poll({ worker_id: "w", handlers: [], wait_ms: 1 }, new AbortController().signal);
    const lease = heldLease(polled?.assignment.lease ?? fail());
    const request: StepRecordRequest = {
      lease,
      step_key: "charge",
      tool_name: "pay.charge",
      tool_version: "1",
      policy: "at_least_once",
      input_hash: "00".repeat(32),
    };
    return { ...fake, request, assignment: polled?.assignment ?? fail() };
  }

  it("heartbeats the lease before journaling a step and derives ids from incarnation and accept_seq", async () => {
    const { transport, sent, request, assignment } = await polledTransport(() => ({
      status: 200,
      body: { lease_expires_unix_ns: "1" },
    }));
    const record = await transport.recordStep(request);
    expect(sent.map((s) => s.url.slice(s.url.lastIndexOf("/") + 1))).toEqual(["poll", "heartbeat"]);
    expect(record.step.step_id).toBe(stepId(assignment.lease.incarnation, assignment.accept_seq, "charge"));
    expect(record.effect).toBe(effectId(assignment.lease.incarnation, assignment.accept_seq, "charge"));
    expect(record.attempt).toBe(1);
  });

  it("refuses a step for a fenced lease without touching the journal", async () => {
    const { transport, request, assignment } = await polledTransport(() => ({
      status: 409,
      body: { code: "ownership_lost", retry: "never", reason: "taken over", ids: {} },
    }));
    expect(await caught(transport.recordStep(request))).toBeInstanceOf(OwnershipLost);
    expect(await transport.journal.effects(assignment.operation)).toEqual([]);
  });

  it("refuses a step for a turn it never polled", async () => {
    const { transport } = transportWith(() => ({ status: 200, body: { lease_expires_unix_ns: "1" } }));
    const error = await caught(
      transport.recordStep({
        lease: LEASE,
        step_key: "k",
        tool_name: "t",
        tool_version: "1",
        policy: "manual",
        input_hash: "",
      }),
    );
    expect(error).toBeInstanceOf(UnsupportedFormat);
  });

  it("refuses a failed commit while an authorized attempt has no result, and sends it once settled", async () => {
    const { transport, sent, request } = await polledTransport(() => ({ status: 200, body: { lease_expires_unix_ns: "1" } }));
    const record = await transport.recordStep(request);
    const failed = { lease: request.lease, outcome: "failed", error: "boom", terminal: false } as const;
    expect(await caught(transport.commit(failed))).toBeInstanceOf(EffectInDoubt);
    expect(sent.some((s) => s.url.endsWith("/commit"))).toBe(false);
    await transport.failEffect({ effect: record.effect, attempt: record.attempt, error: "boom" });
    // The 404 answer shows the commit reached the wire.
    expect(await caught(transport.commit(failed))).toBeInstanceOf(UnsupportedFormat);
    expect(sent.filter((s) => s.url.endsWith("/commit"))).toHaveLength(1);
  });
});

describe("MemoryStepJournal", () => {
  const scope = { agentId: "a", incarnation: 1, operation: "op", acceptSeq: 3, turnId: "t" };
  const step = (policy: "at_least_once" | "manual", inputHash = "h") => ({
    scope,
    stepKey: "k",
    toolName: "tool",
    toolVersion: "1",
    policy,
    inputHash,
  });

  it("authorizes a new attempt at an unsettled at_least_once step and replays a settled one", async () => {
    const journal = new MemoryStepJournal();
    const first = await journal.recordStep(step("at_least_once"));
    const second = await journal.recordStep(step("at_least_once"));
    expect([first.attempt, second.attempt]).toEqual([1, 2]);
    expect(second.effect).toBe(first.effect);
    expect(await journal.settle({ effect: first.effect, attempt: 1, result: { output: 1 } })).toEqual({
      effect: first.effect,
      status: "authorized",
      late: true,
    });
    await journal.settle({ effect: first.effect, attempt: 2, result: { output: 2 } });
    const replay = await journal.recordStep(step("at_least_once"));
    expect(replay.step.status).toBe("completed");
    expect(replay.step.output).toBe(2);
    expect(replay.attempt).toBe(2);
    expect(journal.evidence).toEqual([{ effect: first.effect, attempt: 1, result: { output: 1 } }]);
  });

  it("refuses a reused step key with a different input hash", async () => {
    const journal = new MemoryStepJournal();
    await journal.recordStep(step("at_least_once"));
    expect(await caught(journal.recordStep(step("at_least_once", "other")))).toBeInstanceOf(NonDeterministicReplay);
  });

  it("puts an unsettled manual step in doubt until a reconcile records its result", async () => {
    const journal = new MemoryStepJournal();
    const first = await journal.recordStep(step("manual"));
    expect(await caught(journal.recordStep(step("manual")))).toBeInstanceOf(EffectInDoubt);
    expect(await caught(journal.recordStep(step("manual")))).toBeInstanceOf(EffectInDoubt);
    expect((await journal.effects("op")).map((e) => e.status)).toEqual(["in_doubt"]);
    expect(
      await journal.reconcile({ effect: first.effect, attempt: 1, decision: "record_result", result: { ok: true } }),
    ).toEqual({ effect: first.effect, status: "completed", attempt: 1 });
    const replay = await journal.recordStep(step("manual"));
    expect(replay.step.status).toBe("completed");
    expect(replay.step.output).toEqual({ ok: true });
  });

  it("hands a reconciled retry out exactly once", async () => {
    const journal = new MemoryStepJournal();
    const first = await journal.recordStep(step("manual"));
    await caught(journal.recordStep(step("manual")));
    expect(await journal.reconcile({ effect: first.effect, attempt: 1, decision: "retry" })).toEqual({
      effect: first.effect,
      status: "authorized",
      attempt: 2,
    });
    expect((await journal.recordStep(step("manual"))).attempt).toBe(2);
    expect(await caught(journal.recordStep(step("manual")))).toBeInstanceOf(EffectInDoubt);
  });

  it("refuses a reconcile of an effect that is not in doubt", async () => {
    const journal = new MemoryStepJournal();
    const first = await journal.recordStep(step("manual"));
    expect(await caught(journal.reconcile({ effect: first.effect, attempt: 1, decision: "retry" }))).toBeInstanceOf(
      UnsupportedFormat,
    );
  });
});

describe("serveAgents over HttpTransport", () => {
  const clockAgent = defineAgent({
    name: "order",
    version: "1.0.0",
    state: z.object({ items: z.array(z.unknown()) }),
    initialState: { items: [] },
    messages: (message) => ({
      place: message({
        payload: z.object({ qty: z.number(), sku: z.string() }),
        result: z.object({ now: z.number(), random: z.number() }),
        handler: (ctx) => Promise.resolve({ now: ctx.now().getTime(), random: ctx.random() }),
      }),
    }),
  });

  async function serveOnce(seed: string, polled: { state?: unknown; base_state_version?: number } = {}) {
    const lease = {
      ...pollFixture.assignment.lease,
      handler_digest: handlerDigest(clockAgent, "place"),
      random_seed: seed,
      ...(polled.base_state_version === undefined ? {} : { base_state_version: polled.base_state_version }),
    };
    const assignment = {
      ...pollFixture.assignment,
      lease,
      steps: [],
      ...("state" in polled ? { state: polled.state } : {}),
    };
    let served = false;
    const commits: unknown[] = [];
    const reports: TurnReport[] = [];
    const { fetch, sent } = fakeFetch((route, body) => {
      if (route === "register") return { status: 200, body: { worker_id: "w-00000000000000b2" } };
      if (route === "poll") {
        if (served) return { status: 200, body: { assignment: null } };
        served = true;
        return { status: 200, body: { assignment } };
      }
      commits.push(body);
      return {
        status: 200,
        body: { operation: pollFixture.assignment.operation, status: "completed", state_version: 1, rebased: false, effects: [] },
      };
    });
    const worker = await serveAgents({
      transport: createHttpTransport({ url: "http://ravel.test", token: "t", fetch }),
      agents: [clockAgent],
      pollWaitMs: 1,
      onTurn: (report) => reports.push(report),
    });
    try {
      await until(() => reports.length === 1);
    } finally {
      await worker.close();
    }
    const commit = commits[0] as { outcome: string; state?: unknown; error?: string; output: { now: number; random: number } };
    return { ownerId: worker.ownerId, sent, commit, output: commit.output, reports };
  }

  it("registers, polls under the minted id and runs the turn with the lease's time and seed", async () => {
    const first = await serveOnce("8f8d1b06b1618bd08db0300dbba36423");
    expect(first.ownerId).toBe("w-00000000000000b2");
    expect(first.sent[0]?.url.endsWith("/worker/register")).toBe(true);
    expect((first.sent[1]?.body as { worker_id: string }).worker_id).toBe("w-00000000000000b2");
    expect(first.reports[0]?.outcome).toBe("committed");
    expect(first.output.now).toBe(1_767_225_600_000);
    const again = await serveOnce("8f8d1b06b1618bd08db0300dbba36423");
    const other = await serveOnce("00000000000000000000000000000001");
    expect(again.output.random).toBe(first.output.random);
    expect(other.output.random).not.toBe(first.output.random);
  });

  it("starts a never-committed agent's turn from initialState when the server polls a null state", async () => {
    const fresh = await serveOnce("8f8d1b06b1618bd08db0300dbba36423", { state: null, base_state_version: 0 });
    expect(fresh.commit.outcome).toBe("completed");
    expect(fresh.commit.state).toEqual({ items: [] });
    // A null state on an agent that has committed is not a fresh agent.
    const committed = await serveOnce("8f8d1b06b1618bd08db0300dbba36423", { state: null, base_state_version: 3 });
    expect(committed.commit.outcome).toBe("failed");
    expect(committed.commit.error).toMatch(/^state does not parse/);
  });

  it("uses the tool's effect id and attempt from the journal", async () => {
    const seen: { effectId: string; attempt: number }[] = [];
    const charge = defineTool({
      name: "pay.charge",
      version: "1",
      input: z.object({ cents: z.number() }),
      output: z.object({ ok: z.boolean() }),
      recovery: "at_least_once",
      run: (_input, ctx) => {
        seen.push({ effectId: ctx.effectId, attempt: ctx.attempt });
        return Promise.resolve({ ok: true });
      },
    });
    const agent = defineAgent({
      name: "order",
      version: "1.0.0",
      state: z.object({ items: z.array(z.unknown()) }),
      initialState: { items: [] },
      messages: (message) => ({
        place: message({
          payload: z.object({ qty: z.number(), sku: z.string() }),
          result: z.object({ ok: z.boolean() }),
          handler: (ctx) => ctx.step("charge", charge, { cents: 649 }),
        }),
      }),
    });
    const lease = { ...pollFixture.assignment.lease, handler_digest: handlerDigest(agent, "place") };
    const assignment = { ...pollFixture.assignment, lease, steps: [] };
    let served = false;
    const reports: TurnReport[] = [];
    const { fetch } = fakeFetch((route) => {
      if (route === "poll") {
        if (served) return { status: 200, body: { assignment: null } };
        served = true;
        return { status: 200, body: { assignment } };
      }
      if (route === "heartbeat") return { status: 200, body: { lease_expires_unix_ns: "1" } };
      return {
        status: 200,
        body: { operation: pollFixture.assignment.operation, status: "completed", state_version: 1, rebased: false, effects: [] },
      };
    });
    const transport = createHttpTransport({ url: "http://ravel.test", token: "t", fetch });
    const worker = await serveAgents({
      transport,
      agents: [agent],
      tools: [charge],
      workerId: "w-explicit",
      pollWaitMs: 1,
      onTurn: (report) => reports.push(report),
    });
    try {
      await until(() => reports.length === 1);
    } finally {
      await worker.close();
    }
    const decoded: TurnAssignment = decodeWire(WorkerPollResponse, { assignment }).assignment ?? fail();
    expect(seen).toEqual([{ effectId: effectId(decoded.lease.incarnation, decoded.accept_seq, "charge"), attempt: 1 }]);
    expect((await transport.journal.effects(decoded.operation)).map((e) => e.status)).toEqual(["completed"]);
  });

  it("heartbeats once before each step and sends nothing while a tool runs", async () => {
    const held = gate();
    let running = 0;
    const tool = (name: string, hold: boolean) =>
      defineTool({
        name,
        version: "1",
        input: z.object({}),
        output: z.object({ ok: z.boolean() }),
        recovery: "at_least_once",
        run: async () => {
          running += 1;
          if (hold) await held.wait;
          return { ok: true };
        },
      });
    const reserve = tool("stock.reserve", true);
    const charge = tool("pay.charge", false);
    const agent = defineAgent({
      name: "order",
      version: "1.0.0",
      state: z.object({ items: z.array(z.unknown()) }),
      initialState: { items: [] },
      messages: (message) => ({
        place: message({
          payload: z.object({ qty: z.number(), sku: z.string() }),
          result: z.object({ ok: z.boolean() }),
          handler: async (ctx) => {
            await ctx.step("reserve", reserve, {});
            return ctx.step("charge", charge, {});
          },
        }),
      }),
    });
    const lease = { ...pollFixture.assignment.lease, handler_digest: handlerDigest(agent, "place") };
    const assignment = { ...pollFixture.assignment, lease, steps: [] };
    let served = false;
    const reports: TurnReport[] = [];
    const { fetch, sent } = fakeFetch((route) => {
      if (route === "poll") {
        if (served) return { status: 200, body: { assignment: null } };
        served = true;
        return { status: 200, body: { assignment } };
      }
      if (route === "heartbeat") return { status: 200, body: { lease_expires_unix_ns: "1" } };
      return {
        status: 200,
        body: { operation: pollFixture.assignment.operation, status: "completed", state_version: 1, rebased: false, effects: [] },
      };
    });
    const routes = () => sent.map((s) => s.url.slice(s.url.lastIndexOf("/") + 1));
    const worker = await serveAgents({
      transport: createHttpTransport({ url: "http://ravel.test", token: "t", fetch }),
      agents: [agent],
      workerId: "w-explicit",
      pollWaitMs: 1,
      onTurn: (report) => reports.push(report),
    });
    try {
      await until(() => running === 1);
      const atStart = routes();
      await new Promise((resolve) => setTimeout(resolve, 200));
      expect(atStart).toEqual(["poll", "heartbeat"]);
      expect(routes()).toEqual(atStart);
      held.open();
      await until(() => reports.length === 1);
    } finally {
      await worker.close();
    }
    expect(reports[0]?.outcome).toBe("committed");
    expect(routes().slice(0, 4)).toEqual(["poll", "heartbeat", "heartbeat", "commit"]);
  });
});
