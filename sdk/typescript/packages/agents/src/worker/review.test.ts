import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { createClient, OperationFailed } from "../client/index.js";
import { AgentGone, EffectInDoubt, NonDeterministicReplay } from "../errors.js";
import { gate, until } from "../test-util.js";
import { createTestHarness, FakeCoordinator, type TestHarness } from "../testing/index.js";
import { defineTool } from "../tools/index.js";
import { defineAgent, handlerDigest, serveAgents, StepFailed, type Transport, type TurnReport } from "./index.js";

let harness: TestHarness | undefined;
const cleanups: (() => void)[] = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) cleanup();
  await harness?.close();
  harness = undefined;
});

const flaky = defineTool({
  name: "flaky",
  version: "1",
  input: z.object({}),
  output: z.object({}),
  recovery: "at_least_once",
  run: () => Promise.reject(new Error("ECONNRESET")),
});

describe("step runner review reproductions (#3340)", () => {
  it("P1 a manual tool completed by a dead worker after takeover is not run again", async () => {
    let runs = 0;
    let entries = 0;
    const toolGate = gate();
    const pre = gate();
    cleanups.push(() => {
      toolGate.open();
      pre.open();
    });
    const manual = defineTool({
      name: "pay",
      version: "1",
      input: z.object({}),
      output: z.object({ v: z.number() }),
      recovery: "manual",
      run: async () => {
        runs += 1;
        const mine = runs;
        if (mine === 1) await toolGate.wait;
        return { v: mine };
      },
    });
    const agent = defineAgent({
      name: "m",
      version: "1",
      state: z.object({}),
      initialState: {},
      messages: (message) => ({
        go: message({
          payload: z.object({}),
          result: z.number(),
          handler: async (ctx) => {
            entries += 1;
            if (entries === 2) await pre.wait;
            return (await ctx.step("k", manual, {})).v;
          },
        }),
      }),
    });
    const h = (harness = createTestHarness({ agents: [agent], tools: [manual] }));
    const op = await h.agent(agent, "m-1").send("go", {});
    await until(() => runs === 1);
    await h.crashWorker();
    await until(() => entries === 2);
    toolGate.open();
    await until(() => h.coordinator.steps(op.operationId ?? "")[0]?.status === "completed");
    pre.open();
    // The new worker's transcript said `started`; the journal's result replays.
    expect(await op.result({ deadlineMs: 1000 })).toBe(1);
    expect(runs).toBe(1);
  });

  it("P2 events resume with a smaller page size yields no duplicates", async () => {
    const ticker = defineAgent({
      name: "ticker",
      version: "1",
      state: z.object({}),
      initialState: {},
      messages: (message) => ({
        tick: message({
          payload: z.object({ n: z.number() }),
          result: z.number(),
          handler: (ctx, p) => {
            for (let i = 1; i <= p.n; i += 1) ctx.events.emit("tick", { i });
            return Promise.resolve(p.n);
          },
        }),
      }),
    });
    const h = (harness = createTestHarness({ agents: [ticker] }));
    const op = await h.agent(ticker, "t-1").send("tick", { n: 5 });
    await op.result();
    const seen: number[] = [];
    let cursor = "";
    for await (const e of op.events({ pageSize: 5 })) {
      seen.push(e.eventSeq);
      cursor = e.cursor;
      if (seen.length === 3) break;
    }
    for await (const e of op.events({ pageSize: 2, after: cursor })) seen.push(e.eventSeq);
    expect(seen).toEqual([1, 2, 3, 4, 5]);
  });

  it("P3 a tool version bump under a reused step key fails NonDeterministicReplay", async () => {
    let useV2 = false;
    let held = 0;
    const hold = gate();
    cleanups.push(() => {
      hold.open();
    });
    const tool = (version: string) =>
      defineTool({
        name: "t",
        version,
        input: z.object({}),
        output: z.object({ v: z.string() }),
        recovery: "at_least_once",
        run: () => Promise.resolve({ v: version }),
      });
    const t1 = tool("1");
    const t2 = tool("2");
    const agent = defineAgent({
      name: "v",
      version: "1",
      state: z.object({}),
      initialState: {},
      messages: (message) => ({
        go: message({
          payload: z.object({}),
          result: z.string(),
          handler: async (ctx) => {
            const r = await ctx.step("k", useV2 ? t2 : t1, {});
            held += 1;
            await hold.wait;
            return r.v;
          },
        }),
      }),
    });
    const h = (harness = createTestHarness({ agents: [agent], tools: [t1, t2] }));
    await h.agent(agent, "v-1").send("go", {});
    await until(() => held === 1);
    useV2 = true;
    await h.crashWorker();
    await until(() => h.turns.length === 2 || held === 2);
    expect(held).toBe(1);
    expect(h.turns).toHaveLength(2);
    expect(h.turns[1]).toMatchObject({ outcome: "abandoned" });
    expect((h.turns[1] as { error: unknown }).error).toBeInstanceOf(NonDeterministicReplay);
    expect(h.coordinator.commits).toHaveLength(0);
  });

  it("P3b a journaled step whose tool version differs under the same input hash is refused by the runner itself", async () => {
    // The input hash already covers the tool version, so P3 above would
    // pass through the hash alone. This pins the explicit version check:
    // the first worker journals and completes the step, then a second
    // worker is handed that transcript with the entry's tool version
    // changed and its input hash left as journaled, so only the version
    // disagrees. Without the check the runner replays the record and the
    // turn commits.
    let runs = 0;
    let held = 0;
    const hold = gate();
    cleanups.push(() => {
      hold.open();
    });
    const tool = defineTool({
      name: "t",
      version: "1",
      input: z.object({}),
      output: z.object({ v: z.string() }),
      recovery: "at_least_once",
      run: () => {
        runs += 1;
        return Promise.resolve({ v: "1" });
      },
    });
    const agent = defineAgent({
      name: "v",
      version: "1",
      state: z.object({}),
      initialState: {},
      messages: (message) => ({
        go: message({
          payload: z.object({}),
          result: z.string(),
          handler: async (ctx) => {
            const r = await ctx.step("k", tool, {});
            held += 1;
            await hold.wait;
            return r.v;
          },
        }),
      }),
    });
    const coordinator = new FakeCoordinator({ agents: [agent] });
    cleanups.push(() => {
      coordinator.close();
    });
    const client = createClient({ url: "http://fake.invalid", token: "t", fetch: coordinator.fetch });
    const op = await client.agent<typeof agent>("v", "v-1").send("go", {});
    const first = await serveAgents({ transport: coordinator, agents: [agent], tools: [tool], pollWaitMs: 50 });
    await until(() => held === 1);
    coordinator.revokeLeases();
    await first.close();
    expect(runs).toBe(1);

    const reports: TurnReport[] = [];
    const transport: Transport = {
      register: (request) => coordinator.register(request),
      heartbeat: (request) => coordinator.heartbeat(request),
      poll: async (request, signal) => {
        const polled = await coordinator.poll(request, signal);
        if (polled === null) return null;
        const steps = polled.assignment.steps.map((s) => (s.step_key === "k" ? { ...s, tool_version: "2" } : s));
        return { ...polled, assignment: { ...polled.assignment, steps } };
      },
      recordStep: (request) => coordinator.recordStep(request),
      failEffect: (request) => coordinator.failEffect(request),
      commit: (request) => coordinator.commit(request),
      completeEffect: (request) => coordinator.completeEffect(request),
    };
    const second = await serveAgents({
      transport,
      agents: [agent],
      tools: [tool],
      pollWaitMs: 50,
      onTurn: (report) => reports.push(report),
    });
    try {
      await until(() => reports.length === 1);
      expect(reports[0]).toMatchObject({ outcome: "abandoned" });
      expect((reports[0] as { error: unknown }).error).toBeInstanceOf(NonDeterministicReplay);
      expect(runs).toBe(1);
      expect(held).toBe(1);
      expect(coordinator.commits).toHaveLength(0);
      // The fake's own journal still agrees with the runner's request, so
      // it does not block the operation; the runner refused on its own.
      expect((await op.status()).status).toBe("running");
    } finally {
      await second.close();
    }
  });

  it("P9 a tool that throws a typed RavelAgentsError does not wedge the operation", async () => {
    const tool = defineTool({
      name: "nested",
      version: "1",
      input: z.object({}),
      output: z.object({}),
      recovery: "at_least_once",
      run: () => Promise.reject(new AgentGone("other agent deleted")),
    });
    const agent = defineAgent({
      name: "w",
      version: "1",
      state: z.object({}),
      initialState: {},
      messages: (message) => ({
        go: message({
          payload: z.object({}),
          result: z.string(),
          handler: async (ctx) => {
            try {
              await ctx.step("k", tool, {});
            } catch {
              return "handled";
            }
            return "unreached";
          },
        }),
      }),
    });
    const h = (harness = createTestHarness({ agents: [agent], tools: [tool], maxAttempts: 1 }));
    const op = await h.agent(agent, "w-1").send("go", {});
    const res = await op.result({ deadlineMs: 500 }).then(String, (e: unknown) => (e as Error).name);
    expect(res).toBe("handled");
  });

  it("P10 a different Date under the same step key is detected", async () => {
    let at = new Date(Date.UTC(2026, 0, 1));
    let held = 0;
    const hold = gate();
    cleanups.push(() => {
      hold.open();
    });
    const tool = defineTool({
      name: "book",
      version: "1",
      input: z.object({ at: z.date() }),
      output: z.object({ at: z.string() }),
      recovery: "at_least_once",
      run: (i) => Promise.resolve({ at: i.at.toISOString() }),
    });
    const agent = defineAgent({
      name: "dt",
      version: "1",
      state: z.object({}),
      initialState: {},
      messages: (message) => ({
        go: message({
          payload: z.object({}),
          result: z.string(),
          handler: async (ctx) => {
            const r = await ctx.step("k", tool, { at });
            held += 1;
            await hold.wait;
            return r.at;
          },
        }),
      }),
    });
    const h = (harness = createTestHarness({ agents: [agent], tools: [tool] }));
    await h.agent(agent, "dt-1").send("go", {});
    await until(() => held === 1);
    at = new Date(Date.UTC(2027, 5, 5));
    await h.crashWorker();
    await until(() => h.turns.length === 2 || held === 2);
    expect(held).toBe(1);
    expect(h.turns).toHaveLength(2);
    expect(h.turns[1]).toMatchObject({ outcome: "abandoned" });
    expect((h.turns[1] as { error: unknown }).error).toBeInstanceOf(NonDeterministicReplay);
    expect(h.coordinator.commits).toHaveLength(0);
  });

  it("P11 a tool failure is journaled as the step's result before the failed commit, and replayed", async () => {
    let runs = 0;
    const seen: unknown[] = [];
    const counted = defineTool({
      ...flaky,
      run: () => {
        runs += 1;
        return Promise.reject(new Error("ECONNRESET"));
      },
    });
    const agent = defineAgent({
      name: "f",
      version: "1",
      state: z.object({}),
      initialState: {},
      messages: (message) => ({
        go: message({
          payload: z.object({}),
          result: z.string(),
          handler: async (ctx) => {
            try {
              await ctx.step("k", counted, {});
            } catch (error) {
              seen.push(error);
              throw error;
            }
            return "x";
          },
        }),
      }),
    });
    const h = (harness = createTestHarness({ agents: [agent], tools: [counted], maxAttempts: 3 }));
    const op = await h.agent(agent, "f-1").send("go", {});
    const error: unknown = await op.result({ deadlineMs: 1000 }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(OperationFailed);
    expect(error).toMatchObject({ reason: "ECONNRESET", deadLettered: true });
    expect(h.coordinator.steps(op.operationId ?? "")).toMatchObject([
      { step_key: "k", status: "failed", error: "ECONNRESET" },
    ]);
    // The retries were answered with the recorded failure, not new attempts.
    expect(runs).toBe(1);
    expect(seen).toHaveLength(3);
    for (const error of seen) {
      expect(error).toBeInstanceOf(StepFailed);
      expect(error).toMatchObject({ stepKey: "k", error: "ECONNRESET" });
    }
  });

  it("P11 the fake refuses FailTurn while a step's attempt is authorized without a result", async () => {
    const agent = defineAgent({
      name: "f",
      version: "1",
      state: z.object({}),
      initialState: {},
      messages: (message) => ({
        go: message({ payload: z.object({}), result: z.string(), handler: () => Promise.resolve("x") }),
      }),
    });
    const coordinator = new FakeCoordinator({ agents: [agent] });
    cleanups.push(() => {
      coordinator.close();
    });
    const client = createClient({ url: "http://fake.invalid", token: "t", fetch: coordinator.fetch });
    const op = await client.agent<typeof agent>("f", "f-1").send("go", {});
    const polled = await coordinator.poll(
      { worker_id: "w", handlers: [{ handler: "go", version: "1", digest: handlerDigest(agent, "go") }], wait_ms: 0 },
      new AbortController().signal,
    );
    if (polled === null) throw new Error("no turn leased");
    const lease = polled.assignment.lease;
    const started = await coordinator.recordStep({
      lease,
      step_key: "k",
      tool_name: "flaky",
      tool_version: "1",
      policy: "at_least_once",
      input_hash: "00",
    });
    const fail = { lease, outcome: "failed", error: "ECONNRESET", terminal: false } as const;

    const refused: unknown = await coordinator.commit(fail).catch((e: unknown) => e);
    expect(refused).toBeInstanceOf(EffectInDoubt);
    expect((refused as EffectInDoubt).message).toContain("NoAuthorizedAttemptPending");
    expect((refused as EffectInDoubt).ids.effectId).toBe(started.effect);
    expect(coordinator.commits).toHaveLength(0);

    await coordinator.failEffect({ effect: started.effect, attempt: started.attempt, error: "ECONNRESET" });
    await coordinator.commit(fail);
    expect(coordinator.commits.map((c) => c.outcome)).toEqual(["failed"]);

    // A late result is kept as evidence and leaves the journal as it was.
    const late = await coordinator.completeEffect({ effect: started.effect, attempt: started.attempt, result: {} });
    expect(late.late).toBe(true);
    expect(coordinator.evidence).toEqual([
      { effect: started.effect, attempt: started.attempt, result: { output: {} } },
    ]);
    expect(coordinator.steps(op.operationId ?? "")).toMatchObject([{ status: "failed", error: "ECONNRESET" }]);
  });
});
