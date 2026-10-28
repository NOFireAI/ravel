import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { OperationFailed } from "../client/index.js";
import { EffectInDoubt, IncompatibleHandler } from "../errors.js";
import { effectId } from "../identity.js";
import { gate, until } from "../test-util.js";
import { createTestHarness, type TestHarness } from "../testing/index.js";
import { defineTool } from "../tools/index.js";
import type { ToolContext } from "../types.js";
import { ConcurrentStepError, defineAgent, message } from "./index.js";

let harness: TestHarness | undefined;

afterEach(async () => {
  await harness?.close();
  harness = undefined;
});

const echo = defineTool({
  name: "echo",
  version: "1",
  input: z.object({ n: z.number() }),
  output: z.object({ n: z.number() }),
  recovery: "at_least_once",
  run: (input) => Promise.resolve({ n: input.n }),
});

describe("defineTool", () => {
  it("refuses a recovery policy outside the three", () => {
    expect(() =>
      defineTool({
        name: "bad",
        version: "1",
        input: z.object({}),
        output: z.object({}),
        // @ts-expect-error recovery is one of the three ADR-3331 policies
        recovery: "exactly_once",
        run: () => Promise.resolve({}),
      }),
    ).toThrow(TypeError);
  });
});

describe("steps", () => {
  it("a second step started while one is in flight is refused with ConcurrentStepError", async () => {
    const slow = gate();
    const slowTool = defineTool({
      name: "slow",
      version: "1",
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      recovery: "at_least_once",
      run: async () => {
        await slow.wait;
        return { ok: true };
      },
    });
    const agent = defineAgent({
      name: "racer",
      version: "1",
      state: z.object({}),
      initialState: {},
      messages: {
        race: message({
          payload: z.object({}),
          result: z.object({ refused: z.string(), first: z.boolean() }),
          handler: async (ctx) => {
            const first = ctx.step("a", slowTool, {});
            const second = ctx.step("b", slowTool, {});
            const refused = await second.then(
              () => "ran",
              (error: unknown) => (error instanceof ConcurrentStepError ? error.inFlightKey : "other"),
            );
            slow.open();
            return { refused, first: (await first).ok };
          },
        }),
      },
    });
    harness = createTestHarness({ agents: [agent], tools: [slowTool] });
    const op = await harness.agent(agent, "r-1").send("race", {});
    expect(await op.result()).toEqual({ refused: "a", first: true });
    // Only the first step was journaled; the refused one never reached the coordinator.
    expect(harness.coordinator.steps(op.operationId ?? "").map((s) => s.step_key)).toEqual(["a"]);
    expect(harness.coordinator.commits).toHaveLength(1);
  });

  it("sequential steps journal in order and the turn commits once", async () => {
    const agent = defineAgent({
      name: "seq",
      version: "1",
      state: z.object({ sum: z.number() }),
      initialState: { sum: 0 },
      messages: (message) => ({
        add: message({
          payload: z.object({ a: z.number(), b: z.number() }),
          result: z.number(),
          handler: async (ctx, payload) => {
            const a = await ctx.step("a", echo, { n: payload.a });
            const b = await ctx.step("b", echo, { n: payload.b });
            ctx.setState({ sum: a.n + b.n });
            return a.n + b.n;
          },
        }),
      }),
    });
    harness = createTestHarness({ agents: [agent], tools: [echo] });
    const op = await harness.agent(agent, "s-1").send("add", { a: 2, b: 3 });
    expect(await op.result()).toBe(5);
    const steps = harness.coordinator.steps(op.operationId ?? "");
    expect(steps.map((s) => [s.step_key, s.status, s.output])).toEqual([
      ["a", "completed", { n: 2 }],
      ["b", "completed", { n: 3 }],
    ]);
    expect(harness.coordinator.commits).toHaveLength(1);
  });

  it("a tool receives the effect id, attempt and signal", async () => {
    const seen: ToolContext[] = [];
    const probe = defineTool({
      name: "probe",
      version: "1",
      input: z.object({}),
      output: z.object({}),
      recovery: "provider_idempotency",
      run: (_input, effect) => {
        seen.push(effect);
        return Promise.resolve({});
      },
    });
    const agent = defineAgent({
      name: "prober",
      version: "1",
      state: z.object({}),
      initialState: {},
      messages: {
        go: message({
          payload: z.object({}),
          result: z.null(),
          handler: async (ctx) => {
            await ctx.step("charge", probe, {});
            return null;
          },
        }),
      },
    });
    harness = createTestHarness({ agents: [agent], tools: [probe] });
    await harness.agent(agent, "p-1").call("go", {});
    expect(seen).toHaveLength(1);
    expect(seen[0]?.effectId).toBe(effectId(1, 1, "charge"));
    expect(seen[0]?.attempt).toBe(1);
    expect(seen[0]?.signal).toBeInstanceOf(AbortSignal);
    expect(seen[0]?.signal.aborted).toBe(false);
  });

  it("an at_least_once tool interrupted by a crash runs again under the same effect id", async () => {
    const first = gate();
    const attempts: ToolContext[] = [];
    const flaky = defineTool({
      name: "flaky",
      version: "1",
      input: z.object({}),
      output: z.object({ attempt: z.number() }),
      recovery: "at_least_once",
      run: async (_input, effect) => {
        attempts.push(effect);
        if (effect.attempt === 1) await first.wait;
        return { attempt: effect.attempt };
      },
    });
    const agent = defineAgent({
      name: "flaky-agent",
      version: "1",
      state: z.object({}),
      initialState: {},
      messages: {
        go: message({
          payload: z.object({}),
          result: z.number(),
          handler: async (ctx) => (await ctx.step("call", flaky, {})).attempt,
        }),
      },
    });
    harness = createTestHarness({ agents: [agent], tools: [flaky] });
    const op = await harness.agent(agent, "f-1").send("go", {});
    await until(() => attempts.length === 1);
    await harness.crashWorker();
    expect(await op.result()).toBe(2);
    expect(attempts.map((a) => [a.effectId, a.attempt])).toEqual([
      [effectId(1, 1, "call"), 1],
      [effectId(1, 1, "call"), 2],
    ]);
    // The first attempt's run saw its turn abort.
    expect(attempts[0]?.signal.aborted).toBe(true);
    first.open();
  });

  it("a manual tool interrupted by a crash blocks the operation with EffectInDoubt", async () => {
    const hang = gate();
    let runs = 0;
    const transfer = defineTool({
      name: "bank.transfer",
      version: "1",
      input: z.object({}),
      output: z.object({}),
      recovery: "manual",
      run: async () => {
        runs += 1;
        await hang.wait;
        return {};
      },
    });
    const agent = defineAgent({
      name: "payer",
      version: "1",
      state: z.object({}),
      initialState: {},
      messages: {
        pay: message({
          payload: z.object({}),
          result: z.null(),
          handler: async (ctx) => {
            await ctx.step("transfer", transfer, {});
            return null;
          },
        }),
      },
    });
    harness = createTestHarness({ agents: [agent], tools: [transfer] });
    const op = await harness.agent(agent, "m-1").send("pay", {});
    await until(() => runs === 1);
    await harness.crashWorker();
    await until(() => harness?.turns.length === 2);
    const report = harness.turns[1];
    const error = report?.outcome === "abandoned" ? report.error : undefined;
    expect(error).toBeInstanceOf(EffectInDoubt);
    expect(runs).toBe(1);
    const info = await op.status();
    expect(info.status).toBe("blocked");
    expect(info.blockedEffect).toBe(effectId(1, 1, "transfer"));
    hang.open();
  });
});

describe("turns", () => {
  const hold = { gate: gate(), draft: -1 };
  const counter = defineAgent({
    name: "counter",
    version: "1",
    state: z.object({ total: z.number() }),
    initialState: { total: 0 },
    messages: (message) => ({
      add: message({
        payload: z.object({ by: z.number() }),
        result: z.number(),
        handler: async (ctx, payload) => {
          ctx.state.total += payload.by;
          ctx.events.emit("added", { by: payload.by });
          hold.draft = ctx.state.total;
          await hold.gate.wait;
          return ctx.state.total;
        },
      }),
      boom: message({
        payload: z.object({ by: z.number() }),
        result: z.number(),
        handler: (ctx, payload) => {
          ctx.setState({ total: ctx.state.total + payload.by });
          ctx.events.emit("added", { by: payload.by });
          return Promise.reject(new Error("boom"));
        },
      }),
    }),
  });

  it("state is a draft published only in the turn's commit", async () => {
    hold.gate = gate();
    harness = createTestHarness({ agents: [counter] });
    const agent = harness.agent(counter, "c-1");
    const op = await agent.send("add", { by: 3 });
    await until(() => hold.draft === 3);
    expect((await op.status()).status).toBe("running");
    // The handler already mutated its draft; the published state has not moved.
    expect(await agent.state()).toMatchObject({ stateVersion: 0, state: { total: 0 } });
    // So have the events it emitted: none are readable yet.
    const page = await harness.coordinator.fetch("http://fake.invalid/api/v1/agents/events", {
      method: "POST",
      body: JSON.stringify({ agent: { kind: "counter", key: "c-1" }, cursor: null, limit: 10 }),
    });
    expect(((await page.json()) as { events: unknown[] }).events).toEqual([]);
    hold.gate.open();
    expect(await op.result()).toBe(3);
    expect(await agent.state()).toMatchObject({ stateVersion: 1, state: { total: 3 } });
    const events = [];
    for await (const event of op.events()) events.push([event.eventType, event.payload]);
    expect(events).toEqual([["added", { by: 3 }]]);
  });

  it("a failing handler publishes neither state nor events, and dead-letters after its attempts", async () => {
    harness = createTestHarness({ agents: [counter], maxAttempts: 3 });
    const agent = harness.agent(counter, "c-2");
    const op = await agent.send("boom", { by: 5 });
    const error: unknown = await op.result().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(OperationFailed);
    expect(error).toMatchObject({ status: "failed", reason: "boom", deadLettered: true });
    expect(harness.coordinator.commits.map((c) => c.outcome)).toEqual(["failed", "failed", "failed"]);
    // Each attempt ran under its own turn id.
    expect(new Set(harness.coordinator.commits.map((c) => c.lease.turn_id)).size).toBe(3);
    expect(await agent.state()).toMatchObject({ stateVersion: 0, state: { total: 0 } });
    const events = [];
    for await (const event of op.events()) events.push(event);
    expect(events).toEqual([]);
  });

  it("a payload that does not parse fails the operation terminally", async () => {
    harness = createTestHarness({ agents: [counter] });
    const raw = harness.client.agent<never>("counter", "c-3") as unknown as {
      send(h: string, p: unknown): Promise<{ result(): Promise<unknown> }>;
    };
    const op = await raw.send("add", { by: "x" });
    const error: unknown = await op.result().catch((e: unknown) => e);
    expect(error).toMatchObject({ status: "failed", deadLettered: false });
    expect(harness.coordinator.commits).toHaveLength(1);
  });

  it("a worker without the leased handler version abandons the turn with IncompatibleHandler", async () => {
    const { runTurn } = await import("./runner.js");
    harness = createTestHarness({ agents: [] });
    const report = await runTurn(
      {
        startedUnixNs: 0n,
        assignment: {
          lease: {
            agent_id: "a",
            incarnation: 1,
            owner_id: "w",
            epoch: 1,
            turn_id: "t",
            base_state_version: 0,
            handler_version: "2",
            handler_digest: "d",
            observed_root_revision: 0,
          },
          agent: { kind: "counter", key: "c-1" },
          operation: "op-1",
          accept_seq: 1,
          handler: "add",
          state: { total: 0 },
          payload: { by: 1 },
          steps: [],
        },
      },
      new Map([["counter", counter]]),
      harness.coordinator,
      new AbortController().signal,
    );
    expect(report.outcome).toBe("abandoned");
    expect(report.outcome === "abandoned" ? report.error : undefined).toBeInstanceOf(IncompatibleHandler);
  });
});
