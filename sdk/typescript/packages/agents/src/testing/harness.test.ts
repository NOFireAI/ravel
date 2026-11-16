import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { AgentGone, EffectInDoubt } from "../errors.js";
import { gate, until } from "../test-util.js";
import { defineTool } from "../tools/index.js";
import type { RecoveryPolicy } from "../types.js";
import { defineAgent } from "../worker/index.js";
import { createTestHarness, type TestHarness } from "./index.js";

let harness: TestHarness | undefined;
afterEach(async () => {
  await harness?.close();
  harness = undefined;
});

/** A tool that holds every run open until `hold` opens, and the agent that calls it in one step. */
function holding(recovery: RecoveryPolicy) {
  const hold = gate();
  const runs = { count: 0 };
  const tool = defineTool({
    name: "hold.run",
    version: "1",
    input: z.object({}),
    output: z.object({}),
    recovery,
    run: async () => {
      runs.count += 1;
      await hold.wait;
      return {};
    },
  });
  const agent = defineAgent({
    name: "holder",
    version: "1",
    state: z.object({ done: z.number() }),
    initialState: { done: 0 },
    messages: (message) => ({
      go: message({
        payload: z.object({}),
        result: z.number(),
        handler: async (ctx) => {
          await ctx.step("run", tool, {});
          ctx.setState({ done: ctx.state.done + 1 });
          return ctx.state.done;
        },
      }),
    }),
  });
  return { hold, runs, tool, agent };
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 50));

describe("test harness limits", () => {
  it("advancing the fake's clock past a lease's expiry neither takes the turn away nor runs anything", async () => {
    const { hold, runs, tool, agent } = holding("at_least_once");
    harness = createTestHarness({ agents: [agent], tools: [tool] });
    const op = await harness.agent(agent, "h-1").send("go", {});
    await until(() => runs.count === 1);

    await harness.advanceTime(60 * 60 * 1000);
    await settle();
    expect(runs.count).toBe(1);
    expect(harness.turns).toHaveLength(0);
    expect(harness.coordinator.commits).toHaveLength(0);
    expect((await op.status()).status).toBe("running");

    hold.open();
    expect(await op.result()).toBe(1);
    expect(harness.coordinator.commits).toHaveLength(1);
    expect(harness.turns.map((t) => t.outcome)).toEqual(["committed"]);
  });

  it("an operation blocked with EffectInDoubt stays blocked: no clock advance or new worker reconciles it", async () => {
    const { hold, runs, tool, agent } = holding("manual");
    harness = createTestHarness({ agents: [agent], tools: [tool] });
    const op = await harness.agent(agent, "h-1").send("go", {});
    await until(() => runs.count === 1);
    await harness.crashWorker();
    await until(() => harness?.turns.length === 2);
    expect((await op.status()).status).toBe("blocked");

    await harness.advanceTime(24 * 60 * 60 * 1000);
    await harness.crashWorker();
    await settle();
    expect(runs.count).toBe(1);
    // The killed worker's own turn, then the replacement's EffectInDoubt; the
    // third worker is never leased the blocked turn.
    const errors = harness.turns.map((t) => (t.outcome === "abandoned" ? t.error : t.outcome));
    expect(errors).toHaveLength(2);
    expect(harness.turns[0]?.outcome).toBe("abandoned");
    expect(errors[1]).toBeInstanceOf(EffectInDoubt);
    expect((await op.status()).status).toBe("blocked");
    expect(harness.coordinator.commits).toHaveLength(0);
    hold.open();
  });

  it("nothing outlives close: a new harness has never seen the first one's agent", async () => {
    const { hold, tool, agent } = holding("at_least_once");
    hold.open();
    const first = createTestHarness({ agents: [agent], tools: [tool] });
    expect(await first.agent(agent, "h-1").call("go", {})).toBe(1);
    expect((await first.agent(agent, "h-1").state()).state).toEqual({ done: 1 });
    await first.close();

    harness = createTestHarness({ agents: [agent], tools: [tool] });
    await expect(harness.agent(agent, "h-1").state()).rejects.toBeInstanceOf(AgentGone);
  });
});
