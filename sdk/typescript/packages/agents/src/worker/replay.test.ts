import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { NonDeterministicReplay, OutcomeUnknown } from "../errors.js";
import { gate, type Gate, until } from "../test-util.js";
import { createTestHarness, type TestHarness } from "../testing/index.js";
import { defineTool } from "../tools/index.js";
import { defineAgent } from "./index.js";

/** Per-test knobs the handler and tools read. */
interface Knobs {
  priceRuns: number;
  reserveRuns: number;
  /** How many times the handler reached the hold after both steps. */
  held: number;
  hold: Gate;
  /** Added to the reserve step's quantity: a handler whose input drifted between attempts. */
  drift: number;
  seen: { now: number; random: number }[];
}

let knobs: Knobs;
let harness: TestHarness | undefined;

function fresh(): Knobs {
  return { priceRuns: 0, reserveRuns: 0, held: 0, hold: gate(), drift: 0, seen: [] };
}

const price = defineTool({
  name: "catalog.price",
  version: "1",
  input: z.object({ sku: z.string() }),
  output: z.object({ cents: z.number() }),
  recovery: "at_least_once",
  run: () => {
    knobs.priceRuns += 1;
    return Promise.resolve({ cents: 250 });
  },
});

const reserve = defineTool({
  name: "stock.reserve",
  version: "1",
  input: z.object({ sku: z.string(), qty: z.number() }),
  output: z.object({ reservation: z.string() }),
  recovery: "provider_idempotency",
  run: (input, effect) => {
    knobs.reserveRuns += 1;
    return Promise.resolve({ reservation: `${input.sku}:${String(input.qty)}:${effect.effectId}` });
  },
});

const order = defineAgent({
  name: "order",
  version: "1",
  state: z.object({ total: z.number() }),
  initialState: { total: 0 },
  messages: (message) => ({
    checkout: message({
      payload: z.object({ sku: z.string(), qty: z.number() }),
      result: z.object({ total: z.number(), reservation: z.string() }),
      handler: async (ctx, payload) => {
        knobs.seen.push({ now: ctx.now().getTime(), random: ctx.random() });
        const quoted = await ctx.step("price", price, { sku: payload.sku });
        const reserved = await ctx.step("reserve", reserve, {
          sku: payload.sku,
          qty: payload.qty + knobs.drift,
        });
        knobs.held += 1;
        await knobs.hold.wait;
        const total = quoted.cents * payload.qty;
        ctx.setState({ total: ctx.state.total + total });
        return { total, reservation: reserved.reservation };
      },
    }),
  }),
});

function start(): TestHarness {
  knobs = fresh();
  harness = createTestHarness({ agents: [order], tools: [price, reserve] });
  return harness;
}

afterEach(async () => {
  knobs.hold.open();
  await harness?.close();
  harness = undefined;
});

describe("durable step replay", () => {
  it("replay after crashWorker with two completed steps replays both without calling run and commits once", async () => {
    const h = start();
    const op = await h.agent(order, "o-1").send("checkout", { sku: "A-1", qty: 2 });
    await until(() => knobs.held === 1);
    expect([knobs.priceRuns, knobs.reserveRuns]).toEqual([1, 1]);

    await h.crashWorker();
    await until(() => knobs.held === 2);
    // Both steps came back from the transcript: neither tool ran again.
    expect([knobs.priceRuns, knobs.reserveRuns]).toEqual([1, 1]);
    expect(h.coordinator.commits).toHaveLength(0);

    knobs.hold.open();
    const result = await op.result();
    expect(result.total).toBe(500);
    expect(h.coordinator.commits).toHaveLength(1);
    const [lease] = h.coordinator.commits.map((c) => c.lease);
    expect(lease?.owner_id).toBe("harness-worker-2");
    expect((await h.agent(order, "o-1").state()).state).toEqual({ total: 500 });
  });

  it("reused step key with a different input hash fails NonDeterministicReplay", async () => {
    const h = start();
    const op = await h.agent(order, "o-1").send("checkout", { sku: "A-1", qty: 2 });
    await until(() => knobs.held === 1);

    knobs.drift = 1;
    await h.crashWorker();
    // Either the replay is refused (the turn ends) or it slips through to the hold.
    await until(() => h.turns.length === 2 || knobs.held === 2);
    expect(knobs.held).toBe(1);

    // The first step's input is unchanged: it replayed without running.
    expect(knobs.priceRuns).toBe(1);
    // The second step's key is reused with a different input: refused, not re-run.
    expect(knobs.reserveRuns).toBe(1);
    const report = h.turns[1];
    expect(report?.outcome).toBe("abandoned");
    const error = report?.outcome === "abandoned" ? report.error : undefined;
    expect(error).toBeInstanceOf(NonDeterministicReplay);
    expect((error as NonDeterministicReplay).ids.stepKey).toBe("reserve");
    expect(knobs.held).toBe(1);

    // The turn is blocked: nothing committed, and reads say so.
    expect(h.coordinator.commits).toHaveLength(0);
    expect((await op.status()).status).toBe("blocked");
    await expect(op.result({ deadlineMs: 30 })).rejects.toBeInstanceOf(OutcomeUnknown);
    expect((await h.agent(order, "o-1").state()).stateVersion).toBe(0);
  });

  it("recorded time and seeded random are the same on every attempt at one input", async () => {
    const h = start();
    await h.agent(order, "o-1").send("checkout", { sku: "A-1", qty: 1 });
    await until(() => knobs.held === 1);
    await h.advanceTime(60_000);
    await h.crashWorker();
    await until(() => knobs.held === 2);

    const [first, second] = knobs.seen;
    expect(first?.now).toBe(Date.UTC(2026, 0, 1));
    expect(second).toEqual(first);

    // A different input draws from a different sequence.
    knobs.hold.open();
    await h.agent(order, "o-2").call("checkout", { sku: "A-1", qty: 1 });
    expect(knobs.seen[2]?.random).not.toBe(first?.random);
  });
});
