/**
 * The order agent against `@ravel/agents/testing`: an in-process fake
 * coordinator, so these tests need no server, no network and no API key.
 */
import { readFileSync } from "node:fs";
import { generateContract } from "@ravel/agents/contract";
import { createTestHarness, type TestHarness } from "@ravel/agents/testing";
import { afterEach, describe, expect, it } from "vitest";
import { createOrderAgent } from "./agent.js";
import { orderClient } from "./client.js";
import { PriceList } from "./fixture.js";

let harness: TestHarness | undefined;

function start(prices: PriceList): TestHarness {
  const { order, tools } = createOrderAgent(prices);
  harness = createTestHarness({ agents: [order], tools });
  return harness;
}

/** Resolve once `condition` holds; reject after `ms`. */
async function until(condition: () => boolean, ms = 5_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("condition not reached in time");
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

afterEach(async () => {
  await harness?.close();
  harness = undefined;
});

describe("order agent", () => {
  it("checkout prices the line through the price step and adds it to the total", async () => {
    const prices = new PriceList();
    const h = start(prices);
    const orders = orderClient(h.client, "o-1");

    expect(await orders.call("checkout", { sku: "A-1", qty: 2 })).toEqual({
      sku: "A-1",
      qty: 2,
      lineCents: 500,
      totalCents: 500,
    });
    expect(await orders.call("checkout", { sku: "C-3", qty: 4 })).toMatchObject({ lineCents: 300, totalCents: 800 });
    expect(await orders.call("summary", {})).toEqual({ totalCents: 800, lines: 2 });
    expect(prices.lookups).toEqual(["A-1", "C-3"]);
  });

  it("a SKU not on the price list adds nothing to the total", async () => {
    const h = start(new PriceList());
    const orders = orderClient(h.client, "o-2");

    expect(await orders.call("checkout", { sku: "Z-9", qty: 1 })).toEqual({
      sku: "Z-9",
      qty: 1,
      lineCents: null,
      totalCents: 0,
    });
    expect(await orders.call("summary", {})).toEqual({ totalCents: 0, lines: 0 });
  });

  it("a worker killed inside the price step is replaced and the turn commits once", async () => {
    const prices = new PriceList();
    let release: () => void = () => undefined;
    prices.hold = new Promise<void>((resolve) => {
      release = resolve;
    });
    const h = start(prices);
    const handle = await orderClient(h.client, "o-3").send("checkout", { sku: "B-2", qty: 1 });
    await until(() => prices.lookups.length === 1);

    // Kill the worker while its lookup is in flight; the fresh worker runs the
    // at_least_once step again and commits the turn.
    prices.hold = undefined;
    await h.crashWorker();
    expect(await handle.result()).toMatchObject({ lineCents: 1200, totalCents: 1200 });
    release();

    expect(prices.lookups).toEqual(["B-2", "B-2"]);
    expect(h.coordinator.commits).toHaveLength(1);
    expect(h.coordinator.commits[0]?.lease.owner_id).toBe("harness-worker-2");
    expect(await orderClient(h.client, "o-3").call("summary", {})).toEqual({ totalCents: 1200, lines: 1 });
  });

  it("contract.ts is the generator's output for the agent", () => {
    const { order } = createOrderAgent(new PriceList());
    const written = readFileSync(new URL("./contract.ts", import.meta.url), "utf8");
    // On a mismatch, run `npm run contract` to regenerate contract.ts.
    expect(written).toBe(generateContract(order));
  });
});
