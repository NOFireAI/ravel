/**
 * The order agent: `checkout` prices one line through the durable step
 * `price` and adds it to the order's running total; `summary` reads the
 * total back. The `catalog.price` tool is `at_least_once`: a lookup is
 * safe to repeat, so a worker killed inside it runs it again.
 */
import { defineTool } from "@ravel/agents/tools";
import { defineAgent } from "@ravel/agents/worker";
import { z } from "zod";
import type { PriceList } from "./fixture.js";

export function createOrderAgent(prices: PriceList) {
  const price = defineTool({
    name: "catalog.price",
    version: "1",
    input: z.object({ sku: z.string() }),
    output: z.object({ cents: z.number().int().nullable() }),
    recovery: "at_least_once",
    run: async (input) => ({ cents: await prices.price(input.sku) }),
  });

  const totals = z.object({ totalCents: z.number().int(), lines: z.number().int() });

  const order = defineAgent({
    name: "order",
    version: "1",
    state: totals,
    initialState: { totalCents: 0, lines: 0 },
    messages: (message) => ({
      checkout: message({
        payload: z.object({ sku: z.string().min(1), qty: z.number().int().positive() }),
        result: z.object({
          sku: z.string(),
          qty: z.number().int(),
          /** Null when the SKU is not on the price list; the total is unchanged. */
          lineCents: z.number().int().nullable(),
          totalCents: z.number().int(),
        }),
        handler: async (ctx, payload) => {
          const quoted = await ctx.step("price", price, { sku: payload.sku });
          if (quoted.cents === null) {
            return { sku: payload.sku, qty: payload.qty, lineCents: null, totalCents: ctx.state.totalCents };
          }
          const lineCents = quoted.cents * payload.qty;
          const next = { totalCents: ctx.state.totalCents + lineCents, lines: ctx.state.lines + 1 };
          ctx.setState(next);
          ctx.events.emit("line_added", { sku: payload.sku, qty: payload.qty, lineCents });
          return { sku: payload.sku, qty: payload.qty, lineCents, totalCents: next.totalCents };
        },
      }),
      summary: message({
        payload: z.object({}),
        result: totals,
        handler: (ctx) => Promise.resolve({ totalCents: ctx.state.totalCents, lines: ctx.state.lines }),
      }),
    }),
  });

  return { order, tools: [price] } as const;
}

export type Order = ReturnType<typeof createOrderAgent>["order"];
