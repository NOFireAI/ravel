import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { gate, type Gate, until } from "../test-util.js";
import { createTestHarness, type TestHarness } from "../testing/index.js";
import { defineAgent } from "../worker/index.js";
import type { AgentEvent } from "./index.js";

let harness: TestHarness | undefined;
let hold: Gate = gate();

afterEach(async () => {
  hold.open();
  await harness?.close();
  harness = undefined;
});

const ticker = defineAgent({
  name: "ticker",
  version: "1",
  state: z.object({}),
  initialState: {},
  messages: (message) => ({
    tick: message({
      payload: z.object({ n: z.number(), wait: z.boolean() }),
      result: z.number(),
      handler: async (ctx, payload) => {
        if (payload.wait) await hold.wait;
        for (let i = 1; i <= payload.n; i += 1) ctx.events.emit("tick", { i });
        return payload.n;
      },
    }),
  }),
});

function start(): TestHarness {
  hold = gate();
  harness = createTestHarness({ agents: [ticker] });
  return harness;
}

async function collect(events: AsyncIterable<AgentEvent>): Promise<AgentEvent[]> {
  const out: AgentEvent[] = [];
  for await (const event of events) out.push(event);
  return out;
}

describe("operation events", () => {
  it("the iterator ends once the operation is terminal and its events are read", async () => {
    const h = start();
    const agent = h.agent(ticker, "t-1");
    const other = await agent.send("tick", { n: 2, wait: false });
    await other.result();
    const op = await agent.send("tick", { n: 3, wait: true });
    const reading = collect(op.events({ pageSize: 2, pollIntervalMs: 5 }));
    await until(() => h.coordinator.received.filter((r) => r.route === "events").length >= 2);
    hold.open();
    const events = await reading;
    // Only this operation's events, in order, and the loop ended on its own.
    expect(events.map((e) => [e.eventType, e.payload])).toEqual([
      ["tick", { i: 1 }],
      ["tick", { i: 2 }],
      ["tick", { i: 3 }],
    ]);
    expect(events.map((e) => e.eventSeq)).toEqual([3, 4, 5]);
  });

  it("resumes after a cursor, mid-page and at a page boundary", async () => {
    const h = start();
    const op = await h.agent(ticker, "t-1").send("tick", { n: 4, wait: false });
    await op.result();
    const all = await collect(op.events({ pageSize: 3 }));
    expect(all.map((e) => e.eventSeq)).toEqual([1, 2, 3, 4]);

    // After the first event: the cursor points inside the first page.
    const afterFirst = await collect(op.events({ pageSize: 3, after: all[0]?.cursor ?? "" }));
    expect(afterFirst.map((e) => e.eventSeq)).toEqual([2, 3, 4]);
    // After the third: the cursor is the page's own next cursor.
    const afterThird = await collect(op.events({ pageSize: 3, after: all[2]?.cursor ?? "" }));
    expect(afterThird.map((e) => e.eventSeq)).toEqual([4]);
    // After the last: nothing left, and the iterator still ends.
    expect(await collect(op.events({ after: all[3]?.cursor ?? "" }))).toEqual([]);
  });

  it("a broken-off iteration resumes from the cursor of the last event it saw", async () => {
    const h = start();
    const op = await h.agent(ticker, "t-1").send("tick", { n: 3, wait: false });
    await op.result();
    let cursor = "";
    for await (const event of op.events({ pageSize: 10 })) {
      cursor = event.cursor;
      break;
    }
    const rest = await collect(op.events({ after: cursor }));
    expect(rest.map((e) => e.payload)).toEqual([{ i: 2 }, { i: 3 }]);
  });
});
