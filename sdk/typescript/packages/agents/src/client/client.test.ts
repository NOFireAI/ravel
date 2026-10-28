import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { gate, type Gate } from "../test-util.js";
import { createTestHarness, type TestHarness } from "../testing/index.js";
import { ERROR_CODES } from "../wire/errors.js";
import { defineAgent } from "../worker/index.js";
import {
  createClient,
  IdempotencyConflict,
  OperationFailed,
  OutcomeUnknown,
  RavelAgentsError,
  StoreUnavailable,
  UnsupportedFormat,
} from "./index.js";

let harness: TestHarness | undefined;
let hold: Gate = gate();

afterEach(async () => {
  hold.open();
  await harness?.close();
  harness = undefined;
});

const greeter = defineAgent({
  name: "greeter",
  version: "1",
  state: z.object({ count: z.number() }),
  initialState: { count: 0 },
  messages: (message) => ({
    greet: message({
      payload: z.object({ name: z.string() }),
      result: z.string(),
      handler: (ctx, payload) => {
        ctx.setState({ count: ctx.state.count + 1 });
        return Promise.resolve(`hello ${payload.name}`);
      },
    }),
    slow: message({
      payload: z.object({}),
      result: z.string(),
      handler: async () => {
        await hold.wait;
        return "done";
      },
    }),
    fail: message({
      payload: z.object({}),
      result: z.string(),
      handler: () => Promise.reject(new Error("nope")),
    }),
  }),
});

function start(): TestHarness {
  hold = gate();
  harness = createTestHarness({ agents: [greeter], maxAttempts: 1 });
  return harness;
}

describe("client", () => {
  it("send and result round-trip through the fake coordinator", async () => {
    const h = start();
    const op = await h.agent(greeter, "g-1").send("greet", { name: "ada" });
    expect(op.operationId).toMatch(/^op-/);
    expect(await op.result()).toBe("hello ada");
    expect(await h.agent(greeter, "g-1").call("greet", { name: "bo" })).toBe("hello bo");
  });

  it("a retry after a transient 503 reuses the same request id and hash", async () => {
    const h = start();
    h.coordinator.failNext("send", { status: 503, code: "store_unavailable", times: 2 });
    const op = await h.agent(greeter, "g-1").send("greet", { name: "ada" });
    const sends = h.coordinator.received.filter((r) => r.route === "send");
    expect(sends).toHaveLength(3);
    expect(new Set(sends.map((r) => r.requestId)).size).toBe(1);
    expect(new Set(sends.map((r) => r.requestHash)).size).toBe(1);
    expect(sends[0]?.requestId).toBe(op.requestId);
    expect(await op.result()).toBe("hello ada");
  });

  it("an untyped 5xx is retried the same way", async () => {
    const h = start();
    h.coordinator.failNext("send", { status: 502 });
    const op = await h.agent(greeter, "g-1").send("greet", { name: "ada" });
    const sends = h.coordinator.received.filter((r) => r.route === "send");
    expect(sends.map((r) => r.requestId)).toEqual([op.requestId, op.requestId]);
  });

  it("retries are bounded: an exhausted store_unavailable surfaces as StoreUnavailable", async () => {
    const h = start();
    h.coordinator.failNext("send", { status: 503, code: "store_unavailable", times: 10 });
    await expect(h.agent(greeter, "g-1").send("greet", { name: "ada" })).rejects.toBeInstanceOf(StoreUnavailable);
    // One attempt plus the default three retries.
    expect(h.coordinator.received.filter((r) => r.route === "send")).toHaveLength(4);
  });

  it("the same request id with a different payload fails IdempotencyConflict", async () => {
    const h = start();
    const agent = h.agent(greeter, "g-1");
    const first = await agent.send("greet", { name: "ada" }, { requestId: "r-1" });
    const again = await agent.send("greet", { name: "ada" }, { requestId: "r-1" });
    expect(again.operationId).toBe(first.operationId);
    const error: unknown = await agent.send("greet", { name: "bo" }, { requestId: "r-1" }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(IdempotencyConflict);
    expect((error as IdempotencyConflict).retry).toBe("never");
    // Not retried: a conflict is final.
    expect(h.coordinator.received.filter((r) => r.route === "send")).toHaveLength(3);
  });

  it("an aborted result() wait leaves the operation running", async () => {
    const h = start();
    const op = await h.agent(greeter, "g-1").send("slow", {});
    const controller = new AbortController();
    const waiting = op.result({ signal: controller.signal });
    controller.abort(new Error("caller gave up"));
    await expect(waiting).rejects.toThrow("caller gave up");
    expect((await op.status()).status).toBe("running");
    hold.open();
    expect(await op.result()).toBe("done");
  });

  it("a result() deadline throws OutcomeUnknown carrying the handle", async () => {
    const h = start();
    const op = await h.agent(greeter, "g-1").send("slow", {});
    const error: unknown = await op.result({ deadlineMs: 20 }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(OutcomeUnknown);
    expect((error as OutcomeUnknown).handle).toBe(op);
    expect((error as OutcomeUnknown).retry).toBe("resolve");
    hold.open();
    expect(await op.result()).toBe("done");
  });

  it("a send whose response misses its deadline resolves to the operation it created", async () => {
    const h = start();
    h.coordinator.delayNext("send", 1_000);
    const error: unknown = await h
      .agent(greeter, "g-1")
      .send("greet", { name: "ada" }, { deadlineMs: 20 })
      .catch((e: unknown) => e);
    expect(error).toBeInstanceOf(OutcomeUnknown);
    const handle = (error as OutcomeUnknown<{ result(): Promise<string>; operationId: string | undefined }>).handle;
    expect(handle?.operationId).toBeUndefined();
    expect(await handle?.result()).toBe("hello ada");
    // The request had landed: resolve found it and nothing was sent twice.
    expect(h.coordinator.received.filter((r) => r.route === "send")).toHaveLength(1);
    expect(h.coordinator.received.filter((r) => r.route === "resolve")).toHaveLength(1);
  });

  it("a send that never landed is resent under the same request id once resolve finds nothing", async () => {
    const h = start();
    h.coordinator.failNext("send", { status: 502, times: 4 });
    const error: unknown = await h.agent(greeter, "g-1").send("greet", { name: "ada" }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(OutcomeUnknown);
    const handle = (error as OutcomeUnknown<{ result(): Promise<string>; requestId: string }>).handle;
    expect(await handle?.result()).toBe("hello ada");
    const sends = h.coordinator.received.filter((r) => r.route === "send");
    expect(sends).toHaveLength(5);
    expect(new Set(sends.map((r) => r.requestId))).toEqual(new Set([handle?.requestId]));
  });

  it("a failed operation rejects result() and call() with OperationFailed", async () => {
    const h = start();
    const agent = h.agent(greeter, "g-1");
    const error: unknown = await agent.call("fail", {}).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(OperationFailed);
    expect(error).toMatchObject({ status: "failed", reason: "nope" });
  });

  it("cancel is idempotent and a cancelled operation rejects result()", async () => {
    const h = start();
    const op = await h.agent(greeter, "g-1").send("slow", {});
    expect(await op.cancel()).toBe("running");
    hold.open();
    const error: unknown = await op.result().catch((e: unknown) => e);
    expect(error).toMatchObject({ status: "cancelled" });
    expect(await op.cancel()).toBe("cancelled");
  });

  it("maps every ErrorBody code to its typed error", async () => {
    for (const [code, Class] of Object.entries(ERROR_CODES)) {
      const h = createTestHarness({ agents: [greeter] });
      const op = await h.agent(greeter, "g-1").send("greet", { name: "ada" });
      h.coordinator.failNext("operation", { status: 400, code, times: 10 });
      const error: unknown = await op.status().catch((e: unknown) => e);
      await h.close();
      expect(error, code).toBeInstanceOf(Class);
      expect(error).toBeInstanceOf(RavelAgentsError);
    }
  });

  it("an unknown error code or an off-schema response reads as UnsupportedFormat", async () => {
    const h = start();
    const op = await h.agent(greeter, "g-1").send("greet", { name: "ada" });
    h.coordinator.failNext("operation", { status: 400, code: "brand_new_code" });
    await expect(op.status()).rejects.toBeInstanceOf(UnsupportedFormat);

    const client = createClient({
      url: "http://x.invalid",
      token: "t",
      fetch: () => Promise.resolve(new Response(JSON.stringify({ operation: 1 }), { status: 200 })),
    });
    await expect(client.agent<typeof greeter>("greeter", "g-1").send("greet", { name: "a" })).rejects.toBeInstanceOf(
      UnsupportedFormat,
    );
  });

  it("sends the bearer token and the wire body", async () => {
    const seen: { url: string; init: RequestInit }[] = [];
    const client = createClient({
      url: "http://x.invalid/",
      token: "secret",
      fetch: (url, init = {}) => {
        seen.push({ url: String(url), init });
        return Promise.resolve(
          new Response(JSON.stringify({ operation: "op-9", accept_seq: 1, duplicate: false }), { status: 200 }),
        );
      },
    });
    await client.agent<typeof greeter>("greeter", "g-1").send("greet", { name: "a" }, { requestId: "r-9" });
    expect(seen[0]?.url).toBe("http://x.invalid/api/v1/agents/send");
    expect((seen[0]?.init.headers as Record<string, string>).authorization).toBe("Bearer secret");
    expect(seen[0]?.init.body).toBe(
      '{"agent":{"kind":"greeter","key":"g-1"},"handler":"greet","request_id":"r-9","payload":{"name":"a"}}',
    );
  });
});
