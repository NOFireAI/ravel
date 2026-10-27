/**
 * Compile-time tests, checked by `npm run typecheck` and never executed.
 * Each `@ts-expect-error` line must fail to compile: if a change to the
 * public types makes it compile, tsc reports the unused directive (TS2578)
 * and the typecheck script fails.
 */
import { z } from "zod";
import { createClient } from "@ravel/agents/client";
import { defineTool } from "@ravel/agents/tools";
import { defineAgent, message } from "@ravel/agents/worker";

type Equal<X, Y> = (<T>() => T extends X ? 1 : 2) extends <T>() => T extends Y ? 1 : 2
  ? true
  : false;
function assertType<_T extends true>(): void {
  // Type-level assertion only.
}

const lookup = defineTool({
  name: "lookup",
  version: "1",
  input: z.object({ key: z.string() }),
  output: z.object({ value: z.number() }),
  recovery: "at_least_once",
  run: (input) => Promise.resolve({ value: input.key.length }),
});

export const counter = defineAgent({
  name: "counter",
  version: "1",
  state: z.object({ total: z.number() }),
  initialState: { total: 0 },
  messages: (message) => ({
    increment: message({
      payload: z.object({ by: z.number() }),
      result: z.number(),
      handler: async (ctx, payload) => {
        assertType<Equal<typeof ctx.state, Readonly<{ total: number }>>>();
        const looked = await ctx.callTool("lookup-1", lookup, { key: "k" });
        const total = ctx.state.total + payload.by + looked.value;
        ctx.setState({ total });
        return total;
      },
    }),
    reset: message({
      payload: z.object({}),
      result: z.null(),
      handler: (ctx) => {
        ctx.setState({ total: 0 });
        return Promise.resolve(null);
      },
    }),
  }),
});

// Invalid handler signature: the payload parameter disagrees with the schema.
defineAgent({
  name: "bad-payload-param",
  version: "1",
  state: z.object({ total: z.number() }),
  initialState: { total: 0 },
  messages: {
    increment: message({
      payload: z.object({ by: z.number() }),
      result: z.number(),
      // @ts-expect-error the handler's payload must be { by: number }, not string
      handler: (_ctx, payload: string) => Promise.resolve(payload.length),
    }),
  },
});

// Invalid handler signature: the handler returns something other than the result schema.
message({
  payload: z.object({ by: z.number() }),
  result: z.number(),
  // @ts-expect-error the handler must resolve to number, not string
  handler: (_ctx, payload) => Promise.resolve(String(payload.by)),
});

// Invalid handler signature: a synchronous handler where a Promise is required.
message({
  payload: z.object({ by: z.number() }),
  result: z.number(),
  // @ts-expect-error handlers are async
  handler: (_ctx, payload) => payload.by,
});

// Invalid handler signature: the context's state type disagrees with the agent's state.
defineAgent({
  name: "bad-state",
  version: "1",
  state: z.object({ total: z.number() }),
  initialState: { total: 0 },
  messages: (message) => ({
    increment: message({
      payload: z.object({ by: z.number() }),
      result: z.number(),
      handler: (ctx, payload) => {
        // @ts-expect-error state is { total: number }; setState rejects a string
        ctx.setState("total");
        return Promise.resolve(payload.by);
      },
    }),
  }),
});

const client = createClient({ url: "http://127.0.0.1:9090", token: "dev" });
const agent = client.agent<typeof counter>("counter", "c-1");

export async function sends(): Promise<number> {
  const handle = await agent.send("increment", { by: 1 });
  const result = await handle.wait();
  assertType<Equal<typeof result, number>>();

  // @ts-expect-error invalid payload type: by is a number
  await agent.send("increment", { by: "one" });

  // @ts-expect-error invalid payload type: by is required
  await agent.send("increment", {});

  // @ts-expect-error unknown handler name
  await agent.send("decrement", { by: 1 });

  // @ts-expect-error an agent addressed without its definition type accepts no message
  await client.agent("counter", "c-1").send("increment", { by: 1 });

  return result;
}

defineTool({
  name: "bad-tool",
  version: "1",
  input: z.object({ key: z.string() }),
  output: z.object({ value: z.number() }),
  recovery: "at_least_once",
  // @ts-expect-error invalid tool: run must resolve to the output schema
  run: () => Promise.resolve({ value: "x" }),
});

defineTool({
  name: "bad-policy",
  version: "1",
  input: z.object({ key: z.string() }),
  output: z.object({ value: z.number() }),
  // @ts-expect-error recovery is one of the three ADR-3331 policies
  recovery: "exactly_once",
  run: () => Promise.resolve({ value: 1 }),
});
