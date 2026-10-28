/**
 * The assistant agent: answers a question from the local knowledge base in
 * two `at_least_once` steps (search, then draft) and, when asked to escalate,
 * files a ticket in a third step under `manual`, the policy for an effect
 * that must not be repeated without an operator's decision.
 */
import { defineTool } from "@ravel/agents/tools";
import { defineAgent } from "@ravel/agents/worker";
import { z } from "zod";
import type { AssistantFixture } from "./fixture.js";

export const SEARCH = "kb.search";
export const DRAFT = "kb.draft";
export const TICKET = "desk.ticket";

export function createAssistant(fixture: AssistantFixture) {
  const search = defineTool({
    name: SEARCH,
    version: "1",
    input: z.object({ query: z.string() }),
    output: z.object({ articles: z.array(z.string()) }),
    recovery: "at_least_once",
    run: (input, ctx) => fixture.run(SEARCH, ctx, () => ({ articles: fixture.search(input.query) })),
  });
  const draft = defineTool({
    name: DRAFT,
    version: "1",
    input: z.object({ articles: z.array(z.string()) }),
    output: z.object({ answer: z.string() }),
    recovery: "at_least_once",
    run: (input, ctx) => fixture.run(DRAFT, ctx, () => ({ answer: fixture.draft(input.articles) })),
  });
  const ticket = defineTool({
    name: TICKET,
    version: "1",
    input: z.object({ question: z.string() }),
    output: z.object({ ticket: z.string() }),
    recovery: "manual",
    run: (_input, ctx) => fixture.run(TICKET, ctx, () => ({ ticket: fixture.file() })),
  });

  const assistant = defineAgent({
    name: "assistant",
    version: "1",
    state: z.object({ answered: z.number().int() }),
    initialState: { answered: 0 },
    messages: (message) => ({
      ask: message({
        payload: z.object({ question: z.string(), escalate: z.boolean() }),
        result: z.object({
          answer: z.string(),
          articles: z.array(z.string()),
          ticket: z.string().nullable(),
        }),
        handler: async (ctx, payload) => {
          const found = await ctx.step("search", search, { query: payload.question });
          const drafted = await ctx.step("draft", draft, { articles: found.articles });
          const filed = payload.escalate
            ? await ctx.step("ticket", ticket, { question: payload.question })
            : null;
          ctx.setState({ answered: ctx.state.answered + 1 });
          return { answer: drafted.answer, articles: found.articles, ticket: filed?.ticket ?? null };
        },
      }),
    }),
  });

  return { assistant, tools: [search, draft, ticket] } as const;
}

export type Assistant = ReturnType<typeof createAssistant>["assistant"];
