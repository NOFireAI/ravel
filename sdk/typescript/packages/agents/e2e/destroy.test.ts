import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { type Assistant, createAssistant, SEARCH } from "../../../examples/assistant/assistant.js";
import { AssistantFixture } from "../../../examples/assistant/fixture.js";
import { AgentGone, Forbidden } from "@ravel/agents";
import { createClient } from "@ravel/agents/client";
import { createHttpTransport, serveAgents, type Transport, type TurnReport } from "@ravel/agents/worker";
import { decide, plan, type RunningServer, startServer, TOKENS } from "./server.js";

const target = plan();
const decision = decide(target);

const QUESTION = { question: "How does a worker recover a step after a crash?", escalate: false } as const;
const ANSWER =
  "A worker that dies mid-turn is replaced and the turn is re-run. " +
  "A step whose result is journaled is replayed, never run again.";

async function until(what: string, condition: () => Promise<boolean> | boolean, ms = 60_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!(await condition())) {
    if (Date.now() > deadline) throw new Error(`${what}: not reached within ${String(ms)} ms`);
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

async function rejection(work: Promise<unknown>): Promise<unknown> {
  return work.then(
    () => {
      throw new Error("expected a rejection");
    },
    (error: unknown) => error,
  );
}

if (decision.kind === "fail") {
  describe(`destroy against ravel-server (${target.store})`, () => {
    it("has its prerequisites (RAVEL_E2E_REQUIRE=1)", () => {
      throw new Error(decision.reason);
    });
  });
}

describe.skipIf(decision.kind !== "run")(
  `destroy against ravel-server (${target.store})${
    decision.kind === "skip" ? `: SKIPPED, ${decision.reason}` : ""
  }`,
  () => {
    let server: RunningServer | undefined;
    beforeAll(async () => {
      server = await startServer(target.binary, target.store);
    });
    afterAll(async () => {
      await server?.stop();
    });

    it("destroy fences a worker holding a lease and a recreate starts incarnation two", async () => {
      if (server === undefined) throw new Error("ravel-server did not start");
      const url = server.url;
      const fixture = new AssistantFixture();
      let release: () => void = () => undefined;
      let held = false;
      fixture.holdAt({
        tool: SEARCH,
        attempt: 1,
        release: new Promise<void>((resolve) => {
          release = resolve;
        }),
        onHeld: () => {
          held = true;
        },
      });
      const { assistant, tools } = createAssistant(fixture);
      // Every commit the worker sends is counted, so a worker that commits a
      // turn after `agent_gone` fails below.
      const http = createHttpTransport({ url, token: TOKENS.worker });
      const commits: Parameters<Transport["commit"]>[0][] = [];
      const transport: Transport = {
        register: (request) => http.register(request),
        heartbeat: (request) => http.heartbeat(request),
        poll: (request, signal) => http.poll(request, signal),
        recordStep: (request) => http.recordStep(request),
        completeEffect: (request) => http.completeEffect(request),
        failEffect: (request) => http.failEffect(request),
        commit: (request) => {
          commits.push(request);
          return http.commit(request);
        },
      };
      const reports: TurnReport[] = [];
      const worker = await serveAgents({
        transport,
        agents: [assistant],
        tools,
        onTurn: (report) => reports.push(report),
      });
      try {
        const key = `e2e-destroy-${randomUUID()}`;
        const agent = createClient({ url, token: TOKENS.client }).agent<Assistant>("assistant", key);
        const admin = createClient({ url, token: TOKENS.admin }).agent<Assistant>("assistant", key);

        expect(await agent.create()).toMatchObject({ created: true });
        const old = await agent.send("ask", QUESTION);
        await until("kb.search recorded and held", async () =>
          held && (await agent.effects()).some((e) => e.tool_name === SEARCH && e.status === "authorized"),
        );

        const destroyed = await admin.destroy();
        expect(destroyed).toEqual({
          agent_id: expect.any(String) as string,
          incarnation: 1,
          destroyed: true,
          abandoned: {
            inputs: 1,
            active_turn: true,
            effects_in_doubt: 0,
            effects_authorized: 1,
            timers_scheduled: 0,
          },
        });

        // The held tool returns into a turn whose agent is gone: the worker
        // abandons it with AgentGone and never sends its commit.
        release();
        await until("the held turn abandoned", () => reports.length > 0);
        const [report] = reports;
        expect(report?.outcome).toBe("abandoned");
        expect(report?.operation).toBe(old.operationId);
        expect(report?.outcome === "abandoned" ? report.error : undefined).toBeInstanceOf(AgentGone);
        expect(commits).toEqual([]);
        expect(await rejection(old.status())).toBeInstanceOf(AgentGone);

        // A second destroy finds the tombstone: nothing written, same counts.
        expect(await admin.destroy()).toEqual({ ...destroyed, destroyed: false });

        const refused = await rejection(agent.destroy());
        expect(refused).toBeInstanceOf(Forbidden);
        expect(refused).toMatchObject({ status: 403 });
        expect(String(refused)).toMatch(/agents_admin/);

        expect(await agent.create()).toEqual({ agentId: destroyed.agent_id, created: true });
        expect((await agent.state()).incarnation).toBe(2);
        expect(await rejection(old.status())).toBeInstanceOf(AgentGone);

        const fresh = await agent.send("ask", QUESTION);
        expect(fresh.operationId).not.toBe(old.operationId);
        expect(await fresh.result({ deadlineMs: 60_000 })).toEqual({
          answer: ANSWER,
          articles: ["kb-2", "kb-3"],
          ticket: null,
        });
        const state = await agent.state();
        expect(state.incarnation).toBe(2);
        expect(state.state).toEqual({ answered: 1 });
        expect(commits).toHaveLength(1);
        expect(reports.filter((r) => r.outcome === "abandoned")).toHaveLength(1);
      } finally {
        release();
        await worker.close();
      }
    });
  },
);
