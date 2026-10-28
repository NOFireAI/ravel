import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { runKillAndRecover } from "../../../examples/assistant/scenario.js";
import { plan, type RunningServer, startServer, TOKENS } from "./server.js";

const target = plan();

const ANSWER =
  "A worker that dies mid-turn is replaced and the turn is re-run. " +
  "A step whose result is journaled is replayed, never run again.";

describe.skipIf(target.missing !== undefined)(
  `kill and recover against ravel-server (${target.store})${
    target.missing === undefined ? "" : `: SKIPPED, ${target.missing}`
  }`,
  () => {
    let server: RunningServer;
    beforeAll(async () => {
      server = await startServer(target.binary, target.store);
    });
    afterAll(async () => {
      await server.stop();
    });

    const scenario = (policy: "at_least_once" | "manual") =>
      runKillAndRecover({
        url: server.url,
        clientToken: TOKENS.client,
        workerToken: TOKENS.worker,
        policy,
        key: `e2e-${policy}-${randomUUID()}`,
        requestId: `e2e-request-${randomUUID()}`,
      });

    it("worker killed inside an at_least_once step recovers the same operation on restart", async () => {
      const run = await scenario("at_least_once");

      expect(run.operationId).toMatch(/\S/);
      expect(run.retriedOperationId).toBe(run.operationId);
      expect(run.workerB).not.toBe(run.workerA);
      // search was journaled before the kill and is replayed, not rerun; draft
      // was killed after its effect and runs again under the same effect id.
      expect(run.counts).toEqual({ search: 1, draft: 2, ticket: 0 });
      const drafts = run.calls.filter((c) => c.tool === "kb.draft");
      expect(drafts.map((c) => c.attempt)).toEqual([1, 2]);
      expect(drafts[1]?.effectId).toBe(drafts[0]?.effectId);
      expect(run.resultError).toBeNull();
      expect(run.result).toEqual({ answer: ANSWER, articles: ["kb-2", "kb-3"], ticket: null });
      expect(run.answered).toBe(1);
      expect(run.effects.map((e) => [e.tool_name, e.status, e.attempt])).toEqual([
        ["kb.search", "completed", 1],
        ["kb.draft", "completed", 2],
      ]);
      expect(run.inDoubt).toBeNull();
    });

    it("worker killed inside a manual step blocks as EffectInDoubt until reconciled", async () => {
      const run = await scenario("manual");

      expect(run.retriedOperationId).toBe(run.operationId);
      expect(run.inDoubt?.error).toMatch(/^EffectInDoubt: /);
      expect(run.inDoubt?.effect.tool_name).toBe("desk.ticket");
      expect(run.inDoubt?.effect.status).toBe("in_doubt");
      expect(run.inDoubt?.effect.attempt).toBe(1);
      // The blocked turn never reran the ticket; the reconcile recorded the
      // ticket the killed run filed instead of filing another.
      expect(run.inDoubt?.ticketRunsBeforeReconcile).toBe(1);
      // No server route reports a blocked operation yet (the step and effect
      // routes are pending), so the server still reads it as running.
      expect(run.inDoubt?.serverStatus).toBe("running");
      expect(run.counts).toEqual({ search: 1, draft: 1, ticket: 1 });
      expect(run.resultError).toBeNull();
      expect(run.result).toEqual({ answer: ANSWER, articles: ["kb-2", "kb-3"], ticket: "T-1001" });
      expect(run.answered).toBe(1);
      expect(run.effects.map((e) => [e.tool_name, e.status])).toEqual([
        ["kb.search", "completed"],
        ["kb.draft", "completed"],
        ["desk.ticket", "completed"],
      ]);
    });
  },
);
