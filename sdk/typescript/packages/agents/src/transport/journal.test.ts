import { describe, expect, it } from "vitest";
import { NonDeterministicReplay, UnsupportedFormat } from "../errors.js";
import { MemoryStepJournal } from "./journal.js";

async function caught(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error("expected a rejection");
}

describe("MemoryStepJournal", () => {
  const scope = { agentId: "a", incarnation: 1, operation: "op", acceptSeq: 3, turnId: "t" };
  const step = (policy: "at_least_once" | "manual", inputHash = "h") => ({
    scope,
    stepKey: "k",
    toolName: "tool",
    toolVersion: "1",
    policy,
    inputHash,
  });

  it("refuses a reused step key with a different input hash", async () => {
    const journal = new MemoryStepJournal();
    await journal.recordStep(step("at_least_once"));
    expect(await caught(journal.recordStep(step("at_least_once", "other")))).toBeInstanceOf(NonDeterministicReplay);
  });

  it("refuses a reconcile of an effect that is not in doubt", async () => {
    const journal = new MemoryStepJournal();
    const first = await journal.recordStep(step("manual"));
    expect(await caught(journal.reconcile({ effect: first.effect, attempt: 1, decision: "retry" }))).toBeInstanceOf(
      UnsupportedFormat,
    );
  });
});
