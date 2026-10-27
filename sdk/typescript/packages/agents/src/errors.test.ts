import { describe, expect, it } from "vitest";
import * as errors from "./errors.js";
import { RavelAgentsError, type RetryHint } from "./errors.js";

const EXPECTED: Record<string, RetryHint> = {
  AgentGone: "never",
  IdempotencyConflict: "never",
  OutcomeUnknown: "resolve",
  OwnershipLost: "never",
  IncompatibleHandler: "never",
  NonDeterministicReplay: "never",
  EffectInDoubt: "reconcile",
  MailboxFull: "backoff",
  BudgetExceeded: "never",
  StoreUnavailable: "backoff",
  UnsupportedFormat: "never",
};

type ErrorClass = new (message: string, options?: errors.RavelAgentsErrorOptions) => RavelAgentsError;

function errorClasses(): [string, ErrorClass][] {
  return Object.entries(errors as Record<string, unknown>).filter(
    (entry): entry is [string, ErrorClass] =>
      typeof entry[1] === "function" &&
      (entry[1] as ErrorClass).prototype instanceof RavelAgentsError,
  );
}

describe("typed errors", () => {
  it("exports exactly the eleven ADR-3331 errors", () => {
    expect(errorClasses().map(([name]) => name).sort()).toEqual(Object.keys(EXPECTED).sort());
  });

  it.each(Object.entries(EXPECTED))("%s carries its code, retry hint and safe ids", (name, retry) => {
    const found = errorClasses().find(([n]) => n === name);
    if (found === undefined) throw new Error(`missing ${name}`);
    const Class = found[1];
    const ids = { agentId: "a-1", operationId: "op-1", epoch: 3 };
    const cause = new Error("store said no");
    const err = new Class("boom", { ids, cause });
    expect(err).toBeInstanceOf(RavelAgentsError);
    expect(err).toBeInstanceOf(Error);
    expect(err.code).toBe(name);
    expect(err.name).toBe(name);
    expect(err.retry).toBe(retry);
    expect(err.ids).toEqual(ids);
    expect(err.cause).toBe(cause);
    expect(Object.isFrozen(err.ids)).toBe(true);
    ids.agentId = "changed";
    expect(err.ids.agentId).toBe("a-1");
  });
});
