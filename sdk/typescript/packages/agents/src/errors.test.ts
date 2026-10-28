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

/** Raised by the step runner and never carried on the wire. */
const SDK_LOCAL = ["ConcurrentStepError", "InvalidStepInput"];

/** The server's 401 and 403, which carry no `ErrorBody`. */
const AUTH = ["Forbidden"];

type ErrorClass = new (message: string, options?: errors.RavelAgentsErrorOptions) => RavelAgentsError;

function errorClasses(): [string, ErrorClass][] {
  return Object.entries(errors as Record<string, unknown>).filter(
    (entry): entry is [string, ErrorClass] =>
      typeof entry[1] === "function" &&
      (entry[1] as ErrorClass).prototype instanceof RavelAgentsError,
  );
}

describe("typed errors", () => {
  it("exports exactly the eleven ADR-3331 errors, the auth refusal and the two SDK-local ones", () => {
    expect(errorClasses().map(([name]) => name).sort()).toEqual(
      [...Object.keys(EXPECTED), ...AUTH, ...SDK_LOCAL].sort(),
    );
  });

  it("the auth refusal is typed, carries its HTTP status and is never retried", () => {
    const err = new errors.Forbidden(403, "this route requires the agents capability");
    expect(err).toBeInstanceOf(RavelAgentsError);
    expect(err.code).toBe("Forbidden");
    expect(err.name).toBe("Forbidden");
    expect(err.retry).toBe("never");
    expect(err.status).toBe(403);
  });

  it("the SDK-local errors are typed and never retried", () => {
    const concurrent = new errors.ConcurrentStepError("b", "a", { ids: { operationId: "op-1" } });
    const invalid = new errors.InvalidStepInput("value.at is a Date, not plain JSON");
    for (const err of [concurrent, invalid]) {
      expect(err).toBeInstanceOf(RavelAgentsError);
      expect(err.retry).toBe("never");
    }
    expect(concurrent.code).toBe("ConcurrentStep");
    expect(concurrent.ids).toEqual({ operationId: "op-1", stepKey: "b" });
    expect(invalid.code).toBe("InvalidStepInput");
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
