/**
 * The mapping between an `ErrorBody` and the typed error classes: `code` is
 * the snake_case `ErrorKind` code of `crates/ravel-agents-core/src/error.rs`.
 */
import {
  AgentGone,
  BudgetExceeded,
  EffectInDoubt,
  IdempotencyConflict,
  IncompatibleHandler,
  MailboxFull,
  NonDeterministicReplay,
  OutcomeUnknown,
  OwnershipLost,
  type RavelAgentsError,
  type RavelAgentsErrorOptions,
  type RetryHint,
  type SafeIds,
  StoreUnavailable,
  UnsupportedFormat,
} from "../errors.js";
import type { ErrorBody, SafeIdsBody } from "./index.js";

type ErrorClass = new (message: string, options?: RavelAgentsErrorOptions) => RavelAgentsError;

export const ERROR_CODES: Readonly<Record<string, ErrorClass>> = {
  agent_gone: AgentGone,
  idempotency_conflict: IdempotencyConflict,
  outcome_unknown: OutcomeUnknown,
  ownership_lost: OwnershipLost,
  incompatible_handler: IncompatibleHandler,
  non_deterministic_replay: NonDeterministicReplay,
  effect_in_doubt: EffectInDoubt,
  mailbox_full: MailboxFull,
  budget_exceeded: BudgetExceeded,
  store_unavailable: StoreUnavailable,
  unsupported_format: UnsupportedFormat,
};

const RETRY_CODES: Readonly<Record<RetryHint, string>> = {
  never: "never",
  backoff: "backoff",
  resolve: "resolve_operation",
  reconcile: "reconcile",
};

function idsFromBody(ids: SafeIdsBody): SafeIds {
  const out: Record<string, string> = {};
  if (ids.agent_id !== undefined) out.agentId = ids.agent_id;
  if (ids.incarnation !== undefined) out.incarnation = String(ids.incarnation);
  if (ids.operation !== undefined) out.operationId = ids.operation;
  if (ids.turn_id !== undefined) out.turnId = ids.turn_id;
  if (ids.step_id !== undefined) out.stepId = ids.step_id;
  if (ids.effect_id !== undefined) out.effectId = ids.effect_id;
  if (ids.timer_id !== undefined) out.timerId = ids.timer_id;
  return out;
}

/** The typed error an `ErrorBody` names; an unknown code reads as `UnsupportedFormat`. */
export function errorFromBody(body: ErrorBody): RavelAgentsError {
  const Class = ERROR_CODES[body.code];
  const ids = idsFromBody(body.ids);
  if (Class === undefined) {
    return new UnsupportedFormat(`unknown error code ${body.code}: ${body.reason}`, { ids });
  }
  return new Class(body.reason, { ids });
}

/** The `ErrorBody` a server would send for `error`. */
export function bodyFromError(error: RavelAgentsError): ErrorBody {
  const code = Object.entries(ERROR_CODES).find(([, Class]) => error instanceof Class)?.[0];
  const ids: SafeIdsBody = {};
  if (error.ids.agentId !== undefined) ids.agent_id = error.ids.agentId;
  if (error.ids.incarnation !== undefined) ids.incarnation = Number(error.ids.incarnation);
  if (error.ids.operationId !== undefined) ids.operation = error.ids.operationId;
  if (error.ids.turnId !== undefined) ids.turn_id = error.ids.turnId;
  if (error.ids.stepId !== undefined) ids.step_id = error.ids.stepId;
  if (error.ids.effectId !== undefined) ids.effect_id = error.ids.effectId;
  if (error.ids.timerId !== undefined) ids.timer_id = error.ids.timerId;
  return {
    code: code ?? "unsupported_format",
    retry: RETRY_CODES[error.retry],
    reason: error.message,
    ids,
  };
}
