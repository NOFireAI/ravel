/**
 * The typed errors of ADR-3331 decision 11. Every surface of the SDK reports
 * a protocol outcome as one of these classes, never as a bare Error.
 */

/**
 * Identifiers that are safe to log and to show a caller: they name the
 * objects involved and never carry a payload, a state value or a credential.
 */
export interface SafeIds {
  readonly agentId?: string;
  readonly incarnation?: string;
  readonly requestId?: string;
  readonly operationId?: string;
  readonly turnId?: string;
  readonly stepKey?: string;
  readonly effectId?: string;
  readonly epoch?: number;
}

/**
 * What a caller may do after the error.
 *
 * - `never`: retrying the same request cannot succeed.
 * - `backoff`: the same request may succeed later; wait before retrying.
 * - `resolve`: the outcome is unknown; resolve the existing operation handle
 *   instead of sending again with a fresh request id.
 * - `reconcile`: an authenticated reconciliation must attach a result or
 *   authorize another attempt before work continues.
 */
export type RetryHint = "never" | "backoff" | "resolve" | "reconcile";

export type RavelAgentsErrorCode =
  | "AgentGone"
  | "IdempotencyConflict"
  | "OutcomeUnknown"
  | "OwnershipLost"
  | "IncompatibleHandler"
  | "NonDeterministicReplay"
  | "EffectInDoubt"
  | "MailboxFull"
  | "BudgetExceeded"
  | "StoreUnavailable"
  | "UnsupportedFormat";

export interface RavelAgentsErrorOptions {
  readonly ids?: SafeIds;
  readonly cause?: unknown;
}

export abstract class RavelAgentsError extends Error {
  abstract readonly code: RavelAgentsErrorCode;
  abstract readonly retry: RetryHint;
  readonly ids: Readonly<SafeIds>;

  constructor(message: string, options: RavelAgentsErrorOptions = {}) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.ids = Object.freeze({ ...options.ids });
  }
}

/** The agent was deleted or its incarnation ended. */
export class AgentGone extends RavelAgentsError {
  override readonly name = "AgentGone";
  readonly code = "AgentGone";
  readonly retry = "never";
}

/** A request id was reused with a different handler or payload hash. */
export class IdempotencyConflict extends RavelAgentsError {
  override readonly name = "IdempotencyConflict";
  readonly code = "IdempotencyConflict";
  readonly retry = "never";
}

/** The store failed after a CAS was attempted; the operation may have landed. */
export class OutcomeUnknown extends RavelAgentsError {
  override readonly name = "OutcomeUnknown";
  readonly code = "OutcomeUnknown";
  readonly retry = "resolve";
}

/** The epoch advanced past this worker; it can no longer commit. */
export class OwnershipLost extends RavelAgentsError {
  override readonly name = "OwnershipLost";
  readonly code = "OwnershipLost";
  readonly retry = "never";
}

/** The handler version or digest no longer matches the turn it would commit. */
export class IncompatibleHandler extends RavelAgentsError {
  override readonly name = "IncompatibleHandler";
  readonly code = "IncompatibleHandler";
  readonly retry = "never";
}

/** A step key was reused with a different tool identity or input hash. */
export class NonDeterministicReplay extends RavelAgentsError {
  override readonly name = "NonDeterministicReplay";
  readonly code = "NonDeterministicReplay";
  readonly retry = "never";
}

/** A `manual` tool's attempt is ambiguous and awaits reconciliation. */
export class EffectInDoubt extends RavelAgentsError {
  override readonly name = "EffectInDoubt";
  readonly code = "EffectInDoubt";
  readonly retry = "reconcile";
}

/** The agent's receipt index reached its logical cap. */
export class MailboxFull extends RavelAgentsError {
  override readonly name = "MailboxFull";
  readonly code = "MailboxFull";
  readonly retry = "backoff";
}

/** A quota or budget the request would exceed. */
export class BudgetExceeded extends RavelAgentsError {
  override readonly name = "BudgetExceeded";
  readonly code = "BudgetExceeded";
  readonly retry = "never";
}

/** The object store is unavailable and no CAS was attempted. */
export class StoreUnavailable extends RavelAgentsError {
  override readonly name = "StoreUnavailable";
  readonly code = "StoreUnavailable";
  readonly retry = "backoff";
}

/** An object or message uses a format version this SDK does not read. */
export class UnsupportedFormat extends RavelAgentsError {
  override readonly name = "UnsupportedFormat";
  readonly code = "UnsupportedFormat";
  readonly retry = "never";
}
