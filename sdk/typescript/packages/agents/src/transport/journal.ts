/**
 * An SDK-local step and effect journal from before the server served the
 * step, effect and reconcile routes. No transport consults it: the HTTP
 * transport records steps on the server and the fake coordinator keeps its
 * own. Only its own test imports it; it stays exported, deprecated, for
 * callers that already do, and a later release removes it.
 * {@link MemoryStepJournal} keeps it in process memory; it is not durable
 * and replaces no server state.
 */
import { EffectInDoubt, NonDeterministicReplay, UnsupportedFormat } from "../errors.js";
import { effectId, stepId } from "../identity.js";
import type {
  EffectBody,
  EffectCompleteResponse,
  EffectState,
  Policy,
  ReconcileRequest,
  ReconcileResponse,
  StepBody,
} from "../wire/index.js";
import type { StepRecordResponse } from "../worker/transport.js";

type Json = StepBody["output"];

/** Where a step sits: its agent incarnation and the operation's accept sequence. */
export interface StepScope {
  readonly agentId: string;
  readonly incarnation: number;
  readonly operation: string;
  readonly acceptSeq: number;
  readonly turnId: string;
}

export interface JournalStepRequest {
  readonly scope: StepScope;
  readonly stepKey: string;
  readonly toolName: string;
  readonly toolVersion: string;
  readonly policy: Policy;
  readonly inputHash: string;
}

/** A result for an attempt the journal had already settled or superseded. */
export interface JournalLateResult {
  readonly effect: string;
  readonly attempt: number;
  readonly result: { readonly output: Json } | { readonly error: string };
}

export interface StepJournal {
  /**
   * Journal a step, or answer with its record. A step with no result is
   * authorized a new attempt under `at_least_once` and `provider_idempotency`
   * and rejects with `EffectInDoubt` under `manual`, unless a reconcile has
   * authorized an attempt nobody has run yet.
   */
  recordStep(request: JournalStepRequest): Promise<StepRecordResponse>;
  /** Record an attempt's result; a late one is kept as evidence and changes nothing. */
  settle(request: JournalLateResult): Promise<EffectCompleteResponse>;
  /** The effect of an authorized attempt with no result in `operation`, if any. */
  pendingEffect(operation: string): string | undefined;
  /** Every effect of `operation`. */
  effects(operation: string): Promise<EffectBody[]>;
  /** Attach a result to an in-doubt effect, or authorize another attempt at it. */
  reconcile(request: ReconcileRequest): Promise<ReconcileResponse>;
}

interface Entry {
  readonly operation: string;
  readonly turnId: string;
  readonly stepId: string;
  readonly stepKey: string;
  readonly toolName: string;
  readonly toolVersion: string;
  readonly inputHash: string;
  readonly policy: Policy;
  readonly effect: string;
  attempt: number;
  state: EffectState;
  /** Whether the current authorized attempt was given to a worker. */
  handedOut: boolean;
  output: Json;
  failure: string | null;
}

/** A {@link StepJournal} in process memory; see the module comment for its limits. */
export class MemoryStepJournal implements StepJournal {
  /** Every late result, kept as evidence; none changed a journal entry. */
  readonly evidence: JournalLateResult[] = [];
  private readonly entries = new Map<string, Entry>();

  private record(entry: Entry): StepRecordResponse {
    return {
      step: {
        step_id: entry.stepId,
        step_key: entry.stepKey,
        tool_name: entry.toolName,
        tool_version: entry.toolVersion,
        input_hash: entry.inputHash,
        status: entry.state === "completed" && entry.failure === null ? "completed" : "started",
        output: entry.output,
      },
      effect: entry.effect,
      attempt: entry.attempt,
      failure: entry.failure,
    };
  }

  recordStep(request: JournalStepRequest): Promise<StepRecordResponse> {
    const { scope } = request;
    const effect = effectId(scope.incarnation, scope.acceptSeq, request.stepKey);
    const ids = {
      agentId: scope.agentId,
      operationId: scope.operation,
      turnId: scope.turnId,
      stepKey: request.stepKey,
    };
    const existing = this.entries.get(effect);
    if (existing === undefined) {
      const entry: Entry = {
        operation: scope.operation,
        turnId: scope.turnId,
        stepId: stepId(scope.incarnation, scope.acceptSeq, request.stepKey),
        stepKey: request.stepKey,
        toolName: request.toolName,
        toolVersion: request.toolVersion,
        inputHash: request.inputHash,
        policy: request.policy,
        effect,
        attempt: 1,
        state: "authorized",
        handedOut: true,
        output: null,
        failure: null,
      };
      this.entries.set(effect, entry);
      return Promise.resolve(this.record(entry));
    }
    if (
      existing.toolName !== request.toolName ||
      existing.toolVersion !== request.toolVersion ||
      existing.inputHash !== request.inputHash
    ) {
      return Promise.reject(
        new NonDeterministicReplay(`step ${request.stepKey} reused with a different tool or input hash`, {
          ids: { ...ids, stepId: existing.stepId },
        }),
      );
    }
    if (existing.state === "completed") return Promise.resolve(this.record(existing));
    if (existing.state === "authorized" && !existing.handedOut) {
      existing.handedOut = true;
      return Promise.resolve(this.record(existing));
    }
    if (existing.policy === "manual") {
      existing.state = "in_doubt";
      return Promise.reject(
        new EffectInDoubt(`effect ${effect} of manual step ${request.stepKey} is in doubt`, {
          ids: { ...ids, effectId: effect },
        }),
      );
    }
    existing.attempt += 1;
    existing.state = "authorized";
    existing.handedOut = true;
    return Promise.resolve(this.record(existing));
  }

  settle(request: JournalLateResult): Promise<EffectCompleteResponse> {
    const entry = this.entries.get(request.effect);
    if (entry === undefined) {
      return Promise.reject(new UnsupportedFormat(`unknown effect ${request.effect}`));
    }
    const late = entry.state === "completed" || request.attempt !== entry.attempt;
    if (late) {
      this.evidence.push(structuredClone(request));
    } else {
      entry.state = "completed";
      if ("output" in request.result) entry.output = request.result.output;
      else entry.failure = request.result.error;
    }
    return Promise.resolve({ effect: entry.effect, status: entry.state, late });
  }

  pendingEffect(operation: string): string | undefined {
    for (const entry of this.entries.values()) {
      if (entry.operation === operation && entry.state === "authorized" && entry.handedOut) {
        return entry.effect;
      }
    }
    return undefined;
  }

  effects(operation: string): Promise<EffectBody[]> {
    return Promise.resolve(
      [...this.entries.values()]
        .filter((entry) => entry.operation === operation)
        .map((entry) => ({
          effect: entry.effect,
          turn_id: entry.turnId,
          tool_name: entry.toolName,
          tool_version: entry.toolVersion,
          policy: entry.policy,
          status: entry.state,
          attempt: entry.attempt,
        })),
    );
  }

  reconcile(request: ReconcileRequest): Promise<ReconcileResponse> {
    const entry = this.entries.get(request.effect);
    if (entry === undefined) {
      return Promise.reject(new UnsupportedFormat(`unknown effect ${request.effect}`));
    }
    if (entry.state !== "in_doubt" || request.attempt !== entry.attempt) {
      return Promise.reject(
        new UnsupportedFormat(
          `effect ${entry.effect} is ${entry.state} at attempt ${String(entry.attempt)}; ` +
            `reconcile names attempt ${String(request.attempt)} of an in-doubt effect`,
        ),
      );
    }
    if (request.decision === "record_result") {
      entry.state = "completed";
      entry.output = request.result;
    } else {
      entry.attempt += 1;
      entry.state = "authorized";
      entry.handedOut = false;
    }
    return Promise.resolve({ effect: entry.effect, status: entry.state, attempt: entry.attempt });
  }
}
