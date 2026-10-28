/**
 * What the worker runner needs from a coordinator. The test harness's fake
 * coordinator implements it in memory; an HTTP transport over the
 * `/api/v1/agents/worker/...` routes implements the same interface once a
 * server exists, and both drive the same runner.
 *
 * `poll`, `commit` and `completeEffect` carry the wire bodies of
 * `crates/ravel-agents-core/src/wire.rs`. Three things the runner needs have no
 * wire body yet, so their shapes here are SDK-local and provisional:
 * `recordStep`, which fuses three kernel commands into one call (`RecordStep`
 * to journal the step or read back its record, `CommitIntent` for the effect,
 * and `AuthorizeAttempt`, which is where the answer's `effect` and `attempt`
 * come from; an HTTP transport maps it to more than one route), recording a
 * tool's failure as its attempt's result, and a recorded start time for the
 * turn.
 */
import type {
  EffectCompleteRequest,
  EffectCompleteResponse,
  Policy,
  StepBody,
  TurnAssignment,
  TurnLease,
  WorkerCommitRequest,
  WorkerCommitResponse,
  WorkerPollRequest,
} from "../wire/index.js";

/** One leased turn. */
export interface PolledTurn {
  readonly assignment: TurnAssignment;
  /** When the coordinator first started this input; the turn's `ctx.now()`. */
  readonly startedUnixNs: bigint;
}

/** Journal a step as started, mirroring the kernel's `RecordStep`. */
export interface StepRecordRequest {
  readonly lease: TurnLease;
  readonly step_key: string;
  readonly tool_name: string;
  readonly tool_version: string;
  readonly policy: Policy;
  readonly input_hash: string;
}

/**
 * The step's current record. A step with a recorded result comes back as it
 * is: `step.status` `completed` with its output, or `failure` set. Otherwise
 * `attempt` is an attempt the caller is authorized to run.
 */
export interface StepRecordResponse {
  /** The journaled step; its `step_id` is minted by the coordinator. */
  readonly step: StepBody;
  /** The effect handle the tool's result is completed under. */
  readonly effect: string;
  readonly attempt: number;
  /** The tool failure recorded as the step's result, or `null`. */
  readonly failure: string | null;
}

/** Record a tool's failure as its attempt's result. */
export interface EffectFailRequest {
  readonly effect: string;
  readonly attempt: number;
  readonly error: string;
}

/**
 * Every method rejects with a `RavelAgentsError` subclass for a protocol
 * outcome: `OwnershipLost` for a fenced-out lease, `NonDeterministicReplay` or
 * `EffectInDoubt` for a step the coordinator refuses.
 */
export interface Transport {
  /** Long-poll for a turn; resolves `null` when the wait elapses with no work. */
  poll(request: WorkerPollRequest, signal: AbortSignal): Promise<PolledTurn | null>;
  /**
   * Journal a step, or answer with the record it already has. A step with no
   * result is authorized a new attempt under `at_least_once` and
   * `provider_idempotency`; under `manual` it rejects with `EffectInDoubt`.
   */
  recordStep(request: StepRecordRequest): Promise<StepRecordResponse>;
  /** Record an attempt's result. A `late` result is kept as evidence and changes no journal entry. */
  completeEffect(request: EffectCompleteRequest): Promise<EffectCompleteResponse>;
  /** Record an attempt's failure, under the same `late` rule as {@link completeEffect}. */
  failEffect(request: EffectFailRequest): Promise<EffectCompleteResponse>;
  /** A `failed` commit is refused with `EffectInDoubt` while an authorized attempt has no result. */
  commit(request: WorkerCommitRequest): Promise<WorkerCommitResponse>;
}
