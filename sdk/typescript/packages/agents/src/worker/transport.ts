/**
 * What the worker runner needs from a coordinator. The test harness's fake
 * coordinator implements it in memory and `createHttpTransport` over the
 * server's `/api/v1/agents/worker/...` routes; both drive the same runner.
 *
 * Every method carries the wire bodies of `crates/ravel-agents-core/src/wire.rs`:
 * `recordStep` is `worker/step`, `completeEffect` and `failEffect` are
 * `worker/effects/complete` and `worker/effects/fail`. A polled lease carries
 * the turn's recorded time and random seed.
 */
import type {
  EffectCompleteRequest,
  EffectCompleteResponse,
  EffectFailRequest,
  StepRecordRequest,
  StepRecordResponse,
  TurnAssignment,
  WorkerCommitRequest,
  WorkerCommitResponse,
  WorkerHeartbeatRequest,
  WorkerHeartbeatResponse,
  WorkerPollRequest,
  WorkerRegisterRequest,
  WorkerRegisterResponse,
} from "../wire/index.js";

export type { EffectFailRequest, StepRecordRequest, StepRecordResponse } from "../wire/index.js";

/**
 * One leased turn. `assignment.lease.recorded_unix_ns` is the turn's
 * `ctx.now()` and `assignment.lease.random_seed` seeds its `ctx.random()`.
 */
export interface PolledTurn {
  readonly assignment: TurnAssignment;
}

/**
 * Every method rejects with a `RavelAgentsError` subclass for a protocol
 * outcome: `OwnershipLost` for a fenced-out lease, `NonDeterministicReplay` or
 * `EffectInDoubt` for a step the coordinator refuses, `AgentGone` once the
 * agent's incarnation was destroyed.
 */
export interface Transport {
  /** Register the handlers this worker runs; the answer names the worker for later polls. */
  register(request: WorkerRegisterRequest): Promise<WorkerRegisterResponse>;
  /** Extend a held lease; rejects with `OwnershipLost` once the lease is stale. */
  heartbeat(request: WorkerHeartbeatRequest): Promise<WorkerHeartbeatResponse>;
  /** Long-poll for a turn; resolves `null` when the wait elapses with no work. */
  poll(request: WorkerPollRequest, signal: AbortSignal): Promise<PolledTurn | null>;
  /**
   * Journal a step, or answer with the record it already has. A step with no
   * result is authorized a new attempt under `at_least_once` and
   * `provider_idempotency`; under `manual` it rejects with `EffectInDoubt`.
   * The answer's `step.status` is `completed` with its output, or `failure`
   * is set, for a step with a recorded result; otherwise `effect` and
   * `attempt` name the attempt the caller is authorized to run.
   */
  recordStep(request: StepRecordRequest): Promise<StepRecordResponse>;
  /** Record an attempt's result. A `late` result is kept as evidence and changes no journal entry. */
  completeEffect(request: EffectCompleteRequest): Promise<EffectCompleteResponse>;
  /** Record an attempt's failure, under the same `late` rule as {@link completeEffect}. */
  failEffect(request: EffectFailRequest): Promise<EffectCompleteResponse>;
  /** A `failed` commit is refused with `EffectInDoubt` while an authorized attempt has no result. */
  commit(request: WorkerCommitRequest): Promise<WorkerCommitResponse>;
}
