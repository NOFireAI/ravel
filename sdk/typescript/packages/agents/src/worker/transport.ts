/**
 * What the worker runner needs from a coordinator. The test harness's fake
 * coordinator implements it in memory; an HTTP transport over the
 * `/api/v1/agents/worker/...` routes implements the same interface once a
 * server exists, and both drive the same runner.
 *
 * `poll`, `commit` and `completeEffect` carry the wire bodies of
 * `crates/ravel-agents-core/src/wire.rs`. Two things the runner needs have no
 * wire body yet, so their shapes here are SDK-local and provisional: the
 * kernel's `RecordStep` command (journal a step as started before its tool
 * runs), and a recorded start time for the turn.
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

export interface StepRecordResponse {
  /** The journaled step; its `step_id` is minted by the coordinator. */
  readonly step: StepBody;
  /** The effect handle the tool's result is completed under. */
  readonly effect: string;
  readonly attempt: number;
}

/**
 * Every method rejects with a `RavelAgentsError` subclass for a protocol
 * outcome: `OwnershipLost` for a fenced-out lease, `NonDeterministicReplay` or
 * `EffectInDoubt` for a step the coordinator refuses.
 */
export interface Transport {
  /** Long-poll for a turn; resolves `null` when the wait elapses with no work. */
  poll(request: WorkerPollRequest, signal: AbortSignal): Promise<PolledTurn | null>;
  recordStep(request: StepRecordRequest): Promise<StepRecordResponse>;
  completeEffect(request: EffectCompleteRequest): Promise<EffectCompleteResponse>;
  commit(request: WorkerCommitRequest): Promise<WorkerCommitResponse>;
}
