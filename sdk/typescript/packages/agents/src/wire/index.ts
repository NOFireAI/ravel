/**
 * Schemas for the JSON bodies of the `/api/v1/agents/...` routes, mirroring
 * `crates/ravel-agents-core/src/wire.rs`. Field names and order follow the
 * Rust structs, so `encodeWire` reproduces a fixture byte for byte
 * (`wire.test.ts`). Every `*_unix_ns` field is a canonical decimal string on
 * the wire and a `bigint` in TypeScript; u64 counters are numbers refused
 * past `Number.MAX_SAFE_INTEGER` rather than rounded.
 */
import { z } from "zod";

const UnixNs = z.codec(z.string().regex(/^(0|-?[1-9][0-9]*)$/), z.bigint(), {
  decode: (text) => BigInt(text),
  encode: (value) => value.toString(),
});
const U64 = z.number().int().min(0).max(Number.MAX_SAFE_INTEGER);
const U32 = z.number().int().min(0).max(0xffffffff);
const Json = z.json();

export const AgentRef = z.object({ kind: z.string(), key: z.string() });
export type AgentRef = z.output<typeof AgentRef>;

export const OperationStatus = z.enum([
  "accepted",
  "running",
  "blocked",
  "completed",
  "failed",
  "cancelled",
]);
export type OperationStatus = z.output<typeof OperationStatus>;

export const TERMINAL_STATUSES: readonly OperationStatus[] = ["completed", "failed", "cancelled"];

export const Policy = z.enum(["provider_idempotency", "at_least_once", "manual"]);
export type Policy = z.output<typeof Policy>;

export const EffectState = z.enum(["intent", "authorized", "in_doubt", "completed"]);
export type EffectState = z.output<typeof EffectState>;

export const StepState = z.enum(["started", "completed"]);
export type StepState = z.output<typeof StepState>;

export const CreateAgentRequest = z.object({ agent: AgentRef });
export type CreateAgentRequest = z.output<typeof CreateAgentRequest>;

export const CreateAgentResponse = z.object({
  agent_id: z.string(),
  incarnation: U64,
  created: z.boolean(),
});
export type CreateAgentResponse = z.output<typeof CreateAgentResponse>;

export const SendRequest = z.object({
  agent: AgentRef,
  handler: z.string(),
  request_id: z.string(),
  payload: Json,
});
export type SendRequest = z.output<typeof SendRequest>;

export const SendResponse = z.object({
  operation: z.string(),
  accept_seq: U64,
  duplicate: z.boolean(),
});
export type SendResponse = z.output<typeof SendResponse>;

export const CallRequest = z.object({
  agent: AgentRef,
  handler: z.string(),
  request_id: z.string(),
  payload: Json,
  wait_ms: U32,
});
export type CallRequest = z.output<typeof CallRequest>;

export const ResultBody = z.object({
  status: OperationStatus,
  output: Json.nullable(),
  error: z.string().nullable(),
  dead_lettered: z.boolean(),
  finished_unix_ns: UnixNs,
});
export type ResultBody = z.output<typeof ResultBody>;

export const CallResponse = z.object({
  operation: z.string(),
  status: OperationStatus,
  result: ResultBody.nullable(),
});
export type CallResponse = z.output<typeof CallResponse>;

export const OperationResponse = z.object({
  operation: z.string(),
  status: OperationStatus,
  accept_seq: U64,
  blocked_effect: z.string().nullable(),
});
export type OperationResponse = z.output<typeof OperationResponse>;

export const ResultResponse = z.object({
  operation: z.string(),
  result: ResultBody.nullable(),
});
export type ResultResponse = z.output<typeof ResultResponse>;

export const ResolveRequest = z.object({
  agent: AgentRef,
  handler: z.string(),
  request_id: z.string(),
});
export type ResolveRequest = z.output<typeof ResolveRequest>;

export const ResolveResponse = z.object({
  operation: z.string().nullable(),
  status: OperationStatus.nullable(),
});
export type ResolveResponse = z.output<typeof ResolveResponse>;

export const CancelRequest = z.object({ operation: z.string() });
export type CancelRequest = z.output<typeof CancelRequest>;

export const CancelResponse = z.object({
  operation: z.string(),
  status: OperationStatus,
  requested: z.boolean(),
});
export type CancelResponse = z.output<typeof CancelResponse>;

export const StateResponse = z.object({
  agent_id: z.string(),
  incarnation: U64,
  state_version: U64,
  content_type: z.string(),
  state: Json,
});
export type StateResponse = z.output<typeof StateResponse>;

export const EventsRequest = z.object({
  agent: AgentRef,
  cursor: z.string().nullable(),
  limit: U32,
});
export type EventsRequest = z.output<typeof EventsRequest>;

export const EventBody = z.object({
  event_seq: U64,
  accept_seq: U64,
  turn_id: z.string(),
  event_type: z.string(),
  payload: Json,
});
export type EventBody = z.output<typeof EventBody>;

export const EventsResponse = z.object({
  events: z.array(EventBody),
  next_cursor: z.string(),
});
export type EventsResponse = z.output<typeof EventsResponse>;

export const TimerRequest = z.object({
  agent: AgentRef,
  timer_key: z.string(),
  due_unix_ns: UnixNs,
  handler: z.string(),
  payload: Json,
});
export type TimerRequest = z.output<typeof TimerRequest>;

export const TimerResponse = z.object({ timer: z.string(), generation: U64 });
export type TimerResponse = z.output<typeof TimerResponse>;

export const CancelTimerRequest = z.object({ agent: AgentRef, timer_key: z.string() });
export type CancelTimerRequest = z.output<typeof CancelTimerRequest>;

export const CancelTimerResponse = z.object({
  outcome: z.enum(["cancelled", "already_fired", "unknown"]),
});
export type CancelTimerResponse = z.output<typeof CancelTimerResponse>;

export const EffectBody = z.object({
  effect: z.string(),
  turn_id: z.string(),
  tool_name: z.string(),
  tool_version: z.string(),
  policy: Policy,
  status: EffectState,
  attempt: U32,
});
export type EffectBody = z.output<typeof EffectBody>;

export const EffectsResponse = z.object({ effects: z.array(EffectBody) });
export type EffectsResponse = z.output<typeof EffectsResponse>;

export const ReconcileRequest = z.discriminatedUnion("decision", [
  z.object({ effect: z.string(), attempt: U32, decision: z.literal("retry") }),
  z.object({
    effect: z.string(),
    attempt: U32,
    decision: z.literal("record_result"),
    result: Json,
  }),
]);
export type ReconcileRequest = z.output<typeof ReconcileRequest>;

export const ReconcileResponse = z.object({
  effect: z.string(),
  status: EffectState,
  attempt: U32,
});
export type ReconcileResponse = z.output<typeof ReconcileResponse>;

export const HandlerPinBody = z.object({
  handler: z.string(),
  version: z.string(),
  digest: z.string(),
});
export type HandlerPinBody = z.output<typeof HandlerPinBody>;

export const WorkerPollRequest = z.object({
  worker_id: z.string(),
  handlers: z.array(HandlerPinBody),
  wait_ms: U32,
});
export type WorkerPollRequest = z.output<typeof WorkerPollRequest>;

export const TurnLease = z.object({
  agent_id: z.string(),
  incarnation: U64,
  owner_id: z.string(),
  epoch: U64,
  turn_id: z.string(),
  base_state_version: U64,
  handler_version: z.string(),
  handler_digest: z.string(),
  observed_root_revision: U64,
});
export type TurnLease = z.output<typeof TurnLease>;

export const StepBody = z.object({
  step_id: z.string(),
  step_key: z.string(),
  tool_name: z.string(),
  tool_version: z.string(),
  input_hash: z.string(),
  status: StepState,
  output: Json.nullable(),
});
export type StepBody = z.output<typeof StepBody>;

export const TurnAssignment = z.object({
  lease: TurnLease,
  agent: AgentRef,
  operation: z.string(),
  accept_seq: U64,
  handler: z.string(),
  state: Json,
  payload: Json,
  steps: z.array(StepBody),
});
export type TurnAssignment = z.output<typeof TurnAssignment>;

export const WorkerPollResponse = z.object({ assignment: TurnAssignment.nullable() });
export type WorkerPollResponse = z.output<typeof WorkerPollResponse>;

export const EventSpecBody = z.object({ event_type: z.string(), payload: Json });
export type EventSpecBody = z.output<typeof EventSpecBody>;

export const TimerChangeBody = z.discriminatedUnion("change", [
  z.object({
    change: z.literal("schedule"),
    timer_key: z.string(),
    due_unix_ns: UnixNs,
    handler: z.string(),
    payload: Json,
  }),
  z.object({ change: z.literal("cancel"), timer_key: z.string() }),
]);
export type TimerChangeBody = z.output<typeof TimerChangeBody>;

export const IntentBody = z.object({
  intent_key: z.string(),
  tool_name: z.string(),
  tool_version: z.string(),
  policy: Policy,
  input: Json,
});
export type IntentBody = z.output<typeof IntentBody>;

export const WorkerCommitRequest = z.discriminatedUnion("outcome", [
  z.object({
    lease: TurnLease,
    outcome: z.literal("completed"),
    state: Json,
    output: Json,
    events: z.array(EventSpecBody),
    timers: z.array(TimerChangeBody),
    intents: z.array(IntentBody),
  }),
  z.object({
    lease: TurnLease,
    outcome: z.literal("failed"),
    error: z.string(),
    terminal: z.boolean(),
  }),
]);
export type WorkerCommitRequest = z.output<typeof WorkerCommitRequest>;

export const WorkerCommitResponse = z.object({
  operation: z.string(),
  status: OperationStatus,
  state_version: U64,
  rebased: z.boolean(),
  effects: z.array(z.string()),
});
export type WorkerCommitResponse = z.output<typeof WorkerCommitResponse>;

export const EffectCompleteRequest = z.object({
  effect: z.string(),
  attempt: U32,
  result: Json,
});
export type EffectCompleteRequest = z.output<typeof EffectCompleteRequest>;

export const EffectCompleteResponse = z.object({
  effect: z.string(),
  status: EffectState,
  late: z.boolean(),
});
export type EffectCompleteResponse = z.output<typeof EffectCompleteResponse>;

export const SafeIdsBody = z.object({
  agent_id: z.string().optional(),
  incarnation: U64.optional(),
  operation: z.string().optional(),
  turn_id: z.string().optional(),
  step_id: z.string().optional(),
  effect_id: z.string().optional(),
  timer_id: z.string().optional(),
});
export type SafeIdsBody = z.output<typeof SafeIdsBody>;

export const ErrorBody = z.object({
  code: z.string(),
  retry: z.string(),
  reason: z.string(),
  ids: SafeIdsBody,
});
export type ErrorBody = z.output<typeof ErrorBody>;

/**
 * Each golden fixture under `sdk/fixtures/`, by file stem, and the schema
 * that reads it. `wire.test.ts` fails on a fixture missing here and on a
 * request or response schema missing from here.
 */
export const WIRE_FIXTURES = {
  call_request: CallRequest,
  call_response: CallResponse,
  cancel_request: CancelRequest,
  cancel_response: CancelResponse,
  cancel_timer_request: CancelTimerRequest,
  cancel_timer_response: CancelTimerResponse,
  create_agent_request: CreateAgentRequest,
  create_agent_response: CreateAgentResponse,
  effect_complete_request: EffectCompleteRequest,
  effect_complete_response: EffectCompleteResponse,
  effects_response: EffectsResponse,
  error_body: ErrorBody,
  events_request: EventsRequest,
  events_response: EventsResponse,
  operation_response: OperationResponse,
  reconcile_request: ReconcileRequest,
  reconcile_response: ReconcileResponse,
  resolve_request: ResolveRequest,
  resolve_response: ResolveResponse,
  result_response: ResultResponse,
  result_response_nanosecond: ResultResponse,
  send_request: SendRequest,
  send_response: SendResponse,
  state_response: StateResponse,
  timer_request: TimerRequest,
  timer_response: TimerResponse,
  worker_commit_failed_request: WorkerCommitRequest,
  worker_commit_request: WorkerCommitRequest,
  worker_commit_response: WorkerCommitResponse,
  worker_poll_request: WorkerPollRequest,
  worker_poll_response: WorkerPollResponse,
} as const;

/** Decode a parsed JSON body; throws a ZodError on a body the schema refuses. */
export function decodeWire<S extends z.ZodType>(schema: S, value: unknown): z.output<S> {
  return z.decode(schema, value as z.input<S>);
}

/** Encode to the JSON value the wire carries (bigints become decimal strings). */
export function encodeWire<S extends z.ZodType>(schema: S, value: z.output<S>): z.input<S> {
  return z.encode(schema, value);
}
