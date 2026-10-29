/**
 * Schemas for the JSON bodies of the `/api/v1/agents/...` routes, mirroring
 * `crates/ravel-agents-core/src/wire.rs`. Field names and order follow the
 * Rust structs, so `encodeWire` reproduces a fixture byte for byte
 * (`wire.test.ts`). Every `*_unix_ns` field is a canonical decimal string on
 * the wire and a `bigint` in TypeScript, refused outside i64; u64 counters are numbers refused
 * past `Number.MAX_SAFE_INTEGER` rather than rounded.
 */
import { z } from "zod";

const I64_MIN = -(2n ** 63n);
const I64_MAX = 2n ** 63n - 1n;
const UnixNs = z.codec(
  z
    .string()
    .regex(/^(0|-?[1-9][0-9]*)$/, { abort: true })
    .refine((text) => BigInt(text) >= I64_MIN && BigInt(text) <= I64_MAX, "outside i64"),
  z.bigint().min(I64_MIN).max(I64_MAX),
  {
    decode: (text) => BigInt(text),
    encode: (value) => value.toString(),
  },
);
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

export const ActiveTurnBody = z.object({
  turn_id: z.string(),
  accept_seq: U64,
  handler: z.string(),
  handler_version: z.string(),
  handler_digest: z.string(),
  base_state_version: U64,
  started_unix_ns: UnixNs,
  lease_hint_unix_ns: UnixNs,
  cancel_requested: z.boolean(),
  non_deterministic_replay: z.boolean(),
});
export type ActiveTurnBody = z.output<typeof ActiveTurnBody>;

export const InputSourceBody = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("message"), request_id: z.string() }),
  z.object({ kind: z.literal("timer"), timer: z.string(), generation: U64 }),
]);
export type InputSourceBody = z.output<typeof InputSourceBody>;

export const MailboxEntryBody = z.object({
  accept_seq: U64,
  handler: z.string(),
  source: InputSourceBody,
  accepted_unix_ns: UnixNs,
  failed_attempts: U32,
});
export type MailboxEntryBody = z.output<typeof MailboxEntryBody>;

/** The counts behind each lifetime cap of one agent, from its HEAD as stored. */
export const UsageBody = z.object({
  receipts: U64,
  max_receipts: U64,
  results_pages: U64,
  events_pages: U64,
  timers_pages: U64,
  max_index_pages: U64,
  head_bytes: U64,
  max_head_bytes: U64,
});
export type UsageBody = z.output<typeof UsageBody>;

export const StateResponse = z.object({
  agent_id: z.string(),
  incarnation: U64,
  state_version: U64,
  content_type: z.string(),
  state: Json,
  active_turn: ActiveTurnBody.optional(),
  mailbox: z.array(MailboxEntryBody).optional(),
  usage: UsageBody.optional(),
});
export type StateResponse = z.output<typeof StateResponse>;

export const EventsRequest = z.object({
  agent: AgentRef,
  cursor: z.string().nullable(),
  limit: U32,
  operation: z.string().optional(),
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
  terminal: OperationStatus.optional(),
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

export const TimerBody = z.object({
  timer: z.string(),
  timer_key: z.string(),
  generation: U64,
  due_unix_ns: UnixNs,
  handler: z.string(),
  status: z.enum(["scheduled", "delivered", "cancelled"]),
});
export type TimerBody = z.output<typeof TimerBody>;

export const TimersResponse = z.object({ timers: z.array(TimerBody) });
export type TimersResponse = z.output<typeof TimersResponse>;

export const SweepResponse = z.object({
  wake_list_pages: U64,
  dir_list_pages: U64,
  head_gets: U64,
  hints_read: U64,
  hints_deleted: U64,
  timers_fired: U64,
  agents_loaded: U64,
  dir_entries_repaired: U64,
});
export type SweepResponse = z.output<typeof SweepResponse>;

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

export const AttemptState = z.enum(["open", "in_doubt", "result", "superseded"]);
export type AttemptState = z.output<typeof AttemptState>;

export const EffectAttemptBody = z.object({ attempt: U32, status: AttemptState });
export type EffectAttemptBody = z.output<typeof EffectAttemptBody>;

export const EffectEvidenceBody = z.object({
  attempt: U32,
  result: Json,
  failure: z.string().optional(),
});
export type EffectEvidenceBody = z.output<typeof EffectEvidenceBody>;

export const EffectInspectResponse = EffectBody.extend({
  step_key: z.string(),
  result_attempt: U32.nullable(),
  result: Json.nullable(),
  failure: z.string().optional(),
  attempts: z.array(EffectAttemptBody),
  evidence: z.array(EffectEvidenceBody),
});
export type EffectInspectResponse = z.output<typeof EffectInspectResponse>;

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

/**
 * The lease a poll hands out: the commit-time fields plus the turn's
 * recorded time and random seed, both fixed by the server so a retried turn
 * replays with the same clock and the same random stream.
 */
export const PolledLease = TurnLease.extend({
  recorded_unix_ns: UnixNs,
  random_seed: z.string().regex(/^[0-9a-f]{32}$/),
});
export type PolledLease = z.output<typeof PolledLease>;

/** The fields of a polled lease that heartbeat, step and commit bodies carry. */
export function heldLease(lease: PolledLease): TurnLease {
  return {
    agent_id: lease.agent_id,
    incarnation: lease.incarnation,
    owner_id: lease.owner_id,
    epoch: lease.epoch,
    turn_id: lease.turn_id,
    base_state_version: lease.base_state_version,
    handler_version: lease.handler_version,
    handler_digest: lease.handler_digest,
    observed_root_revision: lease.observed_root_revision,
  };
}

export const WorkerRegisterRequest = z.object({ handlers: z.array(HandlerPinBody) });
export type WorkerRegisterRequest = z.output<typeof WorkerRegisterRequest>;

export const WorkerRegisterResponse = z.object({ worker_id: z.string() });
export type WorkerRegisterResponse = z.output<typeof WorkerRegisterResponse>;

export const WorkerHeartbeatRequest = z.object({ lease: TurnLease });
export type WorkerHeartbeatRequest = z.output<typeof WorkerHeartbeatRequest>;

export const WorkerHeartbeatResponse = z.object({ lease_expires_unix_ns: UnixNs });
export type WorkerHeartbeatResponse = z.output<typeof WorkerHeartbeatResponse>;

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
  lease: PolledLease,
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

/**
 * `input_hash` is `stepInputHash(tool_name, tool_version, canonicalBytes(input))`;
 * `sdk/fixtures/identity/step_input_hash.json` pins the encoding.
 */
export const StepRecordRequest = z.object({
  lease: TurnLease,
  step_key: z.string(),
  tool_name: z.string(),
  tool_version: z.string(),
  policy: Policy,
  input_hash: z.string(),
});
export type StepRecordRequest = z.output<typeof StepRecordRequest>;

export const StepRecordResponse = z.object({
  step: StepBody,
  effect: z.string(),
  attempt: U32,
  failure: z.string().nullable(),
});
export type StepRecordResponse = z.output<typeof StepRecordResponse>;

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

export const EffectFailRequest = z.object({
  effect: z.string(),
  attempt: U32,
  error: z.string(),
});
export type EffectFailRequest = z.output<typeof EffectFailRequest>;

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
  /** The lifetime cap a `budget_exceeded` or `mailbox_full` refusal hit. */
  cap: z.string().optional(),
});
export type ErrorBody = z.output<typeof ErrorBody>;

/** The `410` body of `events` for a cursor below the retained window. */
export const CursorExpiredBody = z.object({
  code: z.literal("cursor_expired"),
  cursor: z.string(),
  oldest_cursor: z.string(),
  restart: z.enum(["restart_from_oldest_cursor"]),
  reason: z.string(),
  ids: SafeIdsBody,
});
export type CursorExpiredBody = z.output<typeof CursorExpiredBody>;

/**
 * The server-wide body of a 401 or 403, which the agents routes share with
 * every other route rather than answering with an {@link ErrorBody}.
 */
export const AuthErrorBody = z.object({
  status: z.literal("error"),
  errorType: z.string(),
  error: z.string(),
});
export type AuthErrorBody = z.output<typeof AuthErrorBody>;

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
  cursor_expired: CursorExpiredBody,
  effect_complete_request: EffectCompleteRequest,
  effect_complete_response: EffectCompleteResponse,
  effect_fail_request: EffectFailRequest,
  effect_inspect_response: EffectInspectResponse,
  effect_inspect_response_failure: EffectInspectResponse,
  effects_response: EffectsResponse,
  error_body: ErrorBody,
  error_body_cap: ErrorBody,
  events_request: EventsRequest,
  events_request_operation: EventsRequest,
  events_response: EventsResponse,
  events_response_terminal: EventsResponse,
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
  state_response_active: StateResponse,
  step_record_request: StepRecordRequest,
  step_record_response: StepRecordResponse,
  sweep_response: SweepResponse,
  timer_request: TimerRequest,
  timer_response: TimerResponse,
  timers_response: TimersResponse,
  worker_commit_failed_request: WorkerCommitRequest,
  worker_commit_request: WorkerCommitRequest,
  worker_commit_response: WorkerCommitResponse,
  worker_heartbeat_request: WorkerHeartbeatRequest,
  worker_heartbeat_response: WorkerHeartbeatResponse,
  worker_poll_request: WorkerPollRequest,
  worker_poll_response: WorkerPollResponse,
  worker_register_request: WorkerRegisterRequest,
  worker_register_response: WorkerRegisterResponse,
} as const;

/** Decode a parsed JSON body; throws a ZodError on a body the schema refuses. */
export function decodeWire<S extends z.ZodType>(schema: S, value: unknown): z.output<S> {
  return z.decode(schema, value as z.input<S>);
}

/** Encode to the JSON value the wire carries (bigints become decimal strings). */
export function encodeWire<S extends z.ZodType>(schema: S, value: z.output<S>): z.input<S> {
  return z.encode(schema, value);
}
