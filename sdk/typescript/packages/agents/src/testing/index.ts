/**
 * Testing entry: run agents and tools in process against an in-memory fake
 * coordinator, with control over time, ownership and crashes. Server-only.
 *
 * Nothing here is durable. The fake keeps every agent, operation, step and
 * event in process memory and loses all of it on `close()`; it is a model of
 * the coordinator's protocol, not of its storage.
 */
import type { z } from "zod";
import { createClient, type AgentClient, type RavelAgentsClient } from "../client/index.js";
import {
  AgentGone,
  EffectInDoubt,
  IdempotencyConflict,
  NonDeterministicReplay,
  OwnershipLost,
  type RavelAgentsError,
  RavelAgentsError as RavelAgentsErrorClass,
  UnsupportedFormat,
} from "../errors.js";
import { agentId, canonicalBytes, effectId, requestHash, stepId, turnId } from "../identity.js";
import type { AnyAgentDefinition, AnyToolDefinition } from "../types.js";
import { bodyFromError, ERROR_CODES } from "../wire/errors.js";
import {
  CallRequest,
  CallResponse,
  CancelRequest,
  CancelResponse,
  CreateAgentRequest,
  CreateAgentResponse,
  type EffectCompleteRequest,
  type EffectCompleteResponse,
  type EffectState,
  type EventBody,
  encodeWire,
  EventsRequest,
  EventsResponse,
  OperationResponse,
  type OperationStatus,
  type Policy,
  ResolveRequest,
  ResolveResponse,
  type ResultBody,
  ResultResponse,
  SendRequest,
  SendResponse,
  StateResponse,
  type StepBody,
  TERMINAL_STATUSES,
  type TurnLease,
  type WorkerCommitRequest,
  type WorkerCommitResponse,
  type WorkerPollRequest,
} from "../wire/index.js";
import { serveAgents, type TurnReport, type WorkerHandle } from "../worker/index.js";
import type { PolledTurn, StepRecordRequest, StepRecordResponse, Transport } from "../worker/transport.js";

type Json = z.output<typeof SendRequest>["payload"];

/** 2026-01-01T00:00:00Z, the fake's clock at creation. */
const DEFAULT_START_UNIX_NS = 1_767_225_600_000_000_000n;
const API = "/api/v1/agents/";

export interface ReceivedRequest {
  readonly method: string;
  /** The path after `/api/v1/agents/`, e.g. `send`. */
  readonly route: string;
  readonly query: Readonly<Record<string, string>>;
  /** The parsed JSON body, as received. */
  readonly body: unknown;
  /** For `send` and `call`: the request id and the request hash the fake recorded. */
  readonly requestId?: string;
  readonly requestHash?: string;
}

export interface FailureInjection {
  readonly status: number;
  /** A snake_case error code; absent sends an empty body. */
  readonly code?: string;
  /** How many requests to fail. Default 1. */
  readonly times?: number;
}

interface AgentRecord {
  readonly agentId: string;
  readonly kind: string;
  readonly key: string;
  readonly incarnation: number;
  stateVersion: number;
  state: Json;
  epoch: number;
  nextAcceptSeq: number;
  readonly events: EventBody[];
  /** The operation this agent's one turn at a time is leased for. */
  leased: { readonly operation: string; readonly lease: TurnLease } | undefined;
}

interface StepRecord {
  readonly stepId: string;
  readonly stepKey: string;
  readonly toolName: string;
  readonly toolVersion: string;
  readonly inputHash: string;
  readonly policy: Policy;
  readonly effect: string;
  attempt: number;
  effectState: EffectState;
  output: Json | null;
}

interface OperationRecord {
  readonly id: string;
  readonly agent: AgentRecord;
  readonly handler: string;
  readonly payload: Json;
  readonly acceptSeq: number;
  status: OperationStatus;
  failedAttempts: number;
  readonly steps: Map<string, StepRecord>;
  blockedEffect: string | null;
  blockedReason: string | null;
  cancelRequested: boolean;
  startedUnixNs: bigint | undefined;
  result: ResultBody | null;
}

const ERROR_STATUS: Readonly<Record<string, number>> = {
  agent_gone: 404,
  idempotency_conflict: 409,
  ownership_lost: 409,
  mailbox_full: 429,
  store_unavailable: 503,
  outcome_unknown: 504,
};

function aborted(signal: AbortSignal | undefined): boolean {
  return signal?.aborted === true;
}

function errorFor(code: string, reason: string): RavelAgentsError {
  const Class = ERROR_CODES[code] ?? UnsupportedFormat;
  return new Class(reason);
}

export interface FakeCoordinatorOptions {
  /** Agent definitions whose `initialState` an agent starts from. */
  readonly agents: readonly AnyAgentDefinition[];
  readonly startUnixNs?: bigint;
  /** Attempts at one input before a retryable failure dead-letters it. Default 3. */
  readonly maxAttempts?: number;
}

/**
 * An in-memory coordinator. It implements the worker {@link Transport} and,
 * through {@link FakeCoordinator.fetch}, the client routes, over the same wire
 * bodies a server would send. Every agent is one incarnation; there are no
 * timers, intents or reconciliation route.
 */
export class FakeCoordinator implements Transport {
  /** Every client request, in arrival order, including ones answered with an injected failure. */
  readonly received: ReceivedRequest[] = [];
  /** Every commit the fake accepted. */
  readonly commits: WorkerCommitRequest[] = [];

  private readonly definitions: ReadonlyMap<string, AnyAgentDefinition>;
  private readonly maxAttempts: number;
  private readonly agents = new Map<string, AgentRecord>();
  private readonly operations = new Map<string, OperationRecord>();
  private readonly receipts = new Map<string, { operation: string; hash: string }>();
  private readonly effects = new Map<string, { operation: OperationRecord; step: StepRecord }>();
  private readonly failures = new Map<string, FailureInjection & { remaining: number }>();
  private readonly delays = new Map<string, number>();
  private waiters = new Set<() => void>();
  private clock: bigint;
  private nextOperation = 1;
  private closed = false;

  constructor(options: FakeCoordinatorOptions) {
    this.definitions = new Map(options.agents.map((a) => [a.name, a]));
    this.maxAttempts = options.maxAttempts ?? 3;
    this.clock = options.startUnixNs ?? DEFAULT_START_UNIX_NS;
  }

  /** The fake's clock in Unix nanoseconds. */
  get nowUnixNs(): bigint {
    return this.clock;
  }

  advanceTime(ms: number): void {
    this.clock += BigInt(Math.round(ms * 1_000_000));
    this.notify();
  }

  /** Answer the next `times` requests to `route` with `status` and, given a code, an `ErrorBody`. */
  failNext(route: string, failure: FailureInjection): void {
    this.failures.set(route, { ...failure, remaining: failure.times ?? 1 });
  }

  /** Process the next request to `route`, then hold its response for `ms`. */
  delayNext(route: string, ms: number): void {
    this.delays.set(route, ms);
  }

  /** Drop every current lease, as a crashed worker's would expire; the turns are leased again. */
  revokeLeases(): void {
    for (const agent of this.agents.values()) {
      if (agent.leased === undefined) continue;
      const op = this.operations.get(agent.leased.operation);
      if (op?.status === "running") op.status = "accepted";
      agent.leased = undefined;
    }
    this.notify();
  }

  /** The operation's journaled steps, for assertions. */
  steps(operation: string): StepBody[] {
    const op = this.operations.get(operation);
    return op === undefined ? [] : [...op.steps.values()].map((s) => this.stepBody(s));
  }

  close(): void {
    this.closed = true;
    this.notify();
  }

  private notify(): void {
    const waiters = this.waiters;
    this.waiters = new Set();
    for (const wake of waiters) wake();
  }

  /** Resolve on the next state change or after `ms`; reject when `signal` aborts. */
  private changed(ms: number, signal: AbortSignal | undefined): Promise<void> {
    return new Promise((resolve, reject) => {
      if (signal?.aborted === true) {
        reject(signal.reason as Error);
        return;
      }
      const done = () => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        this.waiters.delete(done);
        resolve();
      };
      const onAbort = () => {
        clearTimeout(timer);
        this.waiters.delete(done);
        reject(signal?.reason as Error);
      };
      const timer = setTimeout(done, ms);
      this.waiters.add(done);
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  }

  private agentFor(kind: string, key: string, create: boolean): AgentRecord {
    const id = `${kind}\0${key}`;
    const existing = this.agents.get(id);
    if (existing !== undefined) return existing;
    const definition = this.definitions.get(kind);
    if (!create || definition === undefined) {
      throw new AgentGone(`no agent ${kind}/${key}`);
    }
    const agent: AgentRecord = {
      agentId: agentId(kind, key),
      kind,
      key,
      incarnation: 1,
      stateVersion: 0,
      state: definition.initialState as Json,
      epoch: 0,
      nextAcceptSeq: 1,
      events: [],
      leased: undefined,
    };
    this.agents.set(id, agent);
    return agent;
  }

  private stepBody(s: StepRecord): StepBody {
    return {
      step_id: s.stepId,
      step_key: s.stepKey,
      tool_name: s.toolName,
      tool_version: s.toolVersion,
      input_hash: s.inputHash,
      status: s.effectState === "completed" ? "completed" : "started",
      output: s.output,
    };
  }

  private finish(op: OperationRecord, result: Omit<ResultBody, "finished_unix_ns">): void {
    op.status = result.status;
    op.result = { ...result, finished_unix_ns: this.clock };
  }

  private block(op: OperationRecord, reason: string, effect: string | null): void {
    op.status = "blocked";
    op.blockedReason = reason;
    op.blockedEffect = effect;
    if (op.agent.leased?.operation === op.id) op.agent.leased = undefined;
    this.notify();
  }

  // ---- Transport -------------------------------------------------------

  async poll(request: WorkerPollRequest, signal: AbortSignal): Promise<PolledTurn | null> {
    const until = Date.now() + request.wait_ms;
    for (;;) {
      if (this.closed) return null;
      const turn = this.lease(request);
      if (turn !== null) return turn;
      const left = until - Date.now();
      if (left <= 0) return null;
      await this.changed(left, signal);
    }
  }

  private lease(request: WorkerPollRequest): PolledTurn | null {
    for (const agent of this.agents.values()) {
      if (agent.leased !== undefined) continue;
      // One turn per agent at a time, in accept order; a blocked operation holds the mailbox.
      const next = [...this.operations.values()]
        .filter((op) => op.agent === agent && !TERMINAL_STATUSES.includes(op.status))
        .sort((a, b) => a.acceptSeq - b.acceptSeq)[0];
      if (next?.status !== "accepted") continue;
      const definition = this.definitions.get(agent.kind);
      const pin = request.handlers.find(
        (h) => h.handler === next.handler && h.version === definition?.version,
      );
      if (pin === undefined) continue;
      agent.epoch += 1;
      next.status = "running";
      next.startedUnixNs ??= this.clock;
      const lease: TurnLease = {
        agent_id: agent.agentId,
        incarnation: agent.incarnation,
        owner_id: request.worker_id,
        epoch: agent.epoch,
        turn_id: turnId(agent.incarnation, next.acceptSeq, agent.epoch, next.failedAttempts),
        base_state_version: agent.stateVersion,
        handler_version: pin.version,
        handler_digest: pin.digest,
        observed_root_revision: agent.stateVersion,
      };
      agent.leased = { operation: next.id, lease };
      return {
        assignment: structuredClone({
          lease,
          agent: { kind: agent.kind, key: agent.key },
          operation: next.id,
          accept_seq: next.acceptSeq,
          handler: next.handler,
          state: agent.state,
          payload: next.payload,
          steps: [...next.steps.values()].map((s) => this.stepBody(s)),
        }),
        startedUnixNs: next.startedUnixNs,
      };
    }
    return null;
  }

  /** The operation `lease` holds, or `OwnershipLost` when it no longer holds it. */
  private fenced(lease: TurnLease): OperationRecord {
    for (const agent of this.agents.values()) {
      const held = agent.leased;
      if (agent.agentId !== lease.agent_id) continue;
      if (held?.lease.epoch === lease.epoch && held.lease.owner_id === lease.owner_id) {
        const op = this.operations.get(held.operation);
        if (op !== undefined) return op;
      }
    }
    throw new OwnershipLost(`lease at epoch ${String(lease.epoch)} no longer holds its turn`, {
      ids: { agentId: lease.agent_id, turnId: lease.turn_id, epoch: lease.epoch },
    });
  }

  recordStep(request: StepRecordRequest): Promise<StepRecordResponse> {
    const op = this.fenced(request.lease);
    const ids = { agentId: op.agent.agentId, operationId: op.id, turnId: request.lease.turn_id, stepKey: request.step_key };
    const existing = op.steps.get(request.step_key);
    if (existing !== undefined) {
      if (
        existing.toolName !== request.tool_name ||
        existing.toolVersion !== request.tool_version ||
        existing.inputHash !== request.input_hash
      ) {
        const reason = `step ${request.step_key} reused with a different tool or input hash`;
        this.block(op, reason, null);
        return Promise.reject(new NonDeterministicReplay(reason, { ids: { ...ids, stepId: existing.stepId } }));
      }
      if (existing.effectState !== "completed") {
        if (existing.policy === "manual") {
          existing.effectState = "in_doubt";
          const reason = `effect ${existing.effect} of manual step ${request.step_key} is in doubt`;
          this.block(op, reason, existing.effect);
          return Promise.reject(new EffectInDoubt(reason, { ids: { ...ids, effectId: existing.effect } }));
        }
        existing.attempt += 1;
        existing.effectState = "authorized";
      }
      return Promise.resolve({ step: this.stepBody(existing), effect: existing.effect, attempt: existing.attempt });
    }
    const inc = op.agent.incarnation;
    const step: StepRecord = {
      stepId: stepId(inc, op.acceptSeq, request.step_key),
      stepKey: request.step_key,
      toolName: request.tool_name,
      toolVersion: request.tool_version,
      inputHash: request.input_hash,
      policy: request.policy,
      effect: effectId(inc, op.acceptSeq, request.step_key),
      attempt: 1,
      effectState: "authorized",
      output: null,
    };
    op.steps.set(step.stepKey, step);
    this.effects.set(step.effect, { operation: op, step });
    return Promise.resolve({ step: this.stepBody(step), effect: step.effect, attempt: 1 });
  }

  completeEffect(request: EffectCompleteRequest): Promise<EffectCompleteResponse> {
    const found = this.effects.get(request.effect);
    if (found === undefined) {
      return Promise.reject(new UnsupportedFormat(`unknown effect ${request.effect}`));
    }
    const { step } = found;
    const late = step.effectState === "completed" || request.attempt !== step.attempt;
    if (!late) {
      step.effectState = "completed";
      step.output = request.result;
    }
    return Promise.resolve({ effect: step.effect, status: step.effectState, late });
  }

  commit(request: WorkerCommitRequest): Promise<WorkerCommitResponse> {
    let op: OperationRecord;
    try {
      op = this.fenced(request.lease);
    } catch (error) {
      return Promise.reject(error as Error);
    }
    const agent = op.agent;
    agent.leased = undefined;
    this.commits.push(structuredClone(request));
    if (op.cancelRequested) {
      this.finish(op, { status: "cancelled", output: null, error: "cancelled", dead_lettered: false });
    } else if (request.outcome === "completed") {
      agent.state = request.state;
      agent.stateVersion += 1;
      for (const event of request.events) {
        agent.events.push({
          event_seq: agent.events.length + 1,
          accept_seq: op.acceptSeq,
          turn_id: request.lease.turn_id,
          event_type: event.event_type,
          payload: event.payload,
        });
      }
      this.finish(op, { status: "completed", output: request.output, error: null, dead_lettered: false });
    } else {
      op.failedAttempts += 1;
      if (request.terminal) {
        this.finish(op, { status: "failed", output: null, error: request.error, dead_lettered: false });
      } else if (op.failedAttempts >= this.maxAttempts) {
        this.finish(op, { status: "failed", output: null, error: request.error, dead_lettered: true });
      } else {
        op.status = "accepted";
      }
    }
    this.notify();
    return Promise.resolve({
      operation: op.id,
      status: op.status,
      state_version: agent.stateVersion,
      rebased: false,
      effects: [],
    });
  }

  // ---- Client routes ---------------------------------------------------

  /** A `fetch` serving the client routes under `/api/v1/agents/`. */
  readonly fetch = async (input: string | URL | Request, init: RequestInit = {}): Promise<Response> => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    const method = init.method ?? "GET";
    const signal = init.signal ?? undefined;
    if (signal?.aborted === true) throw signal.reason;
    const route = url.pathname.startsWith(API) ? url.pathname.slice(API.length) : url.pathname;
    const query = Object.fromEntries(url.searchParams);
    const text = typeof init.body === "string" ? init.body : undefined;
    const body: unknown = text === undefined ? undefined : JSON.parse(text);
    const received: { -readonly [K in keyof ReceivedRequest]: ReceivedRequest[K] } = { method, route, query, body };
    if ((route === "send" || route === "call") && typeof body === "object" && body !== null) {
      const b = body as { handler?: unknown; request_id?: unknown; payload?: unknown };
      if (typeof b.request_id === "string") received.requestId = b.request_id;
      if (typeof b.handler === "string") received.requestHash = requestHash(b.handler, canonicalBytes(b.payload));
    }
    this.received.push(received);

    const failure = this.failures.get(route);
    if (failure !== undefined) {
      failure.remaining -= 1;
      if (failure.remaining <= 0) this.failures.delete(route);
      const errorBody = failure.code === undefined ? "" : JSON.stringify(bodyFromError(errorFor(failure.code, "injected")));
      return new Response(errorBody, { status: failure.status });
    }

    let response: Response;
    try {
      response = new Response(JSON.stringify(await this.route(method, route, query, body, signal)), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    } catch (error) {
      if (aborted(signal)) throw signal?.reason;
      if (!(error instanceof RavelAgentsErrorClass)) throw error;
      const wire = bodyFromError(error);
      response = new Response(JSON.stringify(wire), { status: ERROR_STATUS[wire.code] ?? 400 });
    }
    const delay = this.delays.get(route);
    if (delay !== undefined) {
      this.delays.delete(route);
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(resolve, delay);
        signal?.addEventListener(
          "abort",
          () => {
            clearTimeout(timer);
            reject(signal.reason as Error);
          },
          { once: true },
        );
      });
    }
    return response;
  };

  private operation(id: unknown): OperationRecord {
    const op = typeof id === "string" ? this.operations.get(id) : undefined;
    if (op === undefined) throw new AgentGone(`unknown operation ${String(id)}`);
    return op;
  }

  private accept(agentRef: { kind: string; key: string }, handler: string, requestId: string, payload: Json) {
    const agent = this.agentFor(agentRef.kind, agentRef.key, true);
    const receipt = `${agent.agentId}\0${handler}\0${requestId}`;
    const hash = requestHash(handler, canonicalBytes(payload));
    const prior = this.receipts.get(receipt);
    if (prior !== undefined) {
      if (prior.hash !== hash) {
        throw new IdempotencyConflict(`request ${requestId} reused with a different payload`, {
          ids: { agentId: agent.agentId, requestId, operationId: prior.operation },
        });
      }
      return { op: this.operation(prior.operation), duplicate: true };
    }
    const op: OperationRecord = {
      id: `op-${String(this.nextOperation++)}`,
      agent,
      handler,
      payload,
      acceptSeq: agent.nextAcceptSeq++,
      status: "accepted",
      failedAttempts: 0,
      steps: new Map(),
      blockedEffect: null,
      blockedReason: null,
      cancelRequested: false,
      startedUnixNs: undefined,
      result: null,
    };
    this.operations.set(op.id, op);
    this.receipts.set(receipt, { operation: op.id, hash });
    this.notify();
    return { op, duplicate: false };
  }

  private async waitTerminal(op: OperationRecord, waitMs: number, signal: AbortSignal | undefined): Promise<void> {
    const until = Date.now() + waitMs;
    while (op.result === null && !this.closed) {
      const left = until - Date.now();
      if (left <= 0) return;
      await this.changed(left, signal);
    }
  }

  private async route(
    method: string,
    route: string,
    query: Record<string, string>,
    body: unknown,
    signal: AbortSignal | undefined,
  ): Promise<unknown> {
    switch (`${method} ${route}`) {
      case "POST create": {
        const req = CreateAgentRequest.parse(body);
        const created = !this.agents.has(`${req.agent.kind}\0${req.agent.key}`);
        const agent = this.agentFor(req.agent.kind, req.agent.key, true);
        return encodeWire(CreateAgentResponse, { agent_id: agent.agentId, incarnation: agent.incarnation, created });
      }
      case "POST send": {
        const req = SendRequest.parse(body);
        const { op, duplicate } = this.accept(req.agent, req.handler, req.request_id, req.payload);
        return encodeWire(SendResponse, { operation: op.id, accept_seq: op.acceptSeq, duplicate });
      }
      case "POST call": {
        const req = CallRequest.parse(body);
        const { op } = this.accept(req.agent, req.handler, req.request_id, req.payload);
        await this.waitTerminal(op, req.wait_ms, signal);
        return encodeWire(CallResponse, { operation: op.id, status: op.status, result: op.result });
      }
      case "GET operation": {
        const op = this.operation(query.operation);
        return encodeWire(OperationResponse, {
          operation: op.id,
          status: op.status,
          accept_seq: op.acceptSeq,
          blocked_effect: op.blockedEffect,
        });
      }
      case "GET result": {
        const op = this.operation(query.operation);
        await this.waitTerminal(op, Math.min(Number(query.wait_ms ?? 0), 20_000), signal);
        return encodeWire(ResultResponse, { operation: op.id, result: op.result });
      }
      case "POST resolve": {
        const req = ResolveRequest.parse(body);
        const agent = this.agents.get(`${req.agent.kind}\0${req.agent.key}`);
        const prior =
          agent === undefined ? undefined : this.receipts.get(`${agent.agentId}\0${req.handler}\0${req.request_id}`);
        const op = prior === undefined ? undefined : this.operations.get(prior.operation);
        return encodeWire(ResolveResponse, { operation: op?.id ?? null, status: op?.status ?? null });
      }
      case "POST cancel": {
        const op = this.operation(CancelRequest.parse(body).operation);
        let requested = false;
        if (!TERMINAL_STATUSES.includes(op.status)) {
          if (op.agent.leased?.operation === op.id) {
            op.cancelRequested = true;
            requested = true;
          } else {
            this.finish(op, { status: "cancelled", output: null, error: "cancelled", dead_lettered: false });
            this.notify();
          }
        }
        return encodeWire(CancelResponse, { operation: op.id, status: op.status, requested });
      }
      case "GET state": {
        const agent = this.agentFor(query.kind ?? "", query.key ?? "", false);
        return encodeWire(StateResponse, {
          agent_id: agent.agentId,
          incarnation: agent.incarnation,
          state_version: agent.stateVersion,
          content_type: "application/json",
          state: agent.state,
        });
      }
      case "POST events": {
        const req = EventsRequest.parse(body);
        const agent = this.agents.get(`${req.agent.kind}\0${req.agent.key}`);
        const from = req.cursor === null ? 0 : Number(req.cursor);
        if (!Number.isInteger(from) || from < 0) throw new UnsupportedFormat(`bad cursor ${String(req.cursor)}`);
        const events = (agent?.events ?? []).slice(from, from + req.limit);
        return encodeWire(EventsResponse, { events, next_cursor: String(from + events.length) });
      }
      default:
        throw new UnsupportedFormat(`no route ${method} ${route}`);
    }
  }
}

export interface TestHarnessOptions {
  readonly agents: readonly AnyAgentDefinition[];
  readonly tools?: readonly AnyToolDefinition[];
  readonly startUnixNs?: bigint;
  readonly maxAttempts?: number;
  /** Client retry delay; default 1 ms so retry tests stay fast. */
  readonly retryDelayMs?: number;
}

export interface TestHarness {
  readonly coordinator: FakeCoordinator;
  /** A client whose `fetch` is the fake coordinator's. */
  readonly client: RavelAgentsClient;
  /** Every turn the harness's workers finished or abandoned. */
  readonly turns: readonly TurnReport[];
  agent<A extends AnyAgentDefinition>(definition: A, key: string): AgentClient<A>;
  /** Advance the fake's clock. Turns already started keep their recorded start time. */
  advanceTime(ms: number): Promise<void>;
  /**
   * Kill the current worker mid-turn: its lease is dropped and its in-flight
   * turn is never committed. A new worker leases the same turn under a new
   * epoch and receives the steps journaled so far.
   */
  crashWorker(): Promise<void>;
  close(): Promise<void>;
}

/** A fake coordinator, a client over it, and one worker serving `agents`. */
export function createTestHarness(options: TestHarnessOptions): TestHarness {
  const coordinator = new FakeCoordinator({
    agents: options.agents,
    ...(options.startUnixNs === undefined ? {} : { startUnixNs: options.startUnixNs }),
    ...(options.maxAttempts === undefined ? {} : { maxAttempts: options.maxAttempts }),
  });
  const client = createClient({
    url: "http://fake.invalid",
    token: "test",
    fetch: coordinator.fetch,
    retryDelayMs: options.retryDelayMs ?? 1,
  });
  const turns: TurnReport[] = [];
  let generation = 0;
  const start = (): Promise<WorkerHandle> =>
    serveAgents({
      transport: coordinator,
      agents: options.agents,
      ...(options.tools === undefined ? {} : { tools: options.tools }),
      workerId: `harness-worker-${String(++generation)}`,
      pollWaitMs: 20_000,
      onTurn: (report) => turns.push(report),
    });
  let worker = start();

  return {
    coordinator,
    client,
    turns,
    agent: (definition, key) => client.agent(definition.name, key),
    advanceTime: (ms) => {
      coordinator.advanceTime(ms);
      return Promise.resolve();
    },
    crashWorker: async () => {
      const old = await worker;
      coordinator.revokeLeases();
      await old.close();
      worker = start();
    },
    close: async () => {
      const current = await worker;
      await current.close();
      coordinator.close();
    },
  };
}
