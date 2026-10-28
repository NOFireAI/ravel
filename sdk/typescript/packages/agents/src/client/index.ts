/**
 * Client entry: send messages to agents and follow their operations. It
 * imports no module from this package's worker, tools or testing entries and
 * none of the child_process, fs and net builtins, which index.test.ts
 * enforces over this package's own module graph; the test does not follow
 * third-party packages.
 */
import type { z } from "zod";
import { Forbidden, OutcomeUnknown, UnsupportedFormat } from "../errors.js";
import type { AnyAgentDefinition, MessageNames, PayloadOf, ResultOf } from "../types.js";
import { errorFromBody } from "../wire/errors.js";
import {
  AuthErrorBody,
  CallRequest,
  CallResponse,
  CancelRequest,
  CancelResponse,
  CreateAgentRequest,
  CreateAgentResponse,
  encodeWire,
  ErrorBody,
  EventsRequest,
  EventsResponse,
  OperationResponse,
  type OperationStatus,
  ResolveRequest,
  ResolveResponse,
  type ResultBody,
  ResultResponse,
  SendRequest,
  SendResponse,
  StateResponse,
  TERMINAL_STATUSES,
} from "../wire/index.js";

export * from "../errors.js";
export type {
  AnyAgentDefinition,
  MessageNames,
  OperationState,
  PayloadOf,
  ResultOf,
} from "../types.js";
export type { OperationStatus } from "../wire/index.js";

/** The longest `wait_ms` the server honours on one result or call request. */
const MAX_WAIT_MS = 20_000;
const API = "/api/v1/agents";

export interface ClientOptions {
  /** Base URL of a Ravel server started with `--agents`. */
  readonly url: string;
  readonly token: string;
  /** Defaults to the global `fetch`. */
  readonly fetch?: typeof fetch;
  /** Retries after the first attempt for a transient failure. Default 3. */
  readonly retries?: number;
  /** Delay before the first retry, doubled per retry. Default 100 ms. */
  readonly retryDelayMs?: number;
  /** Default deadline for `send`, `call` and `result`; none when absent. */
  readonly deadlineMs?: number;
}

export interface RequestOptions {
  readonly signal?: AbortSignal;
  /** Past the deadline the call throws `OutcomeUnknown` carrying the handle. */
  readonly deadlineMs?: number;
}

export interface SendOptions extends RequestOptions {
  /** Idempotency key; the SDK generates one when absent. */
  readonly requestId?: string;
}

export interface OperationInfo {
  readonly operationId: string;
  readonly status: OperationStatus;
  readonly acceptSeq: number;
  /** Set while the operation is blocked on an effect awaiting reconciliation. */
  readonly blockedEffect: string | null;
}

export interface AgentEvent {
  readonly eventSeq: number;
  readonly acceptSeq: number;
  readonly turnId: string;
  readonly eventType: string;
  readonly payload: unknown;
  /** Pass as `after` to resume the stream after this event. */
  readonly cursor: string;
}

export interface EventsOptions {
  /** A cursor from an earlier event; absent reads from the start. */
  readonly after?: string;
  readonly signal?: AbortSignal;
  readonly pageSize?: number;
  /** How long to wait before polling again when a page is empty. Default 50 ms. */
  readonly pollIntervalMs?: number;
}

/** The operation finished without a result: it failed or was cancelled. */
export class OperationFailed extends Error {
  override readonly name = "OperationFailed";

  constructor(
    readonly operationId: string,
    readonly status: OperationStatus,
    readonly reason: string | null,
    readonly deadLettered: boolean,
  ) {
    super(`operation ${operationId} ${status}${reason === null ? "" : `: ${reason}`}`);
  }
}

export interface OperationHandle<R> {
  readonly requestId: string;
  readonly handler: string;
  /** Known once the server acknowledged the request or `resolve` found it. */
  readonly operationId: string | undefined;
  /**
   * Find the operation this request id created. When the server never
   * accepted it, the same request is sent again under the same request id.
   */
  resolve(options?: RequestOptions): Promise<string>;
  /**
   * Wait for the result. A wait that is aborted or reaches its deadline
   * changes nothing on the server.
   */
  result(options?: RequestOptions): Promise<R>;
  status(options?: RequestOptions): Promise<OperationInfo>;
  /** Idempotent cancellation. */
  cancel(options?: RequestOptions): Promise<OperationStatus>;
  /** The operation's events, ending once it is terminal and its events are read. */
  events(options?: EventsOptions): AsyncIterableIterator<AgentEvent>;
}

export interface AgentState<S> {
  readonly agentId: string;
  readonly incarnation: number;
  readonly stateVersion: number;
  readonly state: S;
}

export interface AgentClient<A extends AnyAgentDefinition> {
  readonly kind: A["name"];
  readonly key: string;
  /** Create the agent if absent; the agent id is the same either way. */
  create(options?: RequestOptions): Promise<{ agentId: string; created: boolean }>;
  send<K extends MessageNames<A>>(
    handler: K,
    payload: PayloadOf<A, K>,
    options?: SendOptions,
  ): Promise<OperationHandle<ResultOf<A, K>>>;
  /** Send, then wait for the result. */
  call<K extends MessageNames<A>>(
    handler: K,
    payload: PayloadOf<A, K>,
    options?: SendOptions,
  ): Promise<ResultOf<A, K>>;
  operation<K extends MessageNames<A>>(handler: K, operationId: string): OperationHandle<ResultOf<A, K>>;
  state(options?: RequestOptions): Promise<AgentState<z.output<A["state"]>>>;
}

export interface RavelAgentsClient {
  /**
   * Address one agent by kind and key. Pass the agent's definition type,
   * imported with `import type`, so payloads and results are checked at
   * compile time.
   */
  agent<A extends AnyAgentDefinition = never>(kind: A["name"], key: string): AgentClient<A>;
}

interface Deadline {
  readonly signal: AbortSignal;
  expired(): boolean;
  dispose(): void;
}

function deadline(user: AbortSignal | undefined, ms: number | undefined): Deadline {
  const controller = new AbortController();
  let expired = false;
  const onAbort = () => {
    controller.abort(user?.reason);
  };
  if (user?.aborted === true) controller.abort(user.reason);
  user?.addEventListener("abort", onAbort, { once: true });
  const timer =
    ms === undefined
      ? undefined
      : setTimeout(() => {
          expired = true;
          controller.abort(new Error("deadline elapsed"));
        }, ms);
  return {
    signal: controller.signal,
    expired: () => expired,
    dispose: () => {
      if (timer !== undefined) clearTimeout(timer);
      user?.removeEventListener("abort", onAbort);
    },
  };
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(signal.reason as Error);
      return;
    }
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal.reason as Error);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

/** An events cursor: the seq of the last event delivered, whatever page size carried it. */
function encodeCursor(eventSeq: number): string {
  return btoa(JSON.stringify({ s: eventSeq }))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");
}

function decodeCursor(cursor: string): number {
  try {
    const parsed: unknown = JSON.parse(atob(cursor.replaceAll("-", "+").replaceAll("_", "/")));
    if (typeof parsed === "object" && parsed !== null && "s" in parsed) {
      const { s } = parsed;
      if (typeof s === "number" && Number.isSafeInteger(s) && s >= 0) return s;
    }
  } catch {
    // reported below
  }
  throw new UnsupportedFormat(`not an events cursor: ${cursor}`);
}

export function createClient(options: ClientOptions): RavelAgentsClient {
  const fetchImpl = options.fetch ?? globalThis.fetch.bind(globalThis);
  const base = options.url.replace(/\/+$/, "");
  const retries = options.retries ?? 3;
  const retryDelayMs = options.retryDelayMs ?? 100;

  /**
   * One request, retried on a network failure, an untyped 5xx, or a typed
   * error whose retry hint is `backoff`. The body is serialized once, so
   * every attempt sends the same bytes under the same request id.
   */
  async function exchange<S extends z.ZodType>(
    method: "GET" | "POST",
    route: string,
    query: Record<string, string> | undefined,
    body: string | undefined,
    schema: S,
    signal: AbortSignal,
  ): Promise<z.output<S>> {
    const url = `${base}${API}/${route}${query === undefined ? "" : `?${new URLSearchParams(query).toString()}`}`;
    let last: unknown;
    for (let attempt = 0; attempt <= retries; attempt += 1) {
      if (attempt > 0) await sleep(retryDelayMs * 2 ** (attempt - 1), signal);
      let response: Response;
      try {
        const headers: Record<string, string> = { authorization: `Bearer ${options.token}` };
        if (body !== undefined) headers["content-type"] = "application/json";
        response = await fetchImpl(url, {
          method,
          headers,
          ...(body === undefined ? {} : { body }),
          signal,
        });
      } catch (error) {
        if (signal.aborted) throw signal.reason;
        last = new OutcomeUnknown(`${route}: request failed: ${String(error)}`, { cause: error });
        continue;
      }
      const text = await response.text();
      let json: unknown;
      try {
        json = text === "" ? undefined : JSON.parse(text);
      } catch {
        json = undefined;
      }
      if (response.ok) {
        const decoded = schema.safeDecode(json as z.input<S>);
        if (!decoded.success) {
          throw new UnsupportedFormat(`${route}: response does not match the wire schema`, {
            cause: decoded.error,
          });
        }
        return decoded.data;
      }
      const errorBody = ErrorBody.safeParse(json);
      if (errorBody.success) {
        const error = errorFromBody(errorBody.data);
        if (error.retry !== "backoff") throw error;
        last = error;
        continue;
      }
      if (response.status === 401 || response.status === 403) {
        const refused = AuthErrorBody.safeParse(json);
        const detail = refused.success ? `${refused.data.errorType}: ${refused.data.error}` : "credential refused";
        throw new Forbidden(response.status, `${route}: HTTP ${String(response.status)} ${detail}`);
      }
      if (response.status >= 500) {
        last = new OutcomeUnknown(`${route}: HTTP ${String(response.status)}`);
        continue;
      }
      throw new UnsupportedFormat(`${route}: HTTP ${String(response.status)} without an error body`);
    }
    throw last;
  }

  /** Run `work` under the caller's signal and deadline, mapping expiry to `OutcomeUnknown`. */
  async function bounded<T>(
    request: RequestOptions | undefined,
    handle: unknown,
    work: (signal: AbortSignal) => Promise<T>,
  ): Promise<T> {
    const d = deadline(request?.signal, request?.deadlineMs ?? options.deadlineMs);
    try {
      return await work(d.signal);
    } catch (error) {
      if (d.expired()) {
        throw new OutcomeUnknown("deadline elapsed before the outcome was known", { handle, cause: error });
      }
      if (request?.signal?.aborted === true) throw request.signal.reason;
      if (error instanceof OutcomeUnknown && error.handle === undefined && handle !== undefined) {
        throw new OutcomeUnknown(error.message, { handle, cause: error.cause, ids: error.ids });
      }
      throw error;
    } finally {
      d.dispose();
    }
  }

  function settle(operationId: string, result: ResultBody): unknown {
    if (result.status === "completed") return result.output;
    throw new OperationFailed(operationId, result.status, result.error, result.dead_lettered);
  }

  function agent<A extends AnyAgentDefinition>(kind: A["name"], key: string): AgentClient<A> {
    const ref = { kind, key };

    function handle<R>(handler: string, requestId: string, sendBody: string | undefined, known?: string): OperationHandle<R> {
      let operationId = known;

      async function ensure(signal: AbortSignal): Promise<string> {
        if (operationId !== undefined) return operationId;
        const found = await exchange(
          "POST",
          "resolve",
          undefined,
          JSON.stringify(encodeWire(ResolveRequest, { agent: ref, handler, request_id: requestId })),
          ResolveResponse,
          signal,
        );
        if (found.operation !== null) {
          operationId = found.operation;
          return operationId;
        }
        if (sendBody === undefined) {
          throw new UnsupportedFormat(`request ${requestId} was never accepted`, { ids: { requestId } });
        }
        const sent = await exchange("POST", "send", undefined, sendBody, SendResponse, signal);
        operationId = sent.operation;
        return operationId;
      }

      const self: OperationHandle<R> = {
        requestId,
        handler,
        get operationId() {
          return operationId;
        },
        resolve: (request) => bounded(request, self, ensure),
        result: (request) =>
          bounded(request, self, async (signal) => {
            const id = await ensure(signal);
            for (;;) {
              const response = await exchange(
                "GET",
                "result",
                { operation: id, wait_ms: String(MAX_WAIT_MS) },
                undefined,
                ResultResponse,
                signal,
              );
              if (response.result !== null) return settle(id, response.result) as R;
            }
          }),
        status: (request) =>
          bounded(request, self, async (signal) => {
            const id = await ensure(signal);
            const r = await exchange("GET", "operation", { operation: id }, undefined, OperationResponse, signal);
            return {
              operationId: r.operation,
              status: r.status,
              acceptSeq: r.accept_seq,
              blockedEffect: r.blocked_effect,
            };
          }),
        cancel: (request) =>
          bounded(request, self, async (signal) => {
            const id = await ensure(signal);
            const body = JSON.stringify(encodeWire(CancelRequest, { operation: id }));
            return (await exchange("POST", "cancel", undefined, body, CancelResponse, signal)).status;
          }),
        events: (eventsOptions = {}) => events(self, eventsOptions),
      };
      return self;
    }

    async function* events<R>(op: OperationHandle<R>, eventsOptions: EventsOptions): AsyncIterableIterator<AgentEvent> {
      const signal = eventsOptions.signal ?? new AbortController().signal;
      const pageSize = eventsOptions.pageSize ?? 100;
      const pollMs = eventsOptions.pollIntervalMs ?? 50;
      // The wire cursor is a committed event seq: the page holds the events after it.
      let delivered = eventsOptions.after === undefined ? 0 : decodeCursor(eventsOptions.after);
      let wire: string | null = delivered === 0 ? null : String(delivered);
      for (;;) {
        // Status before the page: a terminal status read here means every
        // event of the operation is already committed and in what follows.
        const info = await op.status({ signal });
        const terminal = TERMINAL_STATUSES.includes(info.status);
        const body = JSON.stringify(encodeWire(EventsRequest, { agent: ref, cursor: wire, limit: pageSize }));
        const page = await exchange("POST", "events", undefined, body, EventsResponse, signal);
        // Events are committed in accept order, so a later operation's event
        // means this one's are all behind it.
        let passed = false;
        for (const event of page.events) {
          if (event.accept_seq > info.acceptSeq) passed = true;
          if (event.event_seq <= delivered || event.accept_seq !== info.acceptSeq) continue;
          delivered = event.event_seq;
          yield {
            eventSeq: event.event_seq,
            acceptSeq: event.accept_seq,
            turnId: event.turn_id,
            eventType: event.event_type,
            payload: event.payload,
            cursor: encodeCursor(delivered),
          };
        }
        // A short page is not an end: the wire does not promise the server
        // fills a page to `limit`.
        if (terminal && (passed || page.events.length === 0)) return;
        if (page.events.length > 0) {
          wire = page.next_cursor;
          continue;
        }
        await sleep(pollMs, signal);
      }
    }

    async function send<R>(handler: string, payload: unknown, sendOptions: SendOptions = {}): Promise<OperationHandle<R>> {
      const requestId = sendOptions.requestId ?? crypto.randomUUID();
      const body = JSON.stringify(
        encodeWire(SendRequest, { agent: ref, handler, request_id: requestId, payload: payload as z.output<typeof SendRequest>["payload"] }),
      );
      const op = handle<R>(handler, requestId, body);
      return bounded(sendOptions, op, async (signal) => {
        const sent = await exchange("POST", "send", undefined, body, SendResponse, signal);
        return handle<R>(handler, requestId, body, sent.operation);
      });
    }

    return {
      kind,
      key,
      create: (request) =>
        bounded(request, undefined, async (signal) => {
          const body = JSON.stringify(encodeWire(CreateAgentRequest, { agent: ref }));
          const r = await exchange("POST", "create", undefined, body, CreateAgentResponse, signal);
          return { agentId: r.agent_id, created: r.created };
        }),
      send: (handler, payload, sendOptions) => send(handler, payload, sendOptions),
      call: async <K extends MessageNames<A>>(handler: K, payload: PayloadOf<A, K>, sendOptions: SendOptions = {}) => {
        const requestId = sendOptions.requestId ?? crypto.randomUUID();
        const sendBody = JSON.stringify(
          encodeWire(SendRequest, { agent: ref, handler, request_id: requestId, payload: payload as z.output<typeof SendRequest>["payload"] }),
        );
        const callBody = JSON.stringify(
          encodeWire(CallRequest, {
            agent: ref,
            handler,
            request_id: requestId,
            payload: payload as z.output<typeof CallRequest>["payload"],
            wait_ms: MAX_WAIT_MS,
          }),
        );
        const started = Date.now();
        const pending = handle<ResultOf<A, K>>(handler, requestId, sendBody);
        const called = await bounded(sendOptions, pending, (signal) =>
          exchange("POST", "call", undefined, callBody, CallResponse, signal),
        );
        if (called.result !== null) return settle(called.operation, called.result) as ResultOf<A, K>;
        const total = sendOptions.deadlineMs ?? options.deadlineMs;
        const rest = total === undefined ? {} : { deadlineMs: Math.max(0, total - (Date.now() - started)) };
        return handle<ResultOf<A, K>>(handler, requestId, sendBody, called.operation).result({
          ...(sendOptions.signal === undefined ? {} : { signal: sendOptions.signal }),
          ...rest,
        });
      },
      operation: (handler, operationId) => handle(handler, "", undefined, operationId),
      state: (request) =>
        bounded(request, undefined, async (signal) => {
          const r = await exchange("GET", "state", { kind, key }, undefined, StateResponse, signal);
          return {
            agentId: r.agent_id,
            incarnation: r.incarnation,
            stateVersion: r.state_version,
            state: r.state as z.output<A["state"]>,
          };
        }),
    };
  }

  return { agent };
}
