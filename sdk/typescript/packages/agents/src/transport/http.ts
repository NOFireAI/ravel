/**
 * A worker {@link Transport} over `fetch` against a Ravel server started with
 * `--agents`. `register`, `poll`, `heartbeat` and `commit` call the
 * `/api/v1/agents/worker/...` routes. The server mounts no step, effect or
 * reconcile route yet (ADR-3331 T16), so `recordStep`, `completeEffect` and
 * `failEffect` go to a {@link StepJournal}: `recordStep` first heartbeats the
 * lease, which the server fences, so a worker that lost its turn is refused
 * before the journal is touched.
 *
 * Every failure is a `RavelAgentsError`: an `ErrorBody` maps to the class its
 * `code` names, a 401 or 403 in the server's plain body to `Forbidden`, a 5xx
 * without a typed body or a request that never got an answer to
 * `OutcomeUnknown`, and any other unexpected body to `UnsupportedFormat`. The
 * transport does not retry; the worker loop polls again.
 */
import type { z } from "zod";
import { EffectInDoubt, Forbidden, OutcomeUnknown, UnsupportedFormat } from "../errors.js";
import { errorFromBody } from "../wire/errors.js";
import {
  AuthErrorBody,
  encodeWire,
  ErrorBody,
  type EffectCompleteRequest,
  type EffectCompleteResponse,
  WorkerCommitRequest,
  WorkerCommitResponse,
  WorkerHeartbeatRequest,
  WorkerHeartbeatResponse,
  WorkerPollRequest,
  WorkerPollResponse,
  WorkerRegisterRequest,
  WorkerRegisterResponse,
} from "../wire/index.js";
import type {
  EffectFailRequest,
  PolledTurn,
  StepRecordRequest,
  StepRecordResponse,
  Transport,
} from "../worker/transport.js";
import { MemoryStepJournal, type StepJournal, type StepScope } from "./journal.js";

export {
  MemoryStepJournal,
  type JournalLateResult,
  type JournalStepRequest,
  type StepJournal,
  type StepScope,
} from "./journal.js";

/** The longest `wait_ms` the server honours on one poll. */
export const MAX_POLL_WAIT_MS = 20_000;
const API = "/api/v1/agents/worker";

export interface HttpTransportOptions {
  /** Base URL of a Ravel server started with `--agents`. */
  readonly url: string;
  /** A credential carrying the `agents_worker` capability. */
  readonly token: string;
  /** Defaults to the global `fetch`. */
  readonly fetch?: typeof fetch;
  /**
   * Where steps and effect results are journaled until the server has routes
   * for them. Workers that should see each other's steps, such as a worker
   * and its replacement, share one. Defaults to a fresh {@link MemoryStepJournal}.
   */
  readonly journal?: StepJournal;
}

export class HttpTransport implements Transport {
  readonly journal: StepJournal;
  private readonly base: string;
  private readonly fetchImpl: typeof fetch;
  /** The scope of every turn this transport polled and the runner has not released. */
  private readonly scopes = new Map<string, StepScope>();

  constructor(private readonly options: HttpTransportOptions) {
    this.base = options.url.replace(/\/+$/, "");
    this.fetchImpl = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.journal = options.journal ?? new MemoryStepJournal();
  }

  private async exchange<Req extends z.ZodType, Res extends z.ZodType>(
    route: string,
    requestSchema: Req,
    request: z.output<Req>,
    responseSchema: Res,
    signal?: AbortSignal,
  ): Promise<z.output<Res>> {
    let response: Response;
    try {
      response = await this.fetchImpl(`${this.base}${API}/${route}`, {
        method: "POST",
        headers: { authorization: `Bearer ${this.options.token}`, "content-type": "application/json" },
        body: JSON.stringify(encodeWire(requestSchema, request)),
        ...(signal === undefined ? {} : { signal }),
      });
    } catch (error) {
      if (signal?.aborted === true) throw signal.reason;
      throw new OutcomeUnknown(`worker/${route}: request failed: ${String(error)}`, { cause: error });
    }
    let text: string;
    try {
      text = await response.text();
    } catch (error) {
      if (signal?.aborted === true) throw signal.reason;
      throw new OutcomeUnknown(`worker/${route}: response body failed: ${String(error)}`, { cause: error });
    }
    let json: unknown;
    try {
      json = text === "" ? undefined : JSON.parse(text);
    } catch {
      json = undefined;
    }
    if (response.ok) {
      const decoded = responseSchema.safeDecode(json as z.input<Res>);
      if (!decoded.success) {
        throw new UnsupportedFormat(`worker/${route}: response does not match the wire schema`, {
          cause: decoded.error,
        });
      }
      return decoded.data;
    }
    const errorBody = ErrorBody.safeParse(json);
    if (errorBody.success) throw errorFromBody(errorBody.data);
    if (response.status === 401 || response.status === 403) {
      const refused = AuthErrorBody.safeParse(json);
      const detail = refused.success ? `${refused.data.errorType}: ${refused.data.error}` : "credential refused";
      throw new Forbidden(response.status, `worker/${route}: HTTP ${String(response.status)} ${detail}`);
    }
    if (response.status >= 500) {
      throw new OutcomeUnknown(`worker/${route}: HTTP ${String(response.status)}`);
    }
    throw new UnsupportedFormat(`worker/${route}: HTTP ${String(response.status)} without an error body`);
  }

  register(request: WorkerRegisterRequest): Promise<WorkerRegisterResponse> {
    return this.exchange("register", WorkerRegisterRequest, request, WorkerRegisterResponse);
  }

  heartbeat(request: WorkerHeartbeatRequest): Promise<WorkerHeartbeatResponse> {
    return this.exchange("heartbeat", WorkerHeartbeatRequest, request, WorkerHeartbeatResponse);
  }

  async poll(request: WorkerPollRequest, signal: AbortSignal): Promise<PolledTurn | null> {
    const clamped = { ...request, wait_ms: Math.min(request.wait_ms, MAX_POLL_WAIT_MS) };
    const response = await this.exchange("poll", WorkerPollRequest, clamped, WorkerPollResponse, signal);
    const assignment = response.assignment;
    if (assignment === null) return null;
    this.scopes.set(assignment.lease.turn_id, {
      agentId: assignment.lease.agent_id,
      incarnation: assignment.lease.incarnation,
      operation: assignment.operation,
      acceptSeq: assignment.accept_seq,
      turnId: assignment.lease.turn_id,
    });
    return { assignment };
  }

  async recordStep(request: StepRecordRequest): Promise<StepRecordResponse> {
    const scope = this.scopes.get(request.lease.turn_id);
    if (scope === undefined) {
      throw new UnsupportedFormat(`turn ${request.lease.turn_id} was not polled through this transport`, {
        ids: { agentId: request.lease.agent_id, turnId: request.lease.turn_id },
      });
    }
    await this.heartbeat({ lease: request.lease });
    return this.journal.recordStep({
      scope,
      stepKey: request.step_key,
      toolName: request.tool_name,
      toolVersion: request.tool_version,
      policy: request.policy,
      inputHash: request.input_hash,
    });
  }

  completeEffect(request: EffectCompleteRequest): Promise<EffectCompleteResponse> {
    return this.journal.settle({
      effect: request.effect,
      attempt: request.attempt,
      result: { output: request.result },
    });
  }

  failEffect(request: EffectFailRequest): Promise<EffectCompleteResponse> {
    return this.journal.settle({
      effect: request.effect,
      attempt: request.attempt,
      result: { error: request.error },
    });
  }

  async commit(request: WorkerCommitRequest): Promise<WorkerCommitResponse> {
    const scope = this.scopes.get(request.lease.turn_id);
    if (request.outcome === "failed" && scope !== undefined) {
      // The kernel refuses FailTurn while an authorized attempt has no result.
      const pending = this.journal.pendingEffect(scope.operation);
      if (pending !== undefined) {
        throw new EffectInDoubt(
          `NoAuthorizedAttemptPending: an authorized attempt has no result (effect ${pending})`,
          {
            ids: {
              agentId: scope.agentId,
              operationId: scope.operation,
              turnId: scope.turnId,
              effectId: pending,
            },
          },
        );
      }
    }
    const response = await this.exchange("commit", WorkerCommitRequest, request, WorkerCommitResponse);
    this.scopes.delete(request.lease.turn_id);
    return response;
  }

  release(turnId: string): void {
    this.scopes.delete(turnId);
  }
}

/** A worker transport against the server at `options.url`; pass it to `serveAgents`. */
export function createHttpTransport(options: HttpTransportOptions): HttpTransport {
  return new HttpTransport(options);
}
