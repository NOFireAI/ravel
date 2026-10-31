/**
 * A worker {@link Transport} over `fetch` against a Ravel server started with
 * `--agents`. Every method posts one `/api/v1/agents/worker/...` route:
 * `register`, `poll`, `heartbeat`, `step`, `effects/complete`,
 * `effects/fail` and `commit`. The server holds the step and effect journal,
 * so a worker that replaces a dead one in another process replays the same
 * steps.
 *
 * Every failure is a `RavelAgentsError`: an `ErrorBody` maps to the class its
 * `code` names (`agent_gone` to `AgentGone`, `effect_in_doubt` to
 * `EffectInDoubt`), a 401 or 403 in the server's plain body to `Forbidden`, a
 * 5xx without a typed body or a request that never got an answer to
 * `OutcomeUnknown`, and any other unexpected body to `UnsupportedFormat`. The
 * transport does not retry; the worker loop polls again.
 */
import type { z } from "zod";
import { Forbidden, OutcomeUnknown, UnsupportedFormat } from "../errors.js";
import { errorFromBody } from "../wire/errors.js";
import {
  AuthErrorBody,
  EffectCompleteRequest,
  EffectCompleteResponse,
  EffectFailRequest,
  encodeWire,
  ErrorBody,
  StepRecordRequest,
  StepRecordResponse,
  WorkerCommitRequest,
  WorkerCommitResponse,
  WorkerHeartbeatRequest,
  WorkerHeartbeatResponse,
  WorkerPollRequest,
  WorkerPollResponse,
  WorkerRegisterRequest,
  WorkerRegisterResponse,
} from "../wire/index.js";
import type { PolledTurn, Transport } from "../worker/transport.js";

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
   * Ignored: steps and effect results are recorded on the server.
   * Accepted so callers written against the in-memory journal still compile.
   */
  readonly journal?: unknown;
}

export class HttpTransport implements Transport {
  private readonly base: string;
  private readonly fetchImpl: typeof fetch;

  constructor(private readonly options: HttpTransportOptions) {
    this.base = options.url.replace(/\/+$/, "");
    this.fetchImpl = options.fetch ?? globalThis.fetch.bind(globalThis);
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
    return response.assignment === null ? null : { assignment: response.assignment };
  }

  recordStep(request: StepRecordRequest): Promise<StepRecordResponse> {
    return this.exchange("step", StepRecordRequest, request, StepRecordResponse);
  }

  completeEffect(request: EffectCompleteRequest): Promise<EffectCompleteResponse> {
    return this.exchange("effects/complete", EffectCompleteRequest, request, EffectCompleteResponse);
  }

  failEffect(request: EffectFailRequest): Promise<EffectCompleteResponse> {
    return this.exchange("effects/fail", EffectFailRequest, request, EffectCompleteResponse);
  }

  commit(request: WorkerCommitRequest): Promise<WorkerCommitResponse> {
    return this.exchange("commit", WorkerCommitRequest, request, WorkerCommitResponse);
  }
}

/** A worker transport against the server at `options.url`; pass it to `serveAgents`. */
export function createHttpTransport(options: HttpTransportOptions): HttpTransport {
  return new HttpTransport(options);
}
