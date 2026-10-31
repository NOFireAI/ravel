/**
 * The durable step runner: runs one leased turn's handler against the step
 * transcript it was handed, and commits the outcome through a Transport.
 */
import type { z } from "zod";
import {
  AgentGone,
  ConcurrentStepError,
  EffectInDoubt,
  IncompatibleHandler,
  InvalidStepInput,
  NonDeterministicReplay,
  OwnershipLost,
  type SafeIds,
} from "../errors.js";
import { canonicalBytes, derive, jsonForm, stepInputHash, toHex } from "../identity.js";
import type {
  AnyAgentDefinition,
  AnyMessageDefinition,
  HandlerContext,
  Schema,
  ToolDefinition,
} from "../types.js";
import {
  type EventSpecBody,
  heldLease,
  type OperationStatus,
  type StepBody,
  type TurnAssignment,
} from "../wire/index.js";
import { pause } from "./backoff.js";
import type { PolledTurn, StepRecordRequest, StepRecordResponse, Transport } from "./transport.js";

/**
 * A step's tool failed, and the failure is the step's recorded result: a
 * retry of the turn replays it rather than running the tool again. `cause`
 * holds the tool's own error on the run that recorded it.
 */
export class StepFailed extends Error {
  override readonly name = "StepFailed";

  constructor(
    readonly stepKey: string,
    readonly error: string,
    options: { cause?: unknown } = {},
  ) {
    super(error, options);
  }
}

/** What happened to one turn. */
export type TurnReport =
  | {
      readonly outcome: "committed";
      readonly operation: string;
      readonly turnId: string;
      readonly commit: "completed" | "failed";
      readonly status: OperationStatus;
    }
  | {
      /** No commit was made: the turn is blocked, fenced out or its worker stopped. */
      readonly outcome: "abandoned";
      readonly operation: string;
      readonly turnId: string;
      readonly error: unknown;
    };

/** The digest a worker pins a handler under: the definition's identity, not its source. */
export function handlerDigest(agent: AnyAgentDefinition, handler: string): string {
  return toHex(derive("handler", [agent.name, agent.version, handler]));
}

/** sfc32 over a 128-bit seed. */
function seededRandom(seed: Uint8Array): () => number {
  const view = new DataView(seed.buffer, seed.byteOffset, 16);
  let a = view.getUint32(0, true);
  let b = view.getUint32(4, true);
  let c = view.getUint32(8, true);
  let d = view.getUint32(12, true);
  return () => {
    const t = (((a + b) >>> 0) + d) >>> 0;
    d = (d + 1) >>> 0;
    a = b ^ (b >>> 9);
    b = (c + (c << 3)) >>> 0;
    c = ((c << 21) | (c >>> 11)) >>> 0;
    c = (c + t) >>> 0;
    return t / 0x100000000;
  };
}

function fromHex(hex: string): Uint8Array {
  return Uint8Array.from({ length: hex.length / 2 }, (_, i) => parseInt(hex.slice(i * 2, i * 2 + 2), 16));
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

class Turn<S> {
  private readonly transcript: Map<string, StepBody>;
  private readonly events: EventSpecBody[] = [];
  private draft: S;
  private inFlight: string | undefined;
  private pending: Promise<unknown> = Promise.resolve();
  /**
   * The first outcome that ends the turn without a commit: a coordinator
   * refusal or a replay drift the runner detected. A tool's own error never
   * sets it.
   */
  blocked: { readonly error: unknown } | undefined;
  readonly random: () => number;

  constructor(
    private readonly turn: PolledTurn,
    state: S,
    private readonly transport: Transport,
    readonly signal: AbortSignal,
  ) {
    const a = turn.assignment;
    this.transcript = new Map(a.steps.map((s) => [s.step_key, s]));
    this.draft = state;
    this.random = seededRandom(fromHex(a.lease.random_seed));
  }

  private get assignment(): TurnAssignment {
    return this.turn.assignment;
  }

  ids(extra: SafeIds = {}): SafeIds {
    const a = this.assignment;
    return {
      agentId: a.lease.agent_id,
      incarnation: String(a.lease.incarnation),
      operationId: a.operation,
      turnId: a.lease.turn_id,
      epoch: a.lease.epoch,
      ...extra,
    };
  }

  context(): HandlerContext<S> {
    const a = this.assignment;
    const startedMs = Number(a.lease.recorded_unix_ns / 1_000_000n);
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const turn = this;
    return {
      agent: { kind: a.agent.kind, key: a.agent.key },
      agentId: a.lease.agent_id,
      operationId: a.operation,
      turnId: a.lease.turn_id,
      get state() {
        return turn.draft;
      },
      setState: (next) => {
        this.draft = next;
      },
      step: (key, tool, input) => this.step(key, tool, input),
      events: {
        emit: (eventType, payload) => {
          this.events.push({ event_type: eventType, payload: payload as EventSpecBody["payload"] });
        },
      },
      now: () => new Date(startedMs),
      random: this.random,
      signal: this.signal,
    };
  }

  async step<I extends Schema, O extends Schema>(
    key: string,
    tool: ToolDefinition<I, O>,
    input: z.input<I>,
  ): Promise<z.output<O>> {
    if (this.inFlight !== undefined) {
      throw new ConcurrentStepError(key, this.inFlight, { ids: this.ids() });
    }
    this.inFlight = key;
    const run = this.runStep(key, tool, input);
    this.pending = run.catch(() => undefined);
    try {
      return await run;
    } finally {
      this.inFlight = undefined;
    }
  }

  /** End the turn without a commit; every later step rejects with `error`. */
  private block(error: unknown): never {
    this.blocked ??= { error };
    throw error;
  }

  /** A coordinator call; its rejection is a refusal and blocks the turn. */
  private async coordinator<T>(call: () => Promise<T>): Promise<T> {
    try {
      return await call();
    } catch (error) {
      return this.block(error);
    }
  }

  /** The recorded result `record` carries, if it has one. */
  private replay<O extends Schema>(
    key: string,
    tool: ToolDefinition<Schema, O>,
    record: StepRecordResponse,
  ): { readonly output: z.output<O> } | undefined {
    if (record.step.status === "completed") return { output: tool.output.parse(record.step.output) };
    if (record.failure !== null) throw new StepFailed(key, record.failure);
    return undefined;
  }

  /** The journal's result for a step whose own result arrived late. */
  private async journaled<O extends Schema>(
    key: string,
    tool: ToolDefinition<Schema, O>,
    request: StepRecordRequest,
    effect: string,
    status: string,
  ): Promise<z.output<O>> {
    if (status === "completed") {
      const current = await this.coordinator(() => this.transport.recordStep(request));
      const replayed = this.replay(key, tool, current);
      if (replayed !== undefined) return replayed.output;
    }
    return this.block(
      new EffectInDoubt(`effect ${effect} of step ${key} has a late result and no journaled one`, {
        ids: this.ids({ stepKey: key, effectId: effect }),
      }),
    );
  }

  private async runStep<I extends Schema, O extends Schema>(
    key: string,
    tool: ToolDefinition<I, O>,
    input: z.input<I>,
  ): Promise<z.output<O>> {
    if (this.blocked !== undefined) throw this.blocked.error;
    const parsed = tool.input.parse(input);
    const inputHash = stepInputHash(tool.name, tool.version, canonicalBytes(jsonForm(parsed)));
    const request: StepRecordRequest = {
      lease: heldLease(this.assignment.lease),
      step_key: key,
      tool_name: tool.name,
      tool_version: tool.version,
      policy: tool.recovery,
      input_hash: inputHash,
    };
    const recorded = this.transcript.get(key);
    if (recorded !== undefined) {
      if (
        recorded.tool_name !== tool.name ||
        recorded.tool_version !== tool.version ||
        recorded.input_hash !== inputHash
      ) {
        const error = new NonDeterministicReplay(
          `step ${key} was journaled for ${recorded.tool_name}@${recorded.tool_version} ` +
            `with input hash ${recorded.input_hash}; this run asks for ` +
            `${tool.name}@${tool.version} with input hash ${inputHash}`,
          { ids: this.ids({ stepKey: key, stepId: recorded.step_id }) },
        );
        // The coordinator holds the journal and blocks the operation on the
        // same check; the runner refuses locally whatever it answers, unless
        // the agent is gone, which ends the turn for every reason at once.
        const refused = await this.transport.recordStep(request).then(
          () => undefined,
          (cause: unknown) => cause,
        );
        this.block(refused instanceof AgentGone ? refused : error);
      }
      if (recorded.status === "completed") return tool.output.parse(recorded.output);
    }
    // A `started` entry may have gained a result since the turn was leased,
    // so the coordinator's current record decides whether the tool runs.
    const started = await this.coordinator(() => this.transport.recordStep(request));
    const replayed = this.replay(key, tool, started);
    if (replayed !== undefined) return replayed.output;
    const effect = { effect: started.effect, attempt: started.attempt };
    let output: z.output<O>;
    try {
      output = tool.output.parse(
        await tool.run(parsed, { effectId: started.effect, attempt: started.attempt, signal: this.signal }),
      );
    } catch (error) {
      // An aborted worker's tool error says nothing about the effect.
      if (this.signal.aborted) throw error;
      const text = errorText(error);
      const failed = await this.coordinator(() => this.transport.failEffect({ ...effect, error: text }));
      if (failed.late) return this.journaled(key, tool, request, started.effect, failed.status);
      throw new StepFailed(key, text, { cause: error });
    }
    const done = await this.coordinator(() =>
      this.transport.completeEffect({ ...effect, result: output as StepBody["output"] & {} }),
    );
    if (done.late) return this.journaled(key, tool, request, started.effect, done.status);
    this.transcript.set(key, { ...started.step, status: "completed", output: output as StepBody["output"] });
    return output;
  }

  async settled(): Promise<void> {
    await this.pending;
  }

  get state(): S {
    return this.draft;
  }

  get emitted(): EventSpecBody[] {
    return this.events;
  }
}

/** Settles with `work`, or rejects once `signal` aborts so a stuck handler cannot hold the worker. */
function untilAborted<T>(work: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(signal.reason as Error);
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      reject(signal.reason as Error);
    };
    signal.addEventListener("abort", onAbort, { once: true });
    work.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error instanceof Error ? error : new Error(String(error)));
      },
    );
  });
}

/**
 * The shortest pause between two lease renewals, and the pause before the
 * first answer names an expiry: a lease at or past its expiry, or a worker
 * clock ahead of the coordinator's, renews at this pace rather than in a spin.
 */
export const MIN_RENEW_INTERVAL_MS = 1_000;

/**
 * The pause before the next renewal: one third of the lease left at `nowMs`
 * by the last answered expiry, so two more renewals fit before it runs out.
 */
export function renewInterval(expiresUnixNs: bigint | undefined, nowMs: number): number {
  if (expiresUnixNs === undefined) return MIN_RENEW_INTERVAL_MS;
  const leftMs = Number(expiresUnixNs / 1_000_000n) - nowMs;
  return Math.max(MIN_RENEW_INTERVAL_MS, Math.floor(leftMs / 3));
}

/**
 * The lease length a worker assumes the coordinator grants, the server's
 * default `lease_ms`. Neither a poll nor a heartbeat answer names it.
 */
export const DEFAULT_LEASE_MS = 30_000;

/**
 * The turn's lease ran out with no renewal answered: by the worker's own
 * monotonic clock, `leaseMs` passed since the last grant it saw (the poll that
 * handed it the turn, or the last heartbeat answer). Another worker may take
 * the turn over from here, so the turn is abandoned rather than committed.
 */
export class LeaseExpired extends Error {
  override readonly name = "LeaseExpired";

  constructor(
    readonly turnId: string,
    readonly leaseMs: number,
  ) {
    super(`lease on turn ${turnId} expired: no renewal answered within ${String(leaseMs)} ms`);
  }
}

/**
 * Renew the turn's lease until `stop` aborts: one heartbeat at a time, the
 * first at once, each next one {@link renewInterval} after the last answer.
 * A refusal (`AgentGone`, `OwnershipLost`) aborts `turn` with that error; any
 * other failure is retried at the next interval. Once `leaseMs` passes after
 * the assignment or the last answered renewal, measured on the worker's
 * timers and not against the answered wall-clock expiry, `turn` is aborted
 * with {@link LeaseExpired}. Never rejects.
 */
async function renewLease(
  assignment: TurnAssignment,
  transport: Transport,
  turn: AbortController,
  stop: AbortSignal,
  leaseMs: number,
): Promise<void> {
  const request = { lease: heldLease(assignment.lease) };
  const stopped = (): boolean => stop.aborted || turn.signal.aborted;
  let expiry: ReturnType<typeof setTimeout> | undefined;
  const granted = () => {
    clearTimeout(expiry);
    expiry = setTimeout(() => {
      if (!stop.aborted) turn.abort(new LeaseExpired(assignment.lease.turn_id, leaseMs));
    }, leaseMs);
  };
  const disarm = () => {
    clearTimeout(expiry);
  };
  stop.addEventListener("abort", disarm, { once: true });
  granted();
  let expires: bigint | undefined;
  try {
    while (!stopped()) {
      try {
        const answer = await transport.heartbeat(request);
        if (stopped()) return;
        granted();
        expires = answer.lease_expires_unix_ns;
      } catch (error) {
        if (stopped()) return;
        if (error instanceof AgentGone || error instanceof OwnershipLost) {
          turn.abort(error);
          return;
        }
      }
      await pause(renewInterval(expires, Date.now()), stop);
    }
  } finally {
    disarm();
    stop.removeEventListener("abort", disarm);
  }
}

/** A handler failure a retry cannot fix: the payload, result or state does not parse. */
class TerminalFailure extends Error {
  override readonly name = "TerminalFailure";
}

/**
 * Run one leased turn to a commit, or to an abandonment that leaves it for the
 * coordinator. A refusal from any coordinator call, `OwnershipLost` and
 * `AgentGone` among them, abandons the turn with that error: the commit is
 * attempted at most once and never retried here. From assignment until the
 * commit is sent the lease is renewed in the background; a refused renewal
 * aborts the handler's signal and abandons the turn with the refusal, and so
 * does a lease that runs out, `leaseMs` after the last grant the worker saw
 * answered, with {@link LeaseExpired}.
 */
export async function runTurn(
  polled: PolledTurn,
  agents: ReadonlyMap<string, AnyAgentDefinition>,
  transport: Transport,
  signal: AbortSignal,
  leaseMs: number = DEFAULT_LEASE_MS,
): Promise<TurnReport> {
  const a = polled.assignment;
  const base = { operation: a.operation, turnId: a.lease.turn_id };
  const agent = agents.get(a.agent.kind);
  const message: AnyMessageDefinition | undefined = agent?.messages[a.handler];
  if (
    agent === undefined ||
    message === undefined ||
    a.lease.handler_version !== agent.version ||
    a.lease.handler_digest !== handlerDigest(agent, a.handler)
  ) {
    const error = new IncompatibleHandler(
      `no handler ${a.agent.kind}.${a.handler}@${a.lease.handler_version} ` +
        `with digest ${a.lease.handler_digest} on this worker`,
      { ids: { agentId: a.lease.agent_id, operationId: a.operation, turnId: a.lease.turn_id } },
    );
    return { outcome: "abandoned", ...base, error };
  }

  // The coordinator holds no state object until an agent's first commit and
  // polls that turn with a null state; the turn starts from `initialState`.
  const fresh = a.state === null && a.lease.base_state_version === 0;
  const stateParse = agent.state.safeParse(fresh ? agent.initialState : a.state);
  const aborter = new AbortController();
  const forward = () => {
    aborter.abort(signal.reason);
  };
  if (signal.aborted) forward();
  else signal.addEventListener("abort", forward, { once: true });
  const renewal = new AbortController();
  void renewLease(a, transport, aborter, renewal.signal, leaseMs);
  try {
    return await settleTurn(polled, agent, message, stateParse, transport, aborter.signal, renewal);
  } finally {
    renewal.abort();
    signal.removeEventListener("abort", forward);
  }
}

/** The handler run and commit of {@link runTurn}, under the turn's own `signal`. */
async function settleTurn(
  polled: PolledTurn,
  agent: AnyAgentDefinition,
  message: AnyMessageDefinition,
  stateParse: ReturnType<z.ZodType["safeParse"]>,
  transport: Transport,
  signal: AbortSignal,
  renewal: AbortController,
): Promise<TurnReport> {
  const a = polled.assignment;
  const base = { operation: a.operation, turnId: a.lease.turn_id };
  const aborted = (): boolean => signal.aborted;
  const turn = new Turn<unknown>(polled, stateParse.success ? stateParse.data : undefined, transport, signal);
  let completed: { output: unknown } | undefined;
  let failure: { error: string; terminal: boolean } | undefined;
  try {
    if (!stateParse.success) throw new TerminalFailure(`state does not parse: ${stateParse.error.message}`);
    const payloadSchema = message.payload as z.ZodType;
    const payload = payloadSchema.safeParse(a.payload);
    if (!payload.success) throw new TerminalFailure(`payload does not parse: ${payload.error.message}`);
    const output: unknown = await untilAborted(message.handler(turn.context(), payload.data), signal);
    const resultSchema = message.result as z.ZodType;
    const result = resultSchema.safeParse(output);
    if (!result.success) throw new TerminalFailure(`result does not parse: ${result.error.message}`);
    completed = { output: result.data };
  } catch (error) {
    failure = {
      error: errorText(error),
      terminal:
        error instanceof TerminalFailure ||
        error instanceof ConcurrentStepError ||
        error instanceof InvalidStepInput,
    };
  }
  if (aborted()) return { outcome: "abandoned", ...base, error: signal.reason };
  await turn.settled();
  if (turn.blocked !== undefined) return { outcome: "abandoned", ...base, error: turn.blocked.error };
  if (aborted()) return { outcome: "abandoned", ...base, error: signal.reason };
  renewal.abort();

  try {
    if (completed !== undefined) {
      const state = agent.state.safeParse(turn.state);
      if (state.success) {
        const response = await transport.commit({
          lease: heldLease(a.lease),
          outcome: "completed",
          state: state.data as StepBody["output"] & {},
          output: completed.output as StepBody["output"] & {},
          events: turn.emitted,
          timers: [],
          intents: [],
        });
        return { outcome: "committed", ...base, commit: "completed", status: response.status };
      }
      failure = { error: `state does not parse: ${state.error.message}`, terminal: true };
    }
    const response = await transport.commit({
      lease: heldLease(a.lease),
      outcome: "failed",
      error: failure?.error ?? "handler failed",
      terminal: failure?.terminal ?? false,
    });
    return { outcome: "committed", ...base, commit: "failed", status: response.status };
  } catch (error) {
    return { outcome: "abandoned", ...base, error };
  }
}
