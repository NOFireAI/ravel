/**
 * The durable step runner: runs one leased turn's handler against the step
 * transcript it was handed, and commits the outcome through a Transport.
 */
import type { z } from "zod";
import {
  IncompatibleHandler,
  NonDeterministicReplay,
  RavelAgentsError,
  type SafeIds,
} from "../errors.js";
import { canonicalBytes, derive, stepInputHash, toHex } from "../identity.js";
import type {
  AnyAgentDefinition,
  AnyMessageDefinition,
  HandlerContext,
  Schema,
  ToolDefinition,
} from "../types.js";
import type { EventSpecBody, OperationStatus, StepBody, TurnAssignment } from "../wire/index.js";
import type { PolledTurn, Transport } from "./transport.js";

/** A second `ctx.step` started while one is still in flight. */
export class ConcurrentStepError extends Error {
  override readonly name = "ConcurrentStepError";
  readonly code = "ConcurrentStep";

  constructor(
    readonly stepKey: string,
    readonly inFlightKey: string,
  ) {
    super(`step ${stepKey} started while step ${inFlightKey} is in flight; steps are sequential`);
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

function u64le(value: number): Uint8Array {
  const out = new Uint8Array(8);
  new DataView(out.buffer).setBigUint64(0, BigInt(value), true);
  return out;
}

/** A protocol outcome that ends the turn without a commit. */
function isBlocking(error: unknown): error is RavelAgentsError {
  return error instanceof RavelAgentsError && error.retry !== "backoff";
}

class Turn<S> {
  private readonly transcript: Map<string, StepBody>;
  private readonly events: EventSpecBody[] = [];
  private draft: S;
  private inFlight: string | undefined;
  private pending: Promise<unknown> = Promise.resolve();
  /** The first blocking outcome a step met; once set the turn never commits. */
  blocked: RavelAgentsError | undefined;
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
    this.random = seededRandom(
      derive("random", [a.lease.agent_id, u64le(a.lease.incarnation), u64le(a.accept_seq)]),
    );
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
    const startedMs = Number(this.turn.startedUnixNs / 1_000_000n);
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
    if (this.inFlight !== undefined) throw new ConcurrentStepError(key, this.inFlight);
    this.inFlight = key;
    const run = this.runStep(key, tool, input);
    this.pending = run.catch(() => undefined);
    try {
      return await run;
    } catch (error) {
      if (isBlocking(error)) this.blocked ??= error;
      throw error;
    } finally {
      this.inFlight = undefined;
    }
  }

  private async runStep<I extends Schema, O extends Schema>(
    key: string,
    tool: ToolDefinition<I, O>,
    input: z.input<I>,
  ): Promise<z.output<O>> {
    if (this.blocked !== undefined) throw this.blocked;
    const parsed = tool.input.parse(input);
    const inputHash = stepInputHash(tool.name, tool.version, canonicalBytes(parsed));
    const lease = this.assignment.lease;
    const request = {
      lease,
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
        // same check; the runner refuses locally whatever it answers.
        await this.transport.recordStep(request).catch(() => undefined);
        throw error;
      }
      if (recorded.status === "completed") return tool.output.parse(recorded.output);
    }
    const started = await this.transport.recordStep(request);
    const output = tool.output.parse(
      await tool.run(parsed, {
        effectId: started.effect,
        attempt: started.attempt,
        signal: this.signal,
      }),
    );
    await this.transport.completeEffect({
      effect: started.effect,
      attempt: started.attempt,
      result: output as StepBody["output"] & {},
    });
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
        reject(error as Error);
      },
    );
  });
}

/** A handler failure a retry cannot fix: the payload, result or state does not parse. */
class TerminalFailure extends Error {
  override readonly name = "TerminalFailure";
}

/** Run one leased turn to a commit, or to an abandonment that leaves it for the coordinator. */
export async function runTurn(
  polled: PolledTurn,
  agents: ReadonlyMap<string, AnyAgentDefinition>,
  transport: Transport,
  signal: AbortSignal,
): Promise<TurnReport> {
  const a = polled.assignment;
  const base = { operation: a.operation, turnId: a.lease.turn_id };
  const agent = agents.get(a.agent.kind);
  const message: AnyMessageDefinition | undefined = agent?.messages[a.handler];
  if (agent === undefined || message === undefined || a.lease.handler_version !== agent.version) {
    const error = new IncompatibleHandler(
      `no handler ${a.agent.kind}.${a.handler}@${a.lease.handler_version} on this worker`,
      { ids: { agentId: a.lease.agent_id, operationId: a.operation, turnId: a.lease.turn_id } },
    );
    return { outcome: "abandoned", ...base, error };
  }

  const stateParse = agent.state.safeParse(a.state);
  const turn = new Turn<unknown>(polled, stateParse.success ? stateParse.data : undefined, transport, signal);
  let completed: { output: unknown } | undefined;
  let failure: { error: string; terminal: boolean } | undefined;
  try {
    if (!stateParse.success) throw new TerminalFailure(`state does not parse: ${stateParse.error.message}`);
    const payload = message.payload.safeParse(a.payload);
    if (!payload.success) throw new TerminalFailure(`payload does not parse: ${payload.error.message}`);
    const output: unknown = await untilAborted(message.handler(turn.context(), payload.data), signal);
    const result = message.result.safeParse(output);
    if (!result.success) throw new TerminalFailure(`result does not parse: ${result.error.message}`);
    completed = { output: result.data };
  } catch (error) {
    failure = {
      error: error instanceof Error ? error.message : String(error),
      terminal: error instanceof TerminalFailure,
    };
  }
  if (signal.aborted) return { outcome: "abandoned", ...base, error: signal.reason };
  await turn.settled();
  if (turn.blocked !== undefined) return { outcome: "abandoned", ...base, error: turn.blocked };

  try {
    if (completed !== undefined) {
      const state = agent.state.safeParse(turn.state);
      if (state.success) {
        const response = await transport.commit({
          lease: a.lease,
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
      lease: a.lease,
      outcome: "failed",
      error: failure?.error ?? "handler failed",
      terminal: failure?.terminal ?? false,
    });
    return { outcome: "committed", ...base, commit: "failed", status: response.status };
  } catch (error) {
    return { outcome: "abandoned", ...base, error };
  }
}
