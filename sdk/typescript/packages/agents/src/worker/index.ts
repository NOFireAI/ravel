/**
 * Worker entry: define agents and their message handlers, and serve them
 * against a coordinator through a {@link Transport}. Server-only; the client
 * entry never imports it.
 */
import type { z } from "zod";
import { Forbidden, RavelAgentsError } from "../errors.js";
import type {
  AgentDefinition,
  AnyAgentDefinition,
  AnyMessageDefinition,
  AnyToolDefinition,
  Handler,
  MessageDefinition,
  Schema,
} from "../types.js";
import { Backoff, OperationBackoffs } from "./backoff.js";
import { handlerDigest, runTurn, type TurnReport } from "./runner.js";
import type { Transport } from "./transport.js";

export type {
  AgentDefinition,
  AnyAgentDefinition,
  Handler,
  HandlerContext,
  MessageDefinition,
} from "../types.js";
export { ConcurrentStepError, InvalidStepInput } from "../errors.js";
export { handlerDigest, runTurn, StepFailed, type TurnReport } from "./runner.js";
export type {
  EffectFailRequest,
  PolledTurn,
  StepRecordRequest,
  StepRecordResponse,
  Transport,
} from "./transport.js";
export {
  createHttpTransport,
  HttpTransport,
  type HttpTransportOptions,
  type JournalLateResult,
  type JournalStepRequest,
  MAX_POLL_WAIT_MS,
  MemoryStepJournal,
  type StepJournal,
  type StepScope,
} from "../transport/http.js";

export interface MessageOptions<P extends Schema, R extends Schema, S> {
  readonly payload: P;
  readonly result: R;
  readonly handler: Handler<S, z.output<P>, z.output<R>>;
}

/** Declares one message handler whose context carries state of type `S`. */
export type MessageFactory<S> = <P extends Schema, R extends Schema>(
  options: MessageOptions<P, R, S>,
) => MessageDefinition<P, R, S>;

/**
 * Declare one message handler that does not read the agent's state. A
 * handler that does uses the `message` parameter of {@link AgentOptions}'s
 * `messages` builder, which carries the agent's state type.
 */
export function message<P extends Schema, R extends Schema, S = unknown>(
  options: MessageOptions<P, R, S>,
): MessageDefinition<P, R, S> {
  if (typeof options.handler !== "function") {
    throw new TypeError("message: handler must be a function");
  }
  return Object.freeze({
    kind: "message",
    payload: options.payload,
    result: options.result,
    handler: options.handler,
  });
}

export interface AgentOptions<
  N extends string,
  St extends Schema,
  M extends Record<string, AnyMessageDefinition<z.output<St>>>,
> {
  readonly name: N;
  readonly version: string;
  readonly state: St;
  readonly initialState: z.output<St>;
  /**
   * The agent's handlers, or a builder receiving a `message` factory typed
   * with this agent's state. A `message(...)` call written directly in the
   * record cannot see the state type: TypeScript does not infer a nested
   * call's type parameter from the outer call's inference in progress.
   */
  readonly messages: M | ((message: MessageFactory<z.output<St>>) => M);
}

export function defineAgent<
  N extends string,
  St extends Schema,
  M extends Record<string, AnyMessageDefinition<z.output<St>>>,
>(options: AgentOptions<N, St, M>): AgentDefinition<N, St, M> {
  const messages =
    typeof options.messages === "function"
      ? options.messages(message)
      : options.messages;
  return Object.freeze({
    kind: "agent",
    name: options.name,
    version: options.version,
    state: options.state,
    initialState: options.state.parse(options.initialState),
    messages: Object.freeze({ ...messages }),
  });
}

export interface ServeOptions {
  readonly transport: Transport;
  readonly agents: readonly AnyAgentDefinition[];
  /** The tools handlers may run; listed for registration, not required by the runner. */
  readonly tools?: readonly AnyToolDefinition[];
  /** Defaults to the id `transport.register` mints for this worker's handlers. */
  readonly workerId?: string;
  /** Long-poll wait per poll; the wire caps it at 20 s. */
  readonly pollWaitMs?: number;
  /**
   * Pause after an abandoned turn before polling again. A turn still leased
   * to this worker is handed straight back to it, so an abandonment whose
   * error hints `reconcile` (`EffectInDoubt`), `backoff` (`StoreUnavailable`,
   * `MailboxFull`) or `resolve` (`OutcomeUnknown`, a lost answer) doubles the
   * pause for its operation, up to `maxBackoffMs`, and that operation's next
   * commit resets it. Each operation keeps its own delay, so another
   * operation's abandonment starts at this one; the pause still holds the
   * whole poll loop. An abandonment hinted `never`, or with no hint, pauses
   * this fixed delay. Default 250 ms.
   */
  readonly abandonDelayMs?: number;
  /** Cap on the doubling pauses after abandoned turns and failed polls. Default 30 s. */
  readonly maxBackoffMs?: number;
  /** Observes every turn the worker finishes or abandons. */
  readonly onTurn?: (report: TurnReport) => void;
  /**
   * Called with every error a poll rejects with. Only `Forbidden`, a refused
   * token, stops the worker: a retry with the same token cannot succeed.
   * Every other error is retried after a pause that doubles from 50 ms up to
   * `maxBackoffMs` and resets on the next poll that answers, including
   * `UnsupportedFormat` and other errors hinted `never`, since a proxy can
   * answer one poll with a page the SDK does not read and the next with a
   * turn. Without `onError`, a stopping error rejects
   * {@link WorkerHandle.done}; an `onError` that throws stops the worker with
   * what it threw.
   */
  readonly onError?: (error: unknown) => void;
}

export interface WorkerHandle {
  readonly ownerId: string;
  /**
   * Settles when the worker stops: fulfils after `close()` or after a stopping
   * error that `onError` handled, and rejects with a stopping error when no
   * `onError` is set. Observe it: an unobserved rejection is an unhandled one.
   */
  readonly done: Promise<void>;
  /** Stop taking turns; an in-flight turn is aborted and not committed. Resolves once stopped. */
  close(): Promise<void>;
}

/** The first pause after a failed poll. */
const POLL_RETRY_MS = 50;

/** Resolves after `ms`, or as soon as `signal` aborts. */
function pause(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const done = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal.addEventListener("abort", done, { once: true });
  });
}

/**
 * Poll `transport` for turns of `agents` and run each to a commit until
 * closed. Without `workerId` it registers first and polls under the id the
 * coordinator minted.
 */
export async function serveAgents(options: ServeOptions): Promise<WorkerHandle> {
  const agents = new Map(options.agents.map((a) => [a.name, a]));
  const handlers = options.agents.flatMap((agent) =>
    Object.keys(agent.messages).map((handler) => ({
      handler,
      version: agent.version,
      digest: handlerDigest(agent, handler),
    })),
  );
  const workerId = options.workerId ?? (await options.transport.register({ handlers })).worker_id;
  const controller = new AbortController();
  const waitMs = Math.min(options.pollWaitMs ?? 20_000, 20_000);
  const abandonDelayMs = options.abandonDelayMs ?? 250;
  const maxBackoffMs = options.maxBackoffMs ?? 30_000;
  const abandonDelays = new OperationBackoffs(abandonDelayMs, maxBackoffMs);
  const pollRetryDelay = new Backoff(POLL_RETRY_MS, maxBackoffMs);
  const abandonPause = (report: Extract<TurnReport, { outcome: "abandoned" }>): number =>
    report.error instanceof RavelAgentsError && report.error.retry !== "never"
      ? abandonDelays.next(report.operation)
      : abandonDelayMs;

  const stopped = (): boolean => controller.signal.aborted;
  const serve = async (): Promise<void> => {
    while (!stopped()) {
      let polled;
      const askedAt = Date.now();
      try {
        polled = await options.transport.poll(
          { worker_id: workerId, handlers, wait_ms: waitMs },
          controller.signal,
        );
      } catch (error) {
        if (stopped()) return;
        if (error instanceof Forbidden) {
          if (options.onError === undefined) throw error;
          options.onError(error);
          return;
        }
        options.onError?.(error);
        await pause(pollRetryDelay.next(), controller.signal);
        continue;
      }
      pollRetryDelay.reset();
      if (polled === null) {
        // An empty answer that came back before the wait elapsed is a
        // transport that is not long-polling (or a closed fake): back off
        // rather than spin.
        if (Date.now() - askedAt < Math.min(waitMs, 50) && !stopped()) {
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
        continue;
      }
      const report = await runTurn(polled, agents, options.transport, controller.signal);
      options.onTurn?.(report);
      if (report.outcome === "committed") {
        abandonDelays.forget(report.operation);
      } else {
        await pause(abandonPause(report), controller.signal);
      }
    }
  };
  const loop = (async () => {
    try {
      await serve();
    } finally {
      abandonDelays.clear();
    }
  })();

  return {
    ownerId: workerId,
    done: loop,
    close: async () => {
      controller.abort(new Error("worker closed"));
      await loop.catch(() => undefined);
    },
  };
}
