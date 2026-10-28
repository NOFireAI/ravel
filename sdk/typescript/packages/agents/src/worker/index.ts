/**
 * Worker entry: define agents and their message handlers, and serve them
 * against a coordinator through a {@link Transport}. Server-only; the client
 * entry never imports it.
 */
import type { z } from "zod";
import type {
  AgentDefinition,
  AnyAgentDefinition,
  AnyMessageDefinition,
  AnyToolDefinition,
  Handler,
  MessageDefinition,
  Schema,
} from "../types.js";
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
  /** Defaults to a random id. */
  readonly workerId?: string;
  /** Long-poll wait per poll; the wire caps it at 20 s. */
  readonly pollWaitMs?: number;
  /** Observes every turn the worker finishes or abandons. */
  readonly onTurn?: (report: TurnReport) => void;
}

export interface WorkerHandle {
  readonly ownerId: string;
  /** Stop taking turns; an in-flight turn is aborted and not committed. */
  close(): Promise<void>;
}

function randomId(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

/** Poll `transport` for turns of `agents` and run each to a commit until closed. */
export function serveAgents(options: ServeOptions): Promise<WorkerHandle> {
  const workerId = options.workerId ?? `worker-${randomId()}`;
  const agents = new Map(options.agents.map((a) => [a.name, a]));
  const handlers = options.agents.flatMap((agent) =>
    Object.keys(agent.messages).map((handler) => ({
      handler,
      version: agent.version,
      digest: handlerDigest(agent, handler),
    })),
  );
  const controller = new AbortController();
  const waitMs = Math.min(options.pollWaitMs ?? 20_000, 20_000);

  const stopped = (): boolean => controller.signal.aborted;
  const loop = (async () => {
    while (!stopped()) {
      let polled;
      try {
        polled = await options.transport.poll(
          { worker_id: workerId, handlers, wait_ms: waitMs },
          controller.signal,
        );
      } catch {
        if (stopped()) return;
        await new Promise((resolve) => setTimeout(resolve, 50));
        continue;
      }
      if (polled === null) continue;
      const report = await runTurn(polled, agents, options.transport, controller.signal);
      options.onTurn?.(report);
    }
  })();

  return Promise.resolve({
    ownerId: workerId,
    close: async () => {
      controller.abort(new Error("worker closed"));
      await loop;
    },
  });
}
