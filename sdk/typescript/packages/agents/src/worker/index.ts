/**
 * Worker entry: define agents and their message handlers, and serve them
 * against a Ravel server. Server-only; the client entry never imports it.
 */
import type { z } from "zod";
import { notImplemented } from "../not-implemented.js";
import type {
  AgentDefinition,
  AnyAgentDefinition,
  AnyMessageDefinition,
  AnyToolDefinition,
  Handler,
  MessageDefinition,
  Schema,
} from "../types.js";

export type {
  AgentDefinition,
  AnyAgentDefinition,
  Handler,
  HandlerContext,
  MessageDefinition,
} from "../types.js";

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
  _options: MessageOptions<P, R, S>,
): MessageDefinition<P, R, S> {
  throw notImplemented("message");
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
>(_options: AgentOptions<N, St, M>): AgentDefinition<N, St, M> {
  throw notImplemented("defineAgent");
}

export interface ServeOptions {
  /** Base URL of a Ravel server started with `--agents`. */
  readonly url: string;
  readonly token: string;
  readonly agents: readonly AnyAgentDefinition[];
  readonly tools?: readonly AnyToolDefinition[];
}

export interface WorkerHandle {
  readonly ownerId: string;
  /** Stop taking turns and release ownership; in-flight turns are not committed. */
  close(): Promise<void>;
}

export function serveAgents(_options: ServeOptions): Promise<WorkerHandle> {
  throw notImplemented("serveAgents");
}
