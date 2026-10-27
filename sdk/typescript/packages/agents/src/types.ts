/**
 * Types shared by every entry point. This module holds declarations only, so
 * the client entry can depend on agent and tool definitions as types without
 * importing the worker or tools runtime.
 */
import type { z } from "zod";

/** A runtime schema; payloads, results, state and tool inputs are validated with it. */
export type Schema = z.ZodType;

/** Recovery policy a tool declares for an ambiguous attempt (ADR-3331 decision 7). */
export type RecoveryPolicy = "provider_idempotency" | "at_least_once" | "manual";

/** Operation states (ADR-3331 decision 5). */
export type OperationState =
  | "accepted"
  | "running"
  | "blocked"
  | "completed"
  | "failed"
  | "cancelled";

export interface ToolContext {
  /** The effect id, stable across re-dispatches of one effect. */
  readonly effectId: string;
  readonly attempt: number;
  readonly signal: AbortSignal;
}

export interface ToolDefinition<I extends Schema = Schema, O extends Schema = Schema> {
  readonly kind: "tool";
  readonly name: string;
  readonly version: string;
  readonly input: I;
  readonly output: O;
  readonly recovery: RecoveryPolicy;
  readonly run: (input: z.output<I>, ctx: ToolContext) => Promise<z.output<O>>;
}

export interface HandlerContext<S> {
  readonly agentId: string;
  readonly turnId: string;
  readonly state: Readonly<S>;
  setState(next: S): void;
  /** A durable step: a completed step replays from its journal record. */
  step<T>(key: string, run: () => Promise<T>): Promise<T>;
  /** Run a tool as a durable step under the tool's recovery policy. */
  callTool<I extends Schema, O extends Schema>(
    key: string,
    tool: ToolDefinition<I, O>,
    input: z.input<I>,
  ): Promise<z.output<O>>;
  /** Recorded time, identical on replay. */
  now(): Date;
  /** Seeded random value in [0, 1), identical on replay. */
  random(): number;
}

export type Handler<S, P, R> = (ctx: HandlerContext<S>, payload: P) => Promise<R>;

export interface MessageDefinition<
  P extends Schema = Schema,
  R extends Schema = Schema,
  S = unknown,
> {
  readonly kind: "message";
  readonly payload: P;
  readonly result: R;
  readonly handler: Handler<S, z.output<P>, z.output<R>>;
}

/**
 * A message definition for an agent whose state is `S`, whatever its payload
 * and result schemas. The handler parameter is contravariant, so only `any`
 * admits every payload type.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AnyMessageDefinition<S = any> = MessageDefinition<any, any, S>;

/**
 * A tool definition whatever its input and output schemas. `run` takes the
 * input contravariantly, so a list of tools needs `any` here for the same
 * reason `AnyMessageDefinition` does.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AnyToolDefinition = ToolDefinition<any, any>;

export interface AgentDefinition<
  N extends string = string,
  St extends Schema = Schema,
  M extends Record<string, AnyMessageDefinition<z.output<St>>> = Record<
    string,
    AnyMessageDefinition<z.output<St>>
  >,
> {
  readonly kind: "agent";
  readonly name: N;
  readonly version: string;
  readonly state: St;
  readonly initialState: z.output<St>;
  readonly messages: M;
}

/** Any agent definition, whatever its state and message types. */
export type AnyAgentDefinition = AgentDefinition<string, Schema, Record<string, AnyMessageDefinition>>;

export type MessageNames<A extends AnyAgentDefinition> = keyof A["messages"] & string;

/** The payload a client sends: the schema's input type, before parsing. */
export type PayloadOf<A extends AnyAgentDefinition, K extends MessageNames<A>> = z.input<
  A["messages"][K]["payload"]
>;

export type ResultOf<A extends AnyAgentDefinition, K extends MessageNames<A>> = z.output<
  A["messages"][K]["result"]
>;
