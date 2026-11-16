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

/** What a tool's `run` receives besides its input (ADR-3331 decision 7). */
export interface ToolContext {
  /** The effect id: stable across attempts at one input, the key a provider deduplicates on. */
  readonly effectId: string;
  /** The attempt this run is, starting at 1. */
  readonly attempt: number;
  /** Aborted when the worker stops or loses the turn. */
  readonly signal: AbortSignal;
}

export interface ToolDefinition<I extends Schema = Schema, O extends Schema = Schema> {
  readonly kind: "tool";
  readonly name: string;
  readonly version: string;
  readonly input: I;
  readonly output: O;
  readonly recovery: RecoveryPolicy;
  readonly run: (input: z.output<I>, effect: ToolContext) => Promise<z.output<O>>;
}

export interface HandlerContext<S> {
  readonly agent: { readonly kind: string; readonly key: string };
  readonly agentId: string;
  readonly operationId: string;
  readonly turnId: string;
  /**
   * A draft of the agent's state. Mutate it or replace it with `setState`;
   * the runner publishes it only in the turn's commit.
   */
  readonly state: S;
  setState(next: S): void;
  /**
   * Run `tool` as the durable step `key`. A step completed by an earlier
   * attempt at this input returns its recorded output without running the
   * tool; a key reused with a different tool or input fails
   * `NonDeterministicReplay`. Steps are sequential: a second call while one
   * is in flight is refused with `ConcurrentStepError`.
   */
  step<I extends Schema, O extends Schema>(
    key: string,
    tool: ToolDefinition<I, O>,
    input: z.input<I>,
  ): Promise<z.output<O>>;
  /** Events buffered here are published with the turn's commit. */
  readonly events: { emit(eventType: string, payload: unknown): void };
  /** The turn's recorded start time; the same value on every attempt at this input. */
  now(): Date;
  /** A value in [0, 1) from a sequence seeded by the input's identity. */
  random(): number;
  /** Aborted when the worker stops or loses the turn. */
  readonly signal: AbortSignal;
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
