/**
 * Tools entry: declare the tools a worker runs as durable steps. Server-only;
 * the client entry never imports it.
 */
import type { z } from "zod";
import type { RecoveryPolicy, Schema, ToolContext, ToolDefinition } from "../types.js";

export type { RecoveryPolicy, ToolContext, ToolDefinition } from "../types.js";

export interface ToolOptions<I extends Schema, O extends Schema> {
  readonly name: string;
  readonly version: string;
  readonly input: I;
  readonly output: O;
  readonly recovery: RecoveryPolicy;
  readonly run: (input: z.output<I>, effect: ToolContext) => Promise<z.output<O>>;
}

const POLICIES: readonly RecoveryPolicy[] = ["provider_idempotency", "at_least_once", "manual"];

/**
 * Declare a tool. A handler runs it with `ctx.step(key, tool, input)`; `run`
 * receives the parsed input and the effect id, attempt and abort signal
 * (`steps.test.ts`, "a tool receives the effect id, attempt and signal").
 */
export function defineTool<I extends Schema, O extends Schema>(
  options: ToolOptions<I, O>,
): ToolDefinition<I, O> {
  if (!POLICIES.includes(options.recovery)) {
    throw new TypeError(`tool ${options.name}: unknown recovery policy ${String(options.recovery)}`);
  }
  return Object.freeze({ kind: "tool", ...options });
}
