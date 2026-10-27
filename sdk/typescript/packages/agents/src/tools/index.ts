/**
 * Tools entry: declare the tools a worker runs as durable effects. Server-only;
 * the client entry never imports it.
 */
import type { z } from "zod";
import { notImplemented } from "../not-implemented.js";
import type { RecoveryPolicy, Schema, ToolContext, ToolDefinition } from "../types.js";

export type { RecoveryPolicy, ToolContext, ToolDefinition } from "../types.js";

export interface ToolOptions<I extends Schema, O extends Schema> {
  readonly name: string;
  readonly version: string;
  readonly input: I;
  readonly output: O;
  readonly recovery: RecoveryPolicy;
  readonly run: (input: z.output<I>, ctx: ToolContext) => Promise<z.output<O>>;
}

export function defineTool<I extends Schema, O extends Schema>(
  _options: ToolOptions<I, O>,
): ToolDefinition<I, O> {
  throw notImplemented("defineTool");
}
