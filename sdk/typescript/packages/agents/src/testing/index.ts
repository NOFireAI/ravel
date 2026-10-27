/**
 * Testing entry: run agents and tools in process against an in-memory store,
 * with control over time, ownership and crashes. Server-only.
 */
import type { AgentClient } from "../client/index.js";
import { notImplemented } from "../not-implemented.js";
import type { AnyAgentDefinition, AnyToolDefinition } from "../types.js";

export interface TestHarnessOptions {
  readonly agents: readonly AnyAgentDefinition[];
  readonly tools?: readonly AnyToolDefinition[];
}

export interface TestHarness {
  agent<A extends AnyAgentDefinition>(definition: A, agentId: string): AgentClient<A>;
  /** Advance recorded time and fire due timers. */
  advanceTime(ms: number): Promise<void>;
  /** Kill the current owner mid-turn; the next owner resumes the same turn. */
  crashWorker(): Promise<void>;
  close(): Promise<void>;
}

export function createTestHarness(_options: TestHarnessOptions): TestHarness {
  throw notImplemented("createTestHarness");
}
