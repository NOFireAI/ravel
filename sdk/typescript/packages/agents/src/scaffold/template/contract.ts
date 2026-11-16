// Client types for the "order" agent at version "1",
// generated from its handler schemas by generateContract (@ravel/agents/contract).
// Regenerate this file after changing a payload or result schema; do not edit it.
import type { OperationHandle } from "@ravel/agents/client";

export const AGENT_KIND = "order";
export const AGENT_VERSION = "1";
export const HANDLER_NAMES = ["checkout", "summary"] as const;

export type HandlerName = (typeof HANDLER_NAMES)[number];

export interface Handlers {
  readonly checkout: {
    readonly input: {
      sku: string;
      qty: number;
    };
    readonly output: {
      sku: string;
      qty: number;
      lineCents: number | null;
      totalCents: number;
    };
  };
  readonly summary: {
    readonly input: Record<string, never>;
    readonly output: {
      totalCents: number;
      lines: number;
    };
  };
}

/** What a client sends to `handler`. */
export type InputOf<K extends HandlerName> = Handlers[K]["input"];

/** What `handler`'s operation result resolves to. */
export type OutputOf<K extends HandlerName> = Handlers[K]["output"];

/** The handle `send` returns for `handler`. */
export type OperationOf<K extends HandlerName> = OperationHandle<OutputOf<K>>;
