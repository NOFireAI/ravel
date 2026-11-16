/**
 * A client for the order agent typed by `contract.ts`, the generated client
 * types, so a caller needs neither the agent definition nor zod to send it
 * messages.
 */
import type { AnyAgentDefinition } from "@ravel/agents";
import type { RavelAgentsClient, SendOptions } from "@ravel/agents/client";
import { AGENT_KIND, type HandlerName, type InputOf, type OperationOf, type OutputOf } from "./contract.js";

export interface OrderClient {
  readonly key: string;
  /** Send `input` to `handler` and return the operation's handle. */
  send<K extends HandlerName>(handler: K, input: InputOf<K>, options?: SendOptions): Promise<OperationOf<K>>;
  /** Send, then wait for the result. */
  call<K extends HandlerName>(handler: K, input: InputOf<K>, options?: SendOptions): Promise<OutputOf<K>>;
}

/** The order agent named `key`, through `client`. */
export function orderClient(client: RavelAgentsClient, key: string): OrderClient {
  const agent = client.agent<AnyAgentDefinition>(AGENT_KIND, key);
  const send = async <K extends HandlerName>(
    handler: K,
    input: InputOf<K>,
    options?: SendOptions,
  ): Promise<OperationOf<K>> => (await agent.send(handler, input, options)) as OperationOf<K>;
  return {
    key,
    send,
    call: async (handler, input, options) => (await send(handler, input, options)).result(),
  };
}
