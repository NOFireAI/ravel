/**
 * Client entry: send messages to agents and follow their operations. It
 * imports no module from this package's worker, tools or testing entries and
 * none of the child_process, fs and net builtins, which index.test.ts
 * enforces over this package's own module graph; the test does not follow
 * third-party packages.
 */
import { notImplemented } from "../not-implemented.js";
import type {
  AnyAgentDefinition,
  MessageNames,
  OperationState,
  PayloadOf,
  ResultOf,
} from "../types.js";

export * from "../errors.js";
export { NotImplementedError } from "../not-implemented.js";
export type {
  AnyAgentDefinition,
  MessageNames,
  OperationState,
  PayloadOf,
  ResultOf,
} from "../types.js";

export interface ClientOptions {
  /** Base URL of a Ravel server started with `--agents`. */
  readonly url: string;
  readonly token: string;
}

export interface SendOptions {
  /** Idempotency key; the SDK generates one when absent. */
  readonly requestId?: string;
}

export interface WaitOptions {
  readonly timeoutMs?: number;
  readonly signal?: AbortSignal;
}

export interface OperationStatus {
  readonly operationId: string;
  readonly state: OperationState;
}

export interface OperationHandle<R> {
  readonly operationId: string;
  readonly requestId: string;
  /** Wait for the result. A wait that times out changes nothing on the server. */
  wait(options?: WaitOptions): Promise<R>;
  status(): Promise<OperationStatus>;
  /** Idempotent cancellation. */
  cancel(): Promise<void>;
}

export interface AgentClient<A extends AnyAgentDefinition> {
  readonly agentId: string;
  send<K extends MessageNames<A>>(
    handler: K,
    payload: PayloadOf<A, K>,
    options?: SendOptions,
  ): Promise<OperationHandle<ResultOf<A, K>>>;
  operation<K extends MessageNames<A>>(
    handler: K,
    operationId: string,
  ): OperationHandle<ResultOf<A, K>>;
}

export interface RavelAgentsClient {
  /**
   * Address one agent. Pass the agent's definition type, imported with
   * `import type`, so payloads and results are checked at compile time.
   */
  agent<A extends AnyAgentDefinition = never>(agentType: A["name"], agentId: string): AgentClient<A>;
}

export function createClient(_options: ClientOptions): RavelAgentsClient {
  throw notImplemented("createClient");
}
