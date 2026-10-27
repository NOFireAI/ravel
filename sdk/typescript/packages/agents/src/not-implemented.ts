/** The epic task that replaces every stub in this package with a runtime. */
export const IMPLEMENTING_TASK = "T12a";

/**
 * Thrown by every public function of this package until the runtime lands.
 * Deliberately outside the {@link RavelAgentsError} hierarchy: it reports a
 * missing implementation, not an outcome of the agent protocol.
 */
export class NotImplementedError extends Error {
  override readonly name = "NotImplementedError";
  readonly code = "NotImplemented";
  readonly task = IMPLEMENTING_TASK;

  constructor(readonly symbol: string) {
    super(`${symbol} is not implemented yet; task ${IMPLEMENTING_TASK} provides it`);
  }
}

export function notImplemented(symbol: string): NotImplementedError {
  return new NotImplementedError(symbol);
}
