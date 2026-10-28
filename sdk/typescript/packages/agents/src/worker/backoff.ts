/** A delay that doubles on each use, from `initialMs` up to `maxMs`, until reset. */
export class Backoff {
  private nextMs: number;

  constructor(
    private readonly initialMs: number,
    private readonly maxMs: number,
  ) {
    this.nextMs = Math.min(initialMs, maxMs);
  }

  /** The delay to wait now; the one after it is twice as long, up to the cap. */
  next(): number {
    const ms = this.nextMs;
    this.nextMs = Math.min(ms * 2, this.maxMs);
    return ms;
  }

  reset(): void {
    this.nextMs = Math.min(this.initialMs, this.maxMs);
  }
}

/** How many operations a worker keeps a backoff for before it forgets the least recently used. */
export const MAX_BACKOFF_OPERATIONS = 1024;

/**
 * One {@link Backoff} per operation id, so one operation's doubling delay is
 * never applied to another. An operation that leaves the worker without a
 * commit (taken over or cancelled elsewhere) is forgotten once `capacity`
 * others have been used after it.
 */
export class OperationBackoffs {
  private readonly byOperation = new Map<string, Backoff>();

  constructor(
    private readonly initialMs: number,
    private readonly maxMs: number,
    private readonly capacity: number = MAX_BACKOFF_OPERATIONS,
  ) {}

  /** The delay to wait now for `operation`; its next one doubles, up to the cap. */
  next(operation: string): number {
    let backoff = this.byOperation.get(operation);
    if (backoff === undefined) {
      backoff = new Backoff(this.initialMs, this.maxMs);
    } else {
      this.byOperation.delete(operation);
    }
    this.byOperation.set(operation, backoff);
    if (this.byOperation.size > this.capacity) {
      const oldest = this.byOperation.keys().next();
      if (oldest.done !== true) this.byOperation.delete(oldest.value);
    }
    return backoff.next();
  }

  /** Start `operation` over at the initial delay. */
  forget(operation: string): void {
    this.byOperation.delete(operation);
  }

  clear(): void {
    this.byOperation.clear();
  }

  get size(): number {
    return this.byOperation.size;
  }
}
