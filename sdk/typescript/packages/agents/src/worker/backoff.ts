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
