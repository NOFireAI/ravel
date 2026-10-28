/**
 * The price list the `catalog.price` tool reads: a fixed table in memory.
 * Nothing here touches the network, reads a clock, draws a random number or
 * needs an API key, so the same SKU always gets the same price and a test can
 * count every lookup.
 */

/** Prices in cents by SKU. */
export const PRICES: ReadonlyMap<string, number> = new Map([
  ["A-1", 250],
  ["B-2", 1200],
  ["C-3", 75],
]);

export class PriceList {
  /** Every lookup, in order, including one a killed worker never finished. */
  readonly lookups: string[] = [];
  /**
   * While set, a lookup waits for this promise after recording itself. A test
   * holds a step open here to kill the worker inside it.
   */
  hold: Promise<void> | undefined;

  /** The price of `sku` in cents, or null for a SKU not on the list. */
  async price(sku: string): Promise<number | null> {
    this.lookups.push(sku);
    if (this.hold !== undefined) await this.hold;
    return PRICES.get(sku) ?? null;
  }
}
