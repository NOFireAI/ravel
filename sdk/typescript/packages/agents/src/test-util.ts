/** Helpers for this package's own tests; no entry point exports them. */

export interface Gate {
  readonly wait: Promise<void>;
  open(): void;
}

export function gate(): Gate {
  let open = () => {
    // replaced below
  };
  const wait = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { wait, open };
}

/** Resolve once `condition` holds, polling every millisecond; reject after `ms`. */
export async function until(condition: () => boolean, ms = 2_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("condition not reached in time");
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}
