# negative/marker-ignores-anchor.cfg: a marker accepted for any anchor

Verdict: VIOLATED as expected, `NoDeleteInsideProtectionWindow`, TLC exit 12.

## Configuration

`positive/window-gate.cfg` with `MarkerIgnoresAnchor = TRUE`. The gate accepts
a marker whatever anchor it records, and `RenewMarker` (ADR-1133 decision 2:
delete and rewrite a marker whose anchor does not match) is disabled.
`StaleMarker` is the environment action that leaves a marker written for
another anchor under a key, with the oldest reading the model can hold.

Exact TLC line:

```text
Error: Invariant NoDeleteInsideProtectionWindow is violated.
```

## Trace (eleven states, as TLC printed it)

1. Initial: `head = {raw1}`, `clock = 0`.
2. `RetireBucket`: `tombRetiredAt["b1"] = 0`.
3. to 7. `Tick` five times: `clock = 5`.
8. `PinQuery`: a query pins the current HEAD, which names `raw1`, with
   `deadline = 5 + 1 + 0 = 6`.
9. `DropRetiredBucketFromHead`: `head = {}` at clock 5.
10. `StaleMarker(b1)`: a marker for anchor 9 (`StaleAnchor`, no live anchor)
    with `obs = 0` appears under `b1`.
11. `RetentionSweep(raw1)`: the anchor does not match `tombRetiredAt["b1"] = 0`,
    but the switch accepts it; its window has long passed
    (`0 + 6 < 5 + 1 + 1`). The witness records
    `deleted |-> {raw1}, permittedNeeds |-> {raw1}` at clock 5.

## The step that breaks safety

State 11, in the same tick as the drop. No window has passed since HEAD stopped
naming `raw1`; the marker's reading predates the drop because it was written
for another anchor. With the anchor check, state 10's marker would count as
absent and `RenewMarker` would rewrite it with a fresh reading, restarting the
window.
