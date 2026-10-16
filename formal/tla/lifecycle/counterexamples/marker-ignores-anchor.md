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

## Trace (ten states, as TLC printed it)

Recorded after reader deadlines became exclusive (issue #2339, round fifteen):
a query reads while `clock < deadline`, and the gate compares with `<=`.

1. Initial: `head = {raw1}`, `clock = 0`.
2. `RetireBucket`: `tombRetiredAt["b1"] = 0`.
3. to 6. `Tick` four times: `clock = 4`.
7. `PinQuery`: a query pins the current HEAD, which names `raw1`, with
   `deadline = 4 + 1 + 0 = 5`.
8. `DropRetiredBucketFromHead`: `head = {}` at clock 4, and
   `cacheUntil[raw1] = 5`.
9. `StaleMarker(b1)`: a marker for anchor 9 (`StaleAnchor`, no live anchor)
   with `obs = 0` appears under `b1`.
10. `RetentionSweep(raw1)`: the anchor does not match `tombRetiredAt["b1"] = 0`,
    but the switch accepts it; its window has passed
    (`0 + 6 <= 4 + 1 + 1`). The witness records
    `deleted |-> {raw1}, permittedNeeds |-> {raw1}` at clock 4.

## The step that breaks safety

State 10, in the same tick as the drop, while the query reads (`4 < 5`). No
window has passed since HEAD stopped naming `raw1`; the marker's reading
predates the drop because it was written for another anchor. With the anchor
check, state 9's marker would count as absent and `RenewMarker` would rewrite
it with a fresh reading, restarting the window.
