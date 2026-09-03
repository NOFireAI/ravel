# Candidate #1133: retention delete not gated on pinned queries

Verdict: CONFIRMED unsafe.

## Configuration

`candidate-1133.cfg` runs the model as shipped except
`HorizonGuardsPinnedQueries = FALSE`. That switch is the only change from the
smoke base. With it FALSE the retention sweep gates a delete on two conditions
only, the protection horizon and an empty effective HEAD for the bucket, and
drops the third clause that also refuses to delete an object an in-window pinned
query still needs (`QueryPermits`).

Invariant checked: `NoDeleteInsideProtectionWindow`. Result: violated, TLC exit
12.

Exact TLC line:

```
Error: Invariant NoDeleteInsideProtectionWindow is violated.
```

## Trace (six states, projected to the load-bearing variables)

1. Initial: `head = {raw1}`, `headState = present`, `clock = 0`, `query`
   inactive, `tombRetiredAt[b1] = 0`, `sysgc.ph = 1`.
2. `PinQuery`: a reader pins on the current HEAD.
   `query = [active |-> TRUE, deadline |-> 1, needs |-> {raw1}]`. The pin is
   valid while `clock <= 1`.
3. `SetHeadState`: `headState = absent`. This is the late fold: a new HEAD is
   published that no longer names the bucket, so `EffectiveHead` is empty and
   the sweep's head-empty gate now passes, even though the query pinned in
   state 2 still needs `raw1`.
4. `RetireBucket`: the bucket's tombstone is written, `tombRetiredAt[b1]`
   observed at 0.
5. `Tick`: `clock = 1`. Now `clock >= tombRetiredAt[b1] + sysgc.ph`
   (`1 >= 0 + 1`), so the horizon gate passes.
6. `RetentionSweep(raw1)`: with `HorizonGuardsPinnedQueries = FALSE` the delete
   is permitted. The witness records
   `rule |-> retention, deleted |-> {raw1}, permittedNeeds |-> {raw1},
   atClock |-> 1`. The query is still active with `clock (1) <= deadline (1)`
   and needs `raw1`, so `permittedNeeds = {raw1}`.

`NoDeleteInsideProtectionWindow` clause 2 requires, for any horizon-gated
delete, `deleted \cap permittedNeeds = {}`. Here the intersection is `{raw1}`,
so the invariant fails at state 6.

## Interpretation

The horizon plus head-empty gate is not sufficient. A query pinned on the
previous HEAD outlives the fold that removes the bucket from the new HEAD, and
the very next sweep can delete an object that query still needs. The shipped
model keeps the third clause (`HorizonGuardsPinnedQueries = TRUE`, the smoke and
exhaustive base), under which this trace has no successor: the sweep in state 6
is disabled while the query is in window. Candidate #1133 should not be adopted;
the pinned-query clause is load-bearing for `NoDeleteInsideProtectionWindow`.
