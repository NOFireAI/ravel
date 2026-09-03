# Candidate #1133: sweep delete not gated on pinned queries

Verdict: CONFIRMED unsafe.

## Configuration

`candidate-1133.cfg` runs the model as shipped except
`HorizonGuardsPinnedQueries = FALSE`. That switch is the only change from the
smoke base. With it FALSE a sweep gates a delete on the protection horizon and
an unnamed HEAD only, and drops the clause that also refuses to delete an object
an in-window pinned query still needs (`QueryPermits`).

Invariant checked: `NoDeleteInsideProtectionWindow`. Result: violated, TLC exit
12.

Exact TLC line:

```
Error: Invariant NoDeleteInsideProtectionWindow is violated.
```

## Trace (six states, projected to the load-bearing variables)

1. Initial: `head = {raw1}`, `headState = present`, `clock = 0`, `query`
   inactive, `superseded = {}`, `supersededAt = 0`, `sysgc.ph = 1`.
2. `PinQuery`: a reader pins on the current HEAD.
   `query = [active |-> TRUE, deadline |-> 1, needs |-> {raw1}]`. The pin is
   valid while `clock <= 1`.
3. `PerformRewrite`: a rewrite over input `{raw1}` runs. It records the input as
   superseded, `superseded = {raw1}` with `supersededAt = 0`, and materialises
   the output `rwA` under a name derived from the input set and its applied
   requests (`variantKey`). HEAD still names `raw1`; the pinned query is
   unaffected.
4. `Tick`: `clock = 1`. Now `clock >= supersededAt + sysgc.ph` (`1 >= 0 + 1`),
   so the superseded-sweep horizon gate passes.
5. `HeadAdvanceRewrite`: `head = {rwA}`. HEAD advances to the rewrite output and
   no longer names `raw1`, so the sweep's HEAD-not-named gate now passes, even
   though the query pinned in state 2 still needs `raw1`.
6. `SupersededSweep(raw1)`: with `HorizonGuardsPinnedQueries = FALSE` the delete
   is permitted. The witness records
   `rule |-> superseded, deleted |-> {raw1}, permittedNeeds |-> {raw1},
   atClock |-> 1`. The query is still active with `clock (1) <= deadline (1)`
   and needs `raw1`, so `permittedNeeds = {raw1}`.

`NoDeleteInsideProtectionWindow` requires, for any horizon-gated delete,
`deleted \cap permittedNeeds = {}`. Here the intersection is `{raw1}`, so the
invariant fails at state 6.

## Interpretation

The horizon plus HEAD-not-named gate is not sufficient. A query pinned on the
previous HEAD outlives the rewrite that advances HEAD off the superseded input,
and the very next sweep can delete an object that query still needs. The horizon
measured from `supersededAt` coincides with the pinned query's deadline, so a
delete at `clock = 1` lands exactly while the query is in window. The shipped
model keeps the pinned-query clause (`HorizonGuardsPinnedQueries = TRUE`, the
smoke base), under which this trace has no successor: the sweep in state 6 is
disabled while the query is in window. Candidate #1133 should not be adopted; the
pinned-query clause is load-bearing for `NoDeleteInsideProtectionWindow`.
