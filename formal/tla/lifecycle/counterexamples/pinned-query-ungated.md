# Negative control: pinned-query-ungated

Switch: `HorizonGuardsPinnedQueries = FALSE` (drops the `QueryPermits` clause
from both `RetentionSweep` and `SupersededSweep`, so a horizon-gated delete no
longer refuses an object an in-window pinned query still needs). All other
switches at base, under `FullEnv = TRUE` and the full smoke invariant list
(`negative/pinned-query-ungated.cfg`). This is the permanent negative-control
form of candidate #1133; `candidate-1133.cfg` and
`counterexamples/candidate-1133.md` are the original manual investigation
(short invariant list, a six-state trace through `PerformRewrite` and
`HeadAdvanceRewrite`). Run here under the full smoke environment, TLC reaches
a shorter violation through plain `RetentionSweep` instead.

Target invariant: `NoDeleteInsideProtectionWindow`. TLC exit 12.

```text
Error: Invariant NoDeleteInsideProtectionWindow is violated.
```

Trace (six states, projected to the load-bearing variables):

1. Initial: `head = {raw1}`, `query` inactive, `clock = 0`, `sysgc.ph = 1`.
2. `PinQuery`: a reader pins on the current HEAD naming `raw1`.
   `query = [active |-> TRUE, deadline |-> 1, needs |-> {raw1}]`.
3. `RetireBucket`: the bucket's retention tombstone is written,
   `tombRetiredAt["b1"] = 0`.
4. `Tick`: `clock = 1`, so `clock >= tombRetiredAt["b1"] + sysgc.ph` (`1 >= 0 +
   1`) and the retention horizon gate passes.
5. `DropRetiredBucketFromHead`: `head = {}`, so the HEAD-not-named gate passes
   too, even though the query pinned in state 2 still needs `raw1`.
6. `RetentionSweep(raw1)`: with `HorizonGuardsPinnedQueries = FALSE` the delete
   is permitted. The witness records `rule |-> retention, deleted |->
   {raw1}, permittedNeeds |-> {raw1}, atClock |-> 1`. The query is still
   active with `clock (1) <= deadline (1)` and needs `raw1`.

`NoDeleteInsideProtectionWindow` requires, for any horizon-gated delete,
`deleted \cap permittedNeeds = {}`. Here the intersection is `{raw1}`, so the
invariant fails at state 6; TLC reports `5600 states generated, 2375 distinct
states found` before halting.

The shipped fix is `crates/ravel-maintain/src/reachability.rs`'s age gate
(`age_and_clear`, `ensure_part_last_modified_ms`): it anchors on the covering
or neighboring snapshot part's own `last_modified`, never on HEAD's, and
blocks a delete until `max_query_duration_ns + clock_skew_allowance_ns` (plus
one second of store `last_modified` granularity) have passed since that
anchor, which this model's `QueryPermits(o)` clause represents.
