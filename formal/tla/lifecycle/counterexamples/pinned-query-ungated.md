# negative/pinned-query-ungated.cfg: no pinned-query guard and no window gate

Verdict: VIOLATED as expected, `NoDeleteInsideProtectionWindow`, TLC exit 12.

## Configuration

`candidate-1133.cfg` promoted to a negative control (ADR-1133 decision 7). The
model runs at the smoke-style bounds of that cfg (`MaxClock = 2`,
`ProtectionHorizon = 1`, `MaxQueryDuration = 1`, `ClockSkew = 0`, `FullEnv`)
with `HorizonGuardsPinnedQueries = FALSE` and `WindowGate = FALSE`: a sweep
delete gates on the protection horizon and an unnamed HEAD only. This is the
delete as it shipped before ADR-1133.

Exact TLC line:

```text
Error: Invariant NoDeleteInsideProtectionWindow is violated.
```

## Trace (six states, as TLC printed it)

1. Initial: `head = {raw1}`, `clock = 0`, no query, no tombstone.
2. `PinQuery`: a query pins the current HEAD.
   `query = [active |-> TRUE, needs |-> {raw1}, deadline |-> 1]`.
3. `RetireBucket`: the retention tombstone for `b1` lands with
   `tombRetiredAt["b1"] = 0`.
4. `DropRetiredBucketFromHead`: the fold drops `b1` from HEAD, `head = {}`. The
   query keeps reading the HEAD it pinned.
5. `Tick`: `clock = 1`, so the retention horizon `0 + 1` has passed. The query is
   still in its window (`1 <= 1`).
6. `RetentionSweep(raw1)`: horizon passed, HEAD names nothing in `b1`, and
   nothing else is checked. The witness records
   `rule |-> retention, deleted |-> {raw1}, permittedNeeds |-> {raw1}`.

## The step that breaks safety

State 6. Clause 4 of `NoDeleteInsideProtectionWindow` requires
`deleted \cap permittedNeeds = {}` for every horizon-gated delete; here it is
`{raw1}`. The original candidate trace in `candidate-1133.md` reaches the same
violation through the superseded sweep; the trace TLC printed for this run is
the retention path, the same length.
