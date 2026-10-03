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

## Trace (seven states, as TLC printed it)

Recorded after reader deadlines became exclusive (issue #2339, round fifteen):
a query reads while `clock < deadline`.

1. Initial: `head = {raw1}`, `clock = 0`, no query, no tombstone.
2. `Tick`: `clock = 1`.
3. `RetireBucket`: the retention tombstone for `b1` lands with
   `tombRetiredAt["b1"] = 1`, so the retention horizon is clock 2.
4. `Tick`: `clock = 2`.
5. `PinQuery`: a query pins the current HEAD, which still names `raw1`.
   `query = [active |-> TRUE, needs |-> {raw1}, deadline |-> 3]`.
6. `DropRetiredBucketFromHead`: the fold drops `b1` from HEAD, `head = {}`. The
   query keeps reading the HEAD it pinned.
7. `RetentionSweep(raw1)` at clock 2: horizon passed, HEAD names nothing in
   `b1`, and nothing else is checked. The query is still in its window
   (`2 < 3`). The witness records
   `rule |-> retention, deleted |-> {raw1}, permittedNeeds |-> {raw1}`.

## The step that breaks safety

State 7. Clause 4 of `NoDeleteInsideProtectionWindow` requires
`deleted \cap permittedNeeds = {}` for every horizon-gated delete; here it is
`{raw1}`. The original candidate trace in `candidate-1133.md` reaches the same
violation through the superseded sweep; the trace TLC printed for this run is
the retention path.
