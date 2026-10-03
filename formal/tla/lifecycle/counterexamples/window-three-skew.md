# negative/window-three-skew.cfg: the window gate one skew term short

Verdict: VIOLATED as expected, `NoDeleteInsideProtectionWindow`, TLC exit 12.

## Configuration

`positive/window-gate.cfg` (`HorizonGuardsPinnedQueries = FALSE`,
`WindowGate = TRUE`, `MaxQueryDuration = 1`, `HeadCacheTtl = 1`,
`ClockSkew = 1`, `ProtectionHorizon = 2`, `MaxClock = 8`) with
`WindowThreeSkew = TRUE`. The window shrinks from `1 + 1 + 4 * 1 = 6` to
`1 + 1 + 3 * 1 = 5`, one sigma short of decision 3. Reader deadlines are
exclusive (a query reads while `clock < deadline`) and the gate compares with
`<=`, as in the positive cfg.

Exact TLC line:

```text
Error: Invariant NoDeleteInsideProtectionWindow is violated.
```

## Trace (eleven states, as TLC printed it)

1. Initial: `head = {raw1}`, `clock = 0`.
2. `RetireBucket`: `tombRetiredAt["b1"] = 0`, so the retention horizon is clock 2.
3. `Tick`, 4. `Tick`: `clock = 2`.
5. `DropRetiredBucketFromHead`: `head = {}` at clock 2, and
   `cacheUntil[raw1] = 3`: a resolve may still be served the cached HEAD that
   names `raw1` through clock 3.
6. `WriteMarker(b1)`: the sweep sees `b1` past its horizon and unnamed and writes
   the marker. The writer's clock runs one behind true time: its reading is 1,
   stored shifted by `ClockSkew` as `obs = 2`.
7. `Tick`: `clock = 3`.
8. `PinQuery`: a query pins through the cache (`CachedNames(raw1)`, clock
   3 <= 3) with the full Flight ticket term of 2, so
   `deadline = 3 + 1 + 2 = 6` and it reads through clock 5.
9. `Tick`, 10. `Tick`: `clock = 5`.
11. `RetentionSweep(raw1)`: the deleting sweeper's clock leads by one and reads
    6. The shortened gate `1 + 5 <= 6` opens (in the model's shifted form,
    `obs + 5 <= clock + ClockSkew + lead`, `7 <= 7`). The witness records
    `deleted |-> {raw1}, permittedNeeds |-> {raw1}` at clock 5.

## The step that breaks safety

State 11. The query is still in its window (`5 < 6`) and needs `raw1`. The
trace spends all four sigma terms: the writer's lag (state 6), both Flight
ticket terms (state 8) and the sweeper's lead (state 11), on top of the full
cache delay and `MaxQueryDuration`. With the full `4 * ClockSkew` the gate
would need the sweeper's clock at `1 + 6 = 7`, true clock 6, the first tick at
which the query no longer reads. Three terms of margin cannot absorb four.
