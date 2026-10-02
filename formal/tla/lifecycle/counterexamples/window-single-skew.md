# negative/window-single-skew.cfg: the window gate with one skew term

Verdict: VIOLATED as expected, `NoDeleteInsideProtectionWindow`, TLC exit 12.

## Configuration

`positive/window-gate.cfg` (`HorizonGuardsPinnedQueries = FALSE`,
`WindowGate = TRUE`, `MaxQueryDuration = 1`, `HeadCacheTtl = 1`,
`ClockSkew = 1`, `ProtectionHorizon = 2`, `MaxClock = 8`) with
`WindowSingleSkew = TRUE`. The window shrinks from `1 + 1 + 4 * 1 = 6` to
`1 + 1 + 1 * 1 = 3`.

Exact TLC line:

```text
Error: Invariant NoDeleteInsideProtectionWindow is violated.
```

## Trace (ten states, as TLC printed it)

1. Initial: `head = {raw1}`, `clock = 0`.
2. `RetireBucket`: `tombRetiredAt["b1"] = 0`, so the retention horizon is clock 2.
3. `Tick`, 4. `Tick`: `clock = 2`.
5. `PinQuery`: a query pins the current HEAD, which still names `raw1`. Its
   Flight ticket term is 1 (of the up to `2 * ClockSkew` it may take), so
   `deadline = 2 + 1 + 1 = 4`.
6. `DropRetiredBucketFromHead`: `head = {}` at clock 2.
7. `WriteMarker(b1)`: the sweep sees `b1` past its horizon and unnamed and writes
   the marker. The writer's clock runs one behind true time: its reading is 1,
   stored shifted by `ClockSkew` as `obs = 2`.
8. `Tick`, 9. `Tick`: `clock = 4`.
10. `RetentionSweep(raw1)`: the deleting sweeper's clock leads by one and reads
    5. The shortened gate `1 + 3 < 5` opens (in the model's shifted form,
    `obs + 3 < clock + ClockSkew + lead`, `5 < 6`). The witness records
    `deleted |-> {raw1}, permittedNeeds |-> {raw1}` at clock 4.

## The step that breaks safety

State 10. The query is still in its window (`4 <= 4`) and needs `raw1`. With the
full `4 * ClockSkew` the gate would need the sweeper's clock past `1 + 6 = 7`,
true clock 7, three ticks after the query's deadline. The trace spends three of
the four sigma terms: the writer's lag (state 7), one Flight ticket term
(state 5) and the sweeper's lead (state 10). One term of margin cannot absorb
them.
