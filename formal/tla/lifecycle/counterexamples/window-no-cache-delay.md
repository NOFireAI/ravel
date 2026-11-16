# negative/window-no-cache-delay.cfg: the window gate without the cache term

Verdict: VIOLATED as expected, `NoDeleteInsideProtectionWindow`, TLC exit 12.

## Configuration

`positive/window-gate.cfg` with `WindowNoCacheDelay = TRUE`. The gate's window
drops `HeadCacheTtl` and becomes `1 + 4 * 1 = 5`, while a pin can still be
served a cached HEAD that names an object through drop clock + `HeadCacheTtl`.

Exact TLC line:

```text
Error: Invariant NoDeleteInsideProtectionWindow is violated.
```

## Trace (eleven states, as TLC printed it)

Recorded after reader deadlines became exclusive (issue #2339, round fifteen):
a query reads while `clock < deadline`, and the gate compares with `<=`.

1. Initial: `head = {raw1}`, `clock = 0`.
2. `RetireBucket`: `tombRetiredAt["b1"] = 0`.
3. `Tick`, 4. `Tick`: `clock = 2`.
5. `DropRetiredBucketFromHead`: `head = {}` at clock 2, and
   `cacheUntil[raw1] = 2 + 1 = 3`.
6. `WriteMarker(b1)`: writer's reading 1 (one behind), stored `obs = 2`.
7. `Tick`: `clock = 3`.
8. `PinQuery`: the current HEAD names nothing, but the resolve is served the
   cached HEAD (`3 <= cacheUntil[raw1]`), so the query pins `raw1`. Its Flight
   ticket term is 2, so `deadline = 3 + 1 + 2 = 6` and it reads through clock 5.
9. `Tick`, 10. `Tick`: `clock = 5`.
11. `RetentionSweep(raw1)`: the sweeper reads 6 (one ahead). The shortened gate
    `1 + 5 <= 6` opens (shifted, `obs + 5 <= clock + ClockSkew + lead`,
    `7 <= 7`). The witness records
    `deleted |-> {raw1}, permittedNeeds |-> {raw1}` at clock 5.

## The step that breaks safety

State 11. The query pinned in state 8 through the cache, after the drop, and is
still in its window (`5 < 6`). The pin in state 8 is only possible through the
cache: HEAD had named nothing since state 5. With the `HeadCacheTtl` term the
gate would need the sweeper's clock at `1 + 6 = 7`, true clock 6, the first
tick at which the query no longer reads.
