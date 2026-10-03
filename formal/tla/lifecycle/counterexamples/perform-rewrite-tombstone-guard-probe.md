# Probe: rewrite never fires over a tombstoned bucket (issue #1122)

Verified against the shipped code first: `erasure_rewrite_bucket`
(`crates/ravel-maintain/src/erasure_rewrite.rs`) does a fresh `list_bucket`
read at the top of the function and returns `ErasureRewriteOutcome::Tombstoned`
whenever `listing.tombstone_key.is_some()`, refusing to rewrite the bucket at
all. Round five's `PerformRewrite` guard (`~PresentObj("rwA") /\
PresentObj("dreqR1") /\ ~PresentObj("doneR1") /\ superseded = {} /\ \A i \in
Predecessors("rwA") : PresentObj(i)`) had no equivalent: `RetireBucket`
(writing `tombB1`) has no dependency on `dreqR1`, `doneR1`, or `superseded`,
so `tombB1` can be present while every one of those conjuncts still holds.

## Probe module

A scratch module under `/tmp`, `EXTENDS MCLifecycleGC`, adding:

```tla
ProbeNoRewriteOverTombstonedBucket ==
    PresentObj("tombB1") => ~(ENABLED PerformRewrite)

ProbeReachesPerformRewrite ==
    ~(PresentObj("rwA") /\ RawInputs \subseteq superseded)
```

Both checked as the sole non-`TypeOK` invariant in their own run,
`SPECIFICATION Spec`, `VIEW View`, every switch at `smoke.cfg`'s shipped
value, `MaxClock = 2`.

## Result against the model without the new guard (insufficient)

`PerformRewrite` with only its pre-existing five conjuncts (no
`~PresentObj("tombB1")`), `ProbeNoRewriteOverTombstonedBucket`. TLC exit 12:

```text
Error: Invariant ProbeNoRewriteOverTombstonedBucket is violated.
```

324 states generated, 169 distinct states found, 112 left on queue at the
first violation: `tombB1` present (`RetireBucket` fired) while `raw1` is
present, `dreqR1` is present, `doneR1` is absent and `superseded = {}` --
the old guard set's `ENABLED PerformRewrite` is still true there.

## Result against the shipped fix

`PerformRewrite` gated additionally on `~PresentObj("tombB1")`,
`ProbeNoRewriteOverTombstonedBucket`. TLC exit 0:

```text
Model checking completed. No error has been found.
```

276015 states generated, 50102 distinct states found, 0 left on queue, depth
21 (full state graph for this probe's cfg, not a bound run).

## Result: the rewrite is still reachable through the ordinary path

Same fixed model, `ProbeReachesPerformRewrite`. TLC exit 12:

```text
Error: Invariant ProbeReachesPerformRewrite is violated.
```

203 states generated, 113 distinct states found, 75 left on queue. 3-state
trace: `Init`, then `RequestErasure` (writes `dreqR1`), then `PerformRewrite`
-- the ordinary path (no tombstone ever written) still reaches the rewrite
output, so the new guard excludes the tombstoned state without killing the
action.
