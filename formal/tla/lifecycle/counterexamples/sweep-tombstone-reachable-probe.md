# Probe: SweepTombstone is reachable

Finding 3 (round four, issue #1122) added `SweepTombstone`: the missing
final tombstone delete after `physical_sweep`'s `bucket_is_empty_but_tombstone`
check. A new action that is only reachable on paper (an unsatisfiable guard
combination) would make its own invariant vacuously true, so the addition
needs its own reachability check.

## Probe module

A scratch module under `/tmp`, `EXTENDS MCLifecycleGC`, checked with
`SPECIFICATION Spec`, `VIEW View`, `INVARIANT TypeOK` plus the probe,
`smoke.cfg`'s constants.

```tla
ProbeReachesSweepTombstone ==
    lastGc.rule # "tombstone"
```

(Every other action calls `NoGc`, which resets `lastGc.rule` to `"none"`
each step, so `lastGc.rule = "tombstone"` is true only in the state
`SweepTombstone` just produced restating "that state is never reached"
as an invariant makes a TLC violation exactly the proof needed.)

## Result

Violates (TLC exit 12): 4821 states generated, 1805 distinct, depth 8. The
trace is `RetireBucket` (writes the tombstone), `DropRetiredBucketFromHead`
(drops `raw1` from HEAD), `Tick`, `RetentionSweep` (deletes `raw1`,
`lastGc.rule = "retention"`), `SweepTombstone` (deletes `tombB1`,
`lastGc.rule = "tombstone"`) exactly the order `physical_sweep` follows:
data objects first, tombstone last, only once the bucket holds nothing else.
