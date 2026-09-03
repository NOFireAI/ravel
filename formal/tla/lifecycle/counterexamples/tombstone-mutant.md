# Non-vacuity mutant: TombstoneExcludesBeforeDelete

Proves `TombstoneExcludesBeforeDelete` is not vacuously true: a retention delete
that runs before its tombstone exists is reachable and the invariant catches it.

## Mutation

Behaviour edit (not a switch), applied to a scratch copy under `/tmp`. The
tombstone-present precondition is removed from both sweep-enabling actions:

```
-    /\ PresentObj("tombB1")
```

removed from `RetentionSweep` and from `DropRetiredBucketFromHead`. Both are
needed: `RetentionSweep` gates on the HEAD naming nothing in the bucket, and the
only action that empties the bucket from HEAD is `DropRetiredBucketFromHead`,
which itself requires the tombstone. Removing the guard from the sweep alone is
unreachable, because HEAD can only be emptied after the tombstone exists, so the
tombstone is always present by the time the sweep could fire. The `RetireBucket`
guard (`~PresentObj("tombB1")`) and the invariant's own `PresentObj("tombB1")`
clause are untouched. The run used the base `smoke.cfg`.

## Result

TLC exit 12. Exact line:

```
Error: Invariant TombstoneExcludesBeforeDelete is violated.
```

With the guard gone the sweep deletes a `b1` object while `tombB1` is still
absent (the horizon test `clock >= tombRetiredAt[b1] + sysgc.ph` passes at
`clock = 1` against the initial `tombRetiredAt[b1] = 0`). The witness carries
`rule |-> retention`, so the invariant's `PresentObj("tombB1")` clause is
evaluated and fails. Restoring the guard makes smoke pass.
