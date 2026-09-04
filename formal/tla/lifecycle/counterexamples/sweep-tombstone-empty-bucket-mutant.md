# Non-vacuity mutant: TombstoneNotDeletedBeforeBucketEmpty

Proves `TombstoneNotDeletedBeforeBucketEmpty` is not vacuously true: a
behaviour that deletes the tombstone while the bucket still holds a data
object is reachable and the invariant catches it.

## Mutation

Behaviour edit (not a switch), applied to a scratch copy under `/tmp`. The
bucket-empty precondition is removed from `SweepTombstone`:

```tla
-    /\ \A o \in DataObjects : Bucket(o) = "b1" => ~PresentObj(o)
```

Nothing else changed; the run used `smoke.cfg`'s constants with every switch
at its shipped value.

## Result

TLC exit 12. Exact line:

```text
Error: Invariant TombstoneNotDeletedBeforeBucketEmpty is violated.
```

1021 states generated, 462 distinct states found. The shortest trace is
`RetireBucket` (writes the tombstone), `DropRetiredBucketFromHead` (drops
`raw1` from HEAD), `SweepTombstone` (deletes `tombB1` directly, skipping
`RetentionSweep`): the resulting state has `store.raw1.present = TRUE` and
`store.tombB1.present = FALSE` with `lastGc.rule = "tombstone"`, so the
invariant's `~PresentObj(o)` clause for `raw1` fails. Restoring the guard
makes smoke pass again, which is the non-vacuity argument: the invariant
fails exactly when the modeled behaviour it constrains is allowed to
misbehave.
