# Non-vacuity mutant: HeldObjectNeverDeleted

Proves `HeldObjectNeverDeleted` is not vacuously true: a behaviour that deletes a
held object exists and the invariant catches it.

## Mutation

Behaviour edit (not a switch), applied to a scratch copy under `/tmp`. The
legal-hold gate is removed from the sweep bodies:

```
-    /\ ~HeldObject(o, heldBuckets)
```

removed from both `RetentionSweep` and `SupersededSweep`. Nothing else changed;
the run used the base `smoke.cfg` with every switch at its shipped value.

## Result

TLC exit 12. Exact line:

```
Error: Invariant HeldObjectNeverDeleted is violated.
```

A hold is placed on `b1`, a rewrite supersedes the raw input and advances HEAD,
and the superseded sweep then deletes the held input (the shortest path TLC
finds). The witness records `held |-> TRUE`, so `~lastGc.held` fails. Restoring
the gate makes the invariant hold again (smoke passes), which is the non-vacuity
argument: the invariant fails exactly when the modeled behaviour it constrains
is allowed to misbehave.
