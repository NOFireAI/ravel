# Probe: a completion can land with a bucket open but out of scope

`RequestErasure` is the only writer of `ingestAckScope`, so a bucket that opens
AFTER the acknowledgement is never captured and never blocks a completion. The
model's docs and `traceability.md` state this as "an hour that opens after the
acknowledgement is out of scope and holds nothing", and the completion guard
`OpenBucketPermitsCompletion` reads the captured scope
(`IngestUnsealedInScope == ingestAckScope /\ ingestPhase # "sealed"`), not the
live bucket.

No shipped lane falsifies the out-of-scope claim. The safety invariant
`CompletionCoversEveryBucketOpenAtRequest` reads the same completion witness the
guard feeds, so a strictly stronger guard (one that also blocked a bucket opened
after the ack) would pass every shipped invariant unchanged: on a scratch copy
with the `ingestAckScope` conjunct deleted from `IngestUnsealedInScope`, smoke
still passes all eighteen invariants at 2,203,866 distinct states. Safety
invariants cannot catch over-strictness. This probe supplies the missing
reachability evidence: it shows the shipped model actually reaches a completion
with an open, out-of-scope bucket, which is what makes the `ingestAckScope`
conjunct load-bearing rather than decorative.

## The probe: assert the out-of-scope completion is unreachable

A scratch copy under `/tmp` carries the shipped model unchanged and one added
state invariant:

```tla
NoCompletionWithOutOfScopeOpenBucket ==
    ~(PresentObj("doneR1") /\ ingestPhase = "open" /\ ~ingestAckScope)
```

`PresentObj("doneR1")` is a landed completion; `ingestPhase = "open"` is a live
open bucket; `~ingestAckScope` is that bucket being out of scope (opened after
the acknowledgement, so `RequestErasure` never captured it). The constants are
`smoke.cfg`'s at `MaxClock = 1`, `SPECIFICATION Spec`, `VIEW View`, and this one
`INVARIANT`. It is a state invariant, so a violation is TLC exit 12. The probe
cfg is not shipped as a control: it is a reachability witness, not a mutant of
the model.

## Result: violated

TLC reaches the state and reports the invariant violated:

```text
Error: Invariant NoCompletionWithOutOfScopeOpenBucket is violated.
Error: The behavior up to this point is:
State 1: <Initial predicate>
State 2: <RequestErasure ...>            ingestPhase = "absent", so ingestAckScope stays FALSE
State 3: <OpenBucket ...>                a bucket opens AFTER the ack; ingestAckScope untouched
State 4: <Tick ...>                      clock = 1
State 5: <RetireBucket ...>
State 6: <DropRetiredBucketFromHead ...>
State 7: <CompleteErasure ...>           doneR1 lands with ingestPhase = "open", ingestAckScope = FALSE
```

At State 7 the witness holds all three conjuncts:

```text
/\ ingestPhase = "open"
/\ ingestAckScope = FALSE
/\ store["doneR1"] = [present |-> TRUE, content |-> "dat", version |-> 4]
/\ clock = 1
```

```text
20275 states generated, 6876 distinct states found, 4148 states left on queue.
The depth of the complete state graph search is 8.
```

Exit 12, counterexample seven states (six transitions). The `RequestErasure` in
State 2 fires while `ingestPhase = "absent"`, so its accumulating capture
`ingestAckScope' = (ingestAckScope \/ ingestPhase = "open")` leaves
`ingestAckScope` FALSE. `OpenBucket` in State 3 opens a bucket without touching
`ingestAckScope`. At the completion in State 7,
`IngestUnsealedInScope = ingestAckScope /\ ingestPhase # "sealed" = FALSE`, so
`OpenBucketPermitsCompletion` holds even though a bucket is open: the open bucket
is genuinely out of scope.

## Non-vacuity of the probe itself

The probe would be worthless if `NoCompletionWithOutOfScopeOpenBucket` were
violated regardless of the guard's shape. Strengthening the guard so that any
open unsealed bucket blocks completion, in scope or not, must make the state
unreachable. Deleting the `ingestAckScope` conjunct from `IngestUnsealedInScope`
in the scratch copy:

```tla
-IngestUnsealedInScope == ingestAckScope /\ ingestPhase # "sealed"
+IngestUnsealedInScope == ingestPhase # "sealed"
```

With that stronger guard the same probe cfg passes:

```text
Model checking completed. No error has been found.
14463840 states generated, 2203866 distinct states found, 0 states left on queue.
```

Exit 0: no completion lands while any bucket is open, so the out-of-scope state
is unreachable and the probe invariant holds. The probe therefore distinguishes
the shipped ack-scoped guard (out-of-scope completion reachable, VIOLATED) from
an over-strict guard that blocks after the ack (unreachable, no error). The
2,203,866-distinct figure is the same run that passes all eighteen safety
invariants under the stronger guard, which is why safety alone cannot flag the
over-strictness and why this reachability witness is needed.

## What this pins for the shipped model

`RequestErasure` remains the only writer of `ingestAckScope`, and the shipped
guard reads that captured scope. The probe shows the model reaches a completion
that legitimately proceeds while a bucket is open but out of scope, so the
`ingestAckScope` conjunct is exercised, not vacuously satisfied. If a future
change widened the scope to capture buckets opened after the acknowledgement,
this reachable completion would disappear and the over-strictness would be
visible here rather than passing silently through every safety invariant.

## Execution note

Run on a fleet executor with no system Java. A Temurin JRE 21 was fetched into
`/tmp` and TLC (`tla2tools-1.7.4`, TLC2 v2.19) was run with `-workers 2 -Xmx2g`;
the exit codes, trace, and state totals above are that run's output. The scratch
model and cfg live under `/tmp` and are not committed. The shipped configs carry
`CompletionCoversEveryBucketOpenAtRequest` over the unmodified model, where the
open bucket's scope is captured only at the acknowledgement.
