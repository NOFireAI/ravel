# Negative control: completion-ignores-open-bucket

Switch: `CompletionIgnoresOpenBucket = TRUE` (drops the ack-open-bucket guard on
`CompleteErasure`, so a `.done` can land while a bucket that was open at the
request's acknowledgement is still unsealed, or still serves a record accepted
before that acknowledgement). All other switches at base.

Target invariant: `CompletionCoversEveryBucketOpenAtRequest`. TLC exit 12.

```text
Error: Invariant CompletionCoversEveryBucketOpenAtRequest is violated.
```

Trace (7 states, 13252 distinct explored, depth 8): `OpenBucket` opens the ingest
bucket at clock 0, `RequestErasure` acknowledges the erasure request while it is
open and captures `ingestAckScope = TRUE`, `RetireBucket` and
`DropRetiredBucketFromHead` clear HEAD of the erased subject, `Tick` advances the
clock to 1, and `CompleteErasure` then writes `.done` with the bucket still
`ingestPhase = "open"`. The completion witness records

```text
/\ lastGc = [ rule |-> "complete",
  openUnsealedInScope |-> TRUE,
  openLateServed |-> FALSE ]
```

so the invariant's first clause (`~lastGc.openUnsealedInScope`) fails.

## Both clauses, by behaviour mutant

TLC stops at the shallowest violation, so this control only exercises the first
clause. Each clause was additionally shown load-bearing by deleting it from the
model in a scratch copy with `CompletionIgnoresOpenBucket = FALSE`, running
`scripts/check-tla.sh smoke -a lifecycle`, and reading the trace.

Deleting the whole guard conjunct from `CompleteErasure`:

```text
-    /\ (CompletionIgnoresOpenBucket \/ OpenBucketPermitsCompletion)
```

```text
Error: Invariant CompletionCoversEveryBucketOpenAtRequest is violated.
```

TLC exit 12, depth 8, 9505 distinct states explored. This is the proof that the
switch is wired to the guard the shipped code carries, not to a dead conjunct.

Deleting only the second conjunct of `OpenBucketPermitsCompletion`, so the guard
still waits for the seal but no longer waits for the pre-ack record:

```text
 OpenBucketPermitsCompletion ==
     /\ ~IngestUnsealedInScope
-    /\ ~IngestLateStillServes
```

```text
Error: Invariant CompletionCoversEveryBucketOpenAtRequest is violated.
```

TLC exit 12, depth 10, 39497 distinct states explored. The trace is
`OpenBucket`, `Tick`, `IngestLateWrite`, `RequestErasure`, `SealBucket`,
`RetireBucket`, `DropRetiredBucketFromHead`, `CompleteErasure`: the record lands
in the open bucket before the acknowledgement, the bucket then seals, and the
completion publishes while the record is still served. Its witness is the mirror
image of the control's:

```text
/\ lastGc = [ rule |-> "complete",
  openUnsealedInScope |-> FALSE,
  openLateServed |-> TRUE ]
```

## Why the ack capture accumulates

`RequestErasure` sets `ingestAckScope' = (ingestAckScope \/ ingestPhase =
"open")` rather than overwriting it. `DreqSweep` removes the `.dreq`, which
re-enables `RequestErasure`, and an overwriting capture let that second
acknowledgement discharge an obligation the first one took on. The transition was
reachable: with the overwriting form, the action property

```text
AckScopeNeverCleared == [][ingestAckScope => ingestAckScope']_vars
```

fails on `smoke.cfg` at depth 11,

```text
Error: Action property AckScopeNeverCleared is violated.
```

with the trace `OpenBucket`, `RequestErasure`, `Tick`, `SealBucket`,
`RetireBucket`, `DropRetiredBucketFromHead`, `CompleteErasure`, `DreqSweep`,
`RequestErasure`. It holds with the accumulating form. No completion is reachable
after the clearing step in the current model, because `doneR1` is written once and
never deleted and `DreqSweep` requires it present, so the overwriting form was not
observably unsafe here. The accumulating form is what makes the guard independent
of that coincidence: only `SealBucket` and `RewriteOpenBucket` discharge the
obligation.
