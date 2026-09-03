# reconcile-on-tick

Negative control. Switch `ReconcileOnTick = TRUE` enables `DoReconcileTick`,
which re-reconciles HEAD in place without advancing the watermark.

Violated invariant: `ReconcileOnlyOnWatermarkAdvance` (safety, TLC exit 12).

## Trace shape

1. A fold commits a valid HEAD at some watermark `w` naming a live L0 commit.
2. A later compaction supersedes that L0 input (its L1 record becomes used).
3. `DoReconcileTick` fires: it recomputes `FoldEntriesFor(head.wm)` at the same
   watermark, which now differs (the superseded input drops out), and writes it
   into HEAD. The witness `lastHead` records `kind = "recTick"`,
   `entriesChanged = TRUE`, and `wmAfter = wmBefore`.

At that state the antecedent of `ReconcileOnlyOnWatermarkAdvance`
(`lastHead.kind \in {"fold","recTick"}` and `entriesChanged`) holds while
`wmAfter > wmBefore` is false, so the invariant is violated.

## Why it is the right control

In the real system reconcile runs only inside a watermark-advancing fold
(`crates/ravel-catalog/src/fold.rs::reconcile_one_bucket`), so a HEAD entry
change is always tied to the watermark moving forward. A reconcile that mutates
the snapshot off a plain tick would let two readers at the same watermark see
different entries, which this invariant forbids.
