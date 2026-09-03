# carryforward-nonvacuity

Non-vacuity probe, not a mutation. `negative/carryforward-nonvacuity.cfg` checks
`NoCarryForward` (no fold ever carries a hour below its reconcile floor forward)
over three-hour bounds where a second, watermark-advancing fold is reachable.
The probe MUST be violated: it proves the bounded incremental fold's carry-forward
branch (`IncrementalFoldEntries`) does real work at these bounds, so the paired
`carryforward.cfg` safety pass over the same bounds is not vacuous.

Violated invariant: `NoCarryForward` (probe, TLC exit 12).

## Trace shape (from the recorded run)

1. An L0 input is committed into hour 0, then a fold against an absent HEAD (a
   full rescan) wins its CAS, so a valid HEAD names hour 0's live input.
2. As successive hours fold-seal, further folds advance the HEAD watermark, one
   step at a time, up to a fold whose prior snapshot already sat at watermark 1.
3. That fold advances the watermark to 2. With `ReconcileWindow = 0` its
   reconcile floor is the prior watermark (1), so hour 0 sits below the floor and
   is carried forward verbatim from the prior snapshot rather than rescanned. The
   fold's `lastHead.reconcileLo` is 1 and its entries still include the hour-0
   entry, so `CarryForwardExercised` becomes true and `NoCarryForward` is false.

## Why it is here

At the two-hour smoke bounds the carry-forward branch is dormant: the watermark
saturates at 1 and no fold advances from it, so `IncrementalFoldEntries`
collapses to a full rescan and is behaviorally identical to the omniscient
oracle. Without this probe the three-hour `carryforward.cfg` safety pass could not
be distinguished from a run that never exercised the incremental path. See #7 in
`results.md`.
