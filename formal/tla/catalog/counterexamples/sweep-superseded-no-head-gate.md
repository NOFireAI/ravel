# sweep-superseded-no-head-gate

Negative control. Switch `SweepSupersededNoHeadGate = TRUE` drops the object-
granular HEAD-reachability gate from the superseded-input sweep, reproducing
the pre-ADR-0020 shape once feared under issue #1134.

Violated invariant: `HeadNamedObjectNeverDeleted` (safety, TLC exit 12).

## Trace shape (from the recorded run)

1. States 2 to 3: ingest publishes L0 commits (an input `r` lands in an hour).
2. States 4 to 5: the clock advances past the fold seal.
3. State 6: a folder starts a fold and stages `r` as a live L0 entry (the
   compaction is not yet published, so `r` is not superseded).
4. State 7: a compaction over the hour publishes, superseding `r`.
5. States 8 to 9: the folder writes its part and its CAS wins, so a valid HEAD
   now names `<<"l0", H, r>>`. The CAS is inside the fold-lifetime bound (it
   commits in the same tick it listed).
6. State 10: the clock advances past the protection horizon.
7. State 11: `DoSweepSuperseded` runs. With the gate dropped, `deletable` is
   the full input set, so `r` is removed from `l0[H]` even though the valid
   HEAD still names it.

At State 11 `head.status = "valid"`, `<<"l0", H, r>> \in head.entries`, and
`ObjectDeleted(<<"l0", H, r>>)` holds, so `HeadNamedObjectNeverDeleted` is
false. A query following HEAD would fetch a missing object.

## Why it is the right control, and the reconnaissance correction

The current crate does have the gate:
`crates/ravel-maintain/src/sweep.rs::sweep_superseded_impl` object-granularly
holds any input a valid HEAD names and fails closed on an unreadable HEAD
(ADR-0020 delete blocker), pinned by
`crates/ravel-maintain/tests/superseded_head_gate.rs::head_named_superseded_inputs_are_held_not_deleted`.
An earlier premise held this sweep had no HEAD gate (issue #1134); that premise
is stale against the code. The correct model keeps the gate and asserts
`HeadNamedObjectNeverDeleted` as a real property; this control drops the gate to
show the property is load-bearing rather than vacuous.
