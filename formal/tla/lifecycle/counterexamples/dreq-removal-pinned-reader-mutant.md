# Non-vacuity mutant: DreqRemovalCannotResurrect

Proves `DreqSweep`'s unconditional `~ServesAny("s1")` gate is load-bearing
and that `DreqRemovalCannotResurrect` is independently falsifiable, not just
a restatement of `ErasedSubjectNeverServedAfterRequest` that happens to
co-fire with it (both read the same `ServesAny`/`.dreq`-presence facts, so a
mutation that breaks the shared gate trips both; this run scopes the
invariant list to `TypeOK` plus the one target, the same way a negative
control does, so the reported line names `DreqRemovalCannotResurrect`
specifically instead of whichever invariant a full-list run reports first).

## Mutation

Behaviour edit (not a switch), applied to a scratch copy under `/tmp`. The
unconditional live-reachability gate is deleted from `DreqSweep`:

```tla
     /\ clock >= dreqHorizon
-    /\ ~ServesAny("s1")
     /\ (DreqIgnoresHeldInputs \/ ~HeldInputServes("s1"))
```

Nothing else changed; the run used a scratch cfg identical to `smoke.cfg`
except the invariant list is narrowed to `TypeOK` plus
`DreqRemovalCannotResurrect`, with every switch at its shipped value.

## Result

TLC exit 12. Exact line:

```text
Error: Invariant DreqRemovalCannotResurrect is violated.
```

Eight states: `Init`, `PinQuery`, `RequestErasure`, `Tick`, `RetireBucket`,
`DropRetiredBucketFromHead`, `CompleteErasure`, `DreqSweep`. A query pins on
the current HEAD before `RequestErasure` marks `s1` erased; with the live
gate removed, `DreqSweep` deletes `dreqR1` anyway while the pinned query can
still reach the pre-erasure object serving `s1`. `s1 \in erasureRequested`
and `dreqR1` is absent, but `ServesAny("s1")` still holds through the pinned
read, failing the invariant. Restoring the deleted conjunct makes the invariant hold again (smoke
passes), which is the non-vacuity argument for this gate.

Running the same mutant against the full invariant list reports
`ErasedSubjectNeverServedAfterRequest` instead (it comes first in the cfg's
`INVARIANT` order and is checked at the same state). In this bounded model,
where `erasureRequested` can only ever hold `"s1"` (`RequestErasure` names
it directly; see `RequestErasure`), the two invariants are logically
equivalent restrictions of the same served-set fact -- one universally
quantified over `erasureRequested`, one written out for `s1` -- so neither
can be violated without the other. `DreqRemovalCannotResurrect` is kept as
its own named invariant for traceability (it names the `.dreq`-removal
scenario `crates/ravel-maintain/src/sweep.rs`'s erasure-request sweep
governs, distinct from the general erased-subject property), not because
the bounded model can exhibit a state where one holds and the other
doesn't; a model with more than one erasable subject would separate them.

