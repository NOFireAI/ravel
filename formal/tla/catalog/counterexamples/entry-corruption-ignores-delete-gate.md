# entry-corruption-ignores-delete-gate

Negative control. Switch `DeletePathIgnoresUndecodableEntry = TRUE` drops the
covering-entry-identity guard from `DoSweepSuperseded`, letting the sweep
proceed while a covering snapshot entry cannot be decoded.

Violated invariant: `CorruptHeadFailsClosedOnDeletePaths` (safety, TLC exit 12).

## Trace shape (from the recorded run)

1. States 2-3: ingest publishes an L0 commit into both hours (`rA` lands in
   hour 0, then hour 1).
2. State 4: the clock advances.
3. State 5: a compaction over hour 0 publishes, superseding `rA` there
   (`crec[0]` records `used = TRUE`, `in = out = {rA}`).
4. State 6: `DoPoisonEntry` fires (one-shot), setting `entryUndecodable[0]` to
   `TRUE`.
5. State 7: the clock advances again.
6. State 8: `DoSweepSuperseded` runs. With the guard dropped, it deletes `rA`
   from hour 0 even though the covering entry's identity fields are
   undecodable.

At State 8, `lastDelete.happened = TRUE` and `lastDelete.entryUndecodable =
TRUE` (with `headStatus = "absent"`, so this is not the already-covered
corrupt-HEAD trigger), so `CorruptHeadFailsClosedOnDeletePaths` is false.

## Why it is the right control, and the reconnaissance correction

The real gate, `crates/ravel-maintain/src/reachability.rs::snapshot_object`,
rejects a decoded snapshot entry whose identity fields do not fit the shape a
fold writes, and `object_gate` treats that as undecidable rather than as
"not a delete candidate" — so the sweep must fail closed rather than treat
the ambiguous entry as safe to proceed past. This is a third, distinct
trigger from HEAD-unreadable and covering-part-unreadable: even a valid,
readable HEAD naming a readable part can still name an entry that fails to
decode. `entryUndecodable` and this control model that trigger as a one-shot
environment fault (`DoPoisonEntry`), fixed to a single canonical hour for the
same branching-cost reason as `DoCorruptPart`. This control drops the guard
to show the trigger is load-bearing rather than vacuous.
