# part-corruption-ignores-delete-gate

Negative control. Switch `DeletePathIgnoresUnreadablePart = TRUE` drops the
covering-snapshot-part guard from `DoSweepSuperseded`, letting the sweep
proceed while the covering part is unreadable.

Violated invariant: `CorruptHeadFailsClosedOnDeletePaths` (safety, TLC exit 12).

## Trace shape (from the recorded run)

1. State 2: ingest publishes an L0 commit (`rA` lands in hour 0).
2. State 3: the clock advances.
3. State 4: a compaction over hour 0 publishes, superseding `rA`.
4. State 5: the clock advances past the protection horizon.
5. State 6: `DoCorruptPart` fires (one-shot), setting `partUnreadable[0]` to
   `TRUE`.
6. State 7: `DoSweepSuperseded` runs. With the guard dropped, it deletes `rA`
   from `l0[0]` even though the covering part for hour 0 is unreadable.

At State 7 `lastDelete.happened = TRUE` and `lastDelete.partUnreadable = TRUE`,
so `CorruptHeadFailsClosedOnDeletePaths` is false.

## Why it is the right control, and the reconnaissance correction

The real gate,
`crates/ravel-maintain/src/reachability.rs::SnapshotReachability::object_gate`
(via `ensure_part`), fails the whole superseded-input sweep closed when a
covering snapshot part cannot be decoded, exactly as it does for an unreadable
HEAD. An earlier round of this model represented only the HEAD-unreadable
trigger; `partUnreadable` and this control close that gap by modelling the
covering-part trigger as a distinct one-shot environment fault
(`DoCorruptPart`), fixed to a single canonical hour so it does not multiply
`Hours`-sized branching into every reachable state (see `DoCorruptPart`'s
header comment in `CatalogMVCC.tla`). This control drops the guard to show the
trigger is load-bearing rather than vacuous.
