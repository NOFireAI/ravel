# missing-index-degrades-to-listing (mutant note)

`MissingIndexDegradesToListing` has no dedicated switch; its non-vacuity is
argued here.

Invariant: a query that resolved while HEAD was present but not valid (corrupt
or unsupported) used `source = "listing"`, never an error.

## Antecedent is reachable

`DoCorruptHead` and `DoUnsupportedHead` drive HEAD to `corrupt` and
`unsupported`. A `DoQueryResolve` or `DoQueryRun` from that state sets
`qy.headStatusAtResolve` to that non-valid, non-none status, so the antecedent
`headStatusAtResolve # "valid" /\ # "none"` is satisfied in the reachable state
space. The invariant is checked against real degraded resolves.

## Mutation that would falsify it

Change `DoQueryResolve` (and the re-resolve branch of `DoQueryRun`) to set
`source = "snapshot"` on a non-valid HEAD, or to move to an error phase instead
of resolving. Either makes the query fail closed on an unreadable index rather
than falling back to a direct commit-plane listing. The correct engine treats
the catalog index as a pure optimization
(`crates/ravel-query/src/engine.rs::QueryEngine::resolve_snapshot_with_retry`):
a missing or unreadable index degrades to listing, which this invariant pins.
