# corrupt-head-fails-closed-on-delete-paths (mutant note)

`CorruptHeadFailsClosedOnDeletePaths` has no dedicated switch; its non-vacuity
is argued here.

Invariant: a HEAD-gated delete pass that saw a corrupt or unsupported HEAD
deleted nothing.

## Antecedent is reachable

`DoCorruptHead` or `DoUnsupportedHead` puts HEAD in a non-readable state; then
`DoSweepBlockedOnCorruptHead` fires and records `lastGatedSweep.ran = TRUE`
with `headStatus \in {"corrupt","unsupported"}`. The antecedent is therefore
satisfied by reachable states, so the property is exercised, not vacuous.

## Mutation that would falsify it

Let a delete-path sweep proceed on an unreadable HEAD. Concretely, add
`"corrupt"` or `"unsupported"` to the enabling set of `DoSweepSuperseded` or
`DoSweepCatalogObjects` and let it delete, or set
`lastGatedSweep.deletedAny = TRUE` in `DoSweepBlockedOnCorruptHead`. Then a pass
that could not read HEAD would remove an object the live snapshot might still
name. The real reachability read fails closed on an undecodable HEAD
(`crates/ravel-maintain/src/reachability.rs::SnapshotReachability::bucket_gate`),
which is what the invariant enforces.
