# Catalog MVCC model

Machine-checked model of the catalog plane: fold, snapshots, compaction, and
MVCC (ADR-1113 decision D3, task T3, issue #1121). One `(tenant, signal)`
catalog over a single shard, a small set of ingest hours, and a small pool of
abstract commit-record and compaction-record identities.

## Files

```
CatalogMVCC.tla     the contract module (operators, actions, invariants)
MCCatalogMVCC.tla   model-check entry: folder symmetry, switch defaults
smoke.cfg           fast safety config (symmetry-reduced)
exhaustive.cfg      full safety + liveness config
carryforward.cfg    three-hour safety config that exercises the incremental fold
                    carry-forward branch (paired with its non-vacuity probe)
bands.tsv           per-config distinct-state and depth bands
traceability.md     TLA+ action or property -> Rust source table
results.md          recorded figures and the bands they must stay in
counterexamples/    one prose note per negative control, plus the recorded
                    liveness shrink for the one temporal property that is
                    defined but not checked
negative/           negative-control configs, one broken switch each
  <name>.cfg        a config that flips exactly one switch
  <name>.expect     the exit code and property that config must violate
```

## What the model drives

The HEAD register is a `RavelObjectStore` instance, so its create, CAS, and
lost-response semantics are exactly the shared object-store contract rather than
a re-modeled copy. The immutable planes (L0 commits, L1 compaction records,
snapshot parts, tombstones) are append-only model variables. Actors: ingest
publishes L0 records into not-yet-fold-sealed hours; the compactor publishes an
L1 record over a maintenance-sealed hour behind a counts-only conservation
gate; the folder reads HEAD, stages a snapshot part, publishes it, then
CAS-swaps HEAD, and rebases if it outran the protection horizon; the sweeper
runs the HEAD-gated superseded-input sweep (ADR-0020 delete blocker) and the
HEAD-gated catalog-object sweep, both fail-closed on an unreadable HEAD;
retention tombstones a bucket; and a query resolves and pins a snapshot or
degrades to a listing.

Two seal predicates are modeled distinctly: the fold watermark adds
`FoldSealDelay` (the fold safety margin) on top of the maintenance seal, so a
bucket is compactable strictly before it is foldable.

## Named safety invariants

`HeadNamesOnlyCompleteParts`, `CompactionPreservesMultiset`,
`CompactionRecordImmutable`, `ReconcileOnlyOnWatermarkAdvance`,
`SnapshotEntriesBelowWatermark`, `PinnedSnapshotStableWithinAttempt`,
`NoLiveCommitOmittedByLostCas`, `MissingIndexDegradesToListing`,
`CorruptHeadFailsClosedOnDeletePaths`, `HeadNamedObjectNeverDeleted`,
`TombstonedBucketContributesNothing`, and `SignalDedupContract`. Temporal
(exhaustive only): `QueryTerminates`. `LateSupersessionEventuallyReflected` is
defined but not checked: it is a recorded shrink (finite-model liveness
limitation; see `counterexamples/late-supersession-shrink.md`).

All twelve named invariants above are shown non-vacuous by a negative control
that flips one switch and makes them falsifiable (see `negative/` and
`counterexamples/`); none is left to a mutant note. Two configs are
non-vacuity probes rather than switch mutants: `negative/carryforward-nonvacuity.cfg`
checks `NoCarryForward` over three-hour bounds where a watermark-advancing
fold is reachable, so TLC reporting it violated proves the bounded
incremental fold's carry-forward branch does real work (see `carryforward.cfg`
and `counterexamples/carryforward-nonvacuity.md`); and
`negative/frontier-reconcile-nonvacuity.cfg` checks `NoFrontierReconcile`, so
TLC reporting it violated proves the ADR-0020 retention-frontier reconcile
added to `DoFoldStart` / `DoRivalFoldWin` is exercised, not dead code, and
that `TombstonedBucketContributesNothing`'s `frontierReconciled` disjunct is
not vacuous.

## Running

```sh
scripts/check-tla.sh smoke        -a catalog   # fast safety (symmetry-reduced)
scripts/check-tla.sh negative     -a catalog   # every negative control must fail
scripts/check-tla.sh traceability -a catalog   # every source ref resolves
scripts/check-tla.sh exhaustive   -a catalog   # full safety + liveness
```

The harness resolves Java from `RAVEL_TLA_JAVA` if set, else `java` on `PATH`.
See `../README.md` for the toolchain and jar-pinning details.
