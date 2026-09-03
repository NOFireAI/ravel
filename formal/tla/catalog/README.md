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
bands.tsv           per-config distinct-state and depth bands
traceability.md     TLA+ action or property -> Rust source table
results.md          recorded figures and the bands they must stay in
counterexamples/    one note per negative control and per switch-less invariant
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
`ReconcileOnlyOnWatermarkAdvance`, `SnapshotEntriesBelowWatermark`,
`PinnedSnapshotStableWithinAttempt`, `NoLiveCommitOmittedByLostCas`,
`MissingIndexDegradesToListing`, `CorruptHeadFailsClosedOnDeletePaths`,
`HeadNamedObjectNeverDeleted`, `TombstonedBucketContributesNothing`, and
`SignalDedupContract`. Temporal (exhaustive only):
`LateSupersessionEventuallyReflected`.

Six invariants are shown non-vacuous by a negative control that flips one
switch and makes them falsifiable (see `negative/` and `counterexamples/`); the
five without a switch carry a mutant note in `counterexamples/` naming a
reachable antecedent state and the mutation that would falsify them.

## Running

```sh
scripts/check-tla.sh smoke        -a catalog   # fast safety (symmetry-reduced)
scripts/check-tla.sh negative     -a catalog   # every negative control must fail
scripts/check-tla.sh traceability -a catalog   # every source ref resolves
scripts/check-tla.sh exhaustive   -a catalog   # full safety + liveness
```

The harness resolves Java from `RAVEL_TLA_JAVA` if set, else `java` on `PATH`.
See `../README.md` for the toolchain and jar-pinning details.
